from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import lower, upper, col, udf, array_contains
from pyspark.sql.types import StringType, BooleanType
import re
import os
import glob


# .config("spark.executor.memory", "16g") \
    # .config("spark.executor.cores", "2") \
    # .config("spark.driver.memory", "16g") \

#gotta start the session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("vinf wiki dump parser") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.9.0") \
    .getOrCreate()

#read the xml, load it into a spark df
df = spark.read.format('xml') \
    .option("rowTag", "page") \
    .load("enwiki-20231101-pages-articles9.xml-p2936261p4045402.bz2")

#this function filters for given names
def contains_given_name_category(text):
    return bool(text and re.search(r"\[\[Category:.* given names\]\]", text))

#registering the user defined function
contains_given_name_category_udf = udf(contains_given_name_category, BooleanType())

#extract the content, limited substring of relevant data because spark decides to quit on me otherwise
dump_df = df.withColumn("text_content", col("revision.text._VALUE")) \
    .filter(contains_given_name_category_udf(col("text_content"))) \
    .select(
        upper(col("title")).alias("name"),
        col("text_content").substr(1, 100).alias("wiki_data")
    )

#load the spark dump df into pandas df
pandas_dump_df = dump_df.toPandas()

#load our name data into spark then pandas
names_df = pd.read_csv("name_data.csv")
names_df = spark.createDataFrame(names_df)

#get the spark df into pandas
pandas_names_df = names_df.toPandas()

#join the data on name
enriched_data = pandas_names_df.merge(
    pandas_dump_df,
    left_on="Name",
    right_on="name",
    how="left"
)

#drop the column we dont want
enriched_data.drop(columns="name", inplace=True)

#transforms the text so its more readable
def remove_nested_curly_braces(text):
    while '{{' in text:
        open_brace = -1
        brace_count = 0
        found_closing_brace = False
        for i, char in enumerate(text):
            if char == '{':
                brace_count += 1
                if brace_count == 1:
                    open_brace = i
            elif char == '}':
                brace_count -= 1
                if brace_count == 0 and open_brace != -1:
                    found_closing_brace = True
                    text = text[:open_brace] + text[i+1:]
                    break

        # Break out of the loop if no closing brace is found
        if not found_closing_brace:
            break

    return text

#transforms the text so its more readable
def remove_empty_lines(text):
    lines = text.split('\n')
    cleaned_lines = [line.strip() for line in lines if line.strip()]
    return '\n'.join(cleaned_lines)

#transforms text so its more readable
def clean_wiki_text(text):
    if pd.notnull(text):
        text = remove_nested_curly_braces(text)
        text = re.sub(r"\[|\]", "", text)
        text = text.replace("'", "")
        text = remove_empty_lines(text)
    return text


#apply the text cleaning function to our wiki dump data inside of our new df
enriched_data['wiki_data'] = enriched_data['wiki_data'].apply(clean_wiki_text)

#output the data
csv_file_path = "/data/enriched_data.csv"
enriched_data.to_csv(csv_file_path, index=False)

print(f"\n\nData written to CSV file!!!!: {csv_file_path}\n\n")

#put spark to sleep
spark.stop()