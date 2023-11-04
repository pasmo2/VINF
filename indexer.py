import pandas as pd
from collections import defaultdict

def create_index(df, columns_to_index):
    #nested dict, first key is the word, second key is the column name
    index = defaultdict(lambda: defaultdict(set))

    for column in columns_to_index:

        for row_number, text in enumerate(df[column]):
            #i want this to be case insensitive, so transforming the text into lowercase, then splitting to words
            words = str(text).lower().split()

            for word in words:
                #save info about the row the word is in, so we can get it easier(will be seaarching which rows a set of words is in)
                index[word][column].add(row_number+2)
                #to represent actual csv rows we must add 2, as we obviously start from index 0, but csv rows start from 1, 
                #and the first row is just column names, so thats a skip too
    
    return index


#function to find rows containing all words
def get_rows(index, words, columns_to_index):
    # #fonvert all words to lowercase TODO
    # words = [word.lower() for word in words]
    #set to keep track of rows where all words are found
    rows_with_all_words = None
    
    #iterate through each word to intersect row sets
    for word in words:
        rows_for_word = set()
        for column in columns_to_index:
            rows_for_word |= index.get(word, {}).get(column, set())
        #if it"s the first word, assign its rows, else intersect
        if rows_with_all_words is None:
            rows_with_all_words = rows_for_word
        else:
            rows_with_all_words &= rows_for_word
            
        #if at any point we have no rows left, break as theres no common row
        if not rows_with_all_words:
            break
    
    return rows_with_all_words if rows_with_all_words else set()


def run_search(index, cols):
    #filter words(operator: AND)
    input_words = ["hindu", "english"]
    rows = get_rows(index, input_words, cols)

    print(f"\nrows that contain the words < {' AND '.join(input_words)} > :")
    for item in rows:
        print("row number",item)
    print("\n")


df = pd.read_csv("name_data.csv")

chosen_columns = ['Name', 'Gender', 'Pronunciation', 'Origin', 'Meaning', 'Facts', 'Synonym']

#create the index
indexed_values = create_index(df, chosen_columns)

run_search(indexed_values, chosen_columns)

