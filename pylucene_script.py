import lucene

lucene.initVM()

import csv
from org.apache.lucene.document import Document, Field, StringField, TextField
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.store import NIOFSDirectory
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.search import TermQuery
from org.apache.lucene.index import Term
from java.nio.file import Paths
from enum import Enum, auto
import time

class SearchableFields(Enum):
    Name = "Name"
    Gender = "Gender"
    Pronunciation = "Pronunciation"
    Origin = "Origin"
    Meaning = "Meaning"
    Facts = "Facts"
    Synonym = "Synonym"

class SearchMethods(Enum):
    BaseSearch = "base search"
    SynonymSearch = "synonym search"
    # Pronunciation = "Pronunciation"
    # Origin = "Origin"
    # Meaning = "Meaning"
    # Facts = "Facts"
    # Synonym = "Synonym"


data_path = "name_data.csv"
index_path = Paths.get('index_directory')
index_dir = NIOFSDirectory(index_path)
analyzer = StandardAnalyzer()
config = IndexWriterConfig(analyzer)
writer = IndexWriter(index_dir, config)

#indexing function - adds as  alucene document
def add_doc(writer, csv_record):
    doc = Document()
    doc.add(TextField("Name", csv_record['Name'], Field.Store.YES))
    doc.add(TextField("Gender", csv_record['Gender'], Field.Store.YES))
    doc.add(TextField("Pronunciation", csv_record['Pronunciation'], Field.Store.YES))
    doc.add(TextField("Origin", csv_record['Origin'], Field.Store.YES))
    doc.add(TextField("Meaning", csv_record['Meaning'], Field.Store.YES))
    doc.add(TextField("Facts", csv_record['Facts'], Field.Store.YES))
    doc.add(TextField("Synonym", csv_record['Synonym'], Field.Store.YES))
    writer.addDocument(doc)

#indexing the data row by row
with open(data_path, 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        add_doc(writer, row)

writer.close()

index_reader = DirectoryReader.open(index_dir)
searcher = IndexSearcher(index_reader)


def search_index(search_field, search_term, num_records = 10):
    field_name = SearchableFields[search_field].value
    query_parser = QueryParser(field_name, analyzer)
    query = query_parser.parse(search_term)
    hits = searcher.search(query, num_records).scoreDocs
    print(f"Found {len(hits)} matches for '{search_term}' in '{field_name}'.\n")
    
    for hit in hits:
        doc = searcher.doc(hit.doc)
        print(f"Name: {doc.get('Name')} - {field_name}: {doc.get(field_name)}")
    print("\n")

    return hits


def base_search_loop():
    while(True):
        print("Fields you can search by:\n")
        for field in SearchableFields:
            print(field.value)

        search_field_input = input("Enter the field to search by(type exit to exit): \n")
        search_field_input = search_field_input.capitalize()
        if search_field_input == "Exit":
            print("Exiting the function\n")
            break
        if search_field_input not in SearchableFields:
            print("Incorrect field name passed\n")
            continue
        search_term_input = input(f"Enter the search term for {search_field_input}: \n")

        search_index(search_field_input, search_term_input)


def synonym_search_loop():
    while(True):
        searched_name = input("Enter the name you want synonyms for(type exit to exit): \n")
        searched_name = searched_name.capitalize()
        if searched_name == "Exit":
            print("Exiting the function\n")
            break

        search_index("Synonym", searched_name, num_records=1000)


def test_base_search():
    start_time = time.time()
    hits = search_index("Origin", "American")
    expected_output=["AMBERLEE","AMBERLYN", "ANALY", "ANNALYNN", "ARLEANA", "BAILON", "BITSIE", "BRENDA LEE", "BRINLEE", "BUTCH"]
    i=0
    assert len(hits) == len(expected_output)
    for hit in hits:
        name = searcher.doc(hit.doc).get("Name")
        assert str(name) == expected_output[i]
        i+=1

    print(f"########################\n########################\nBASE SEARCH TEST WAS SUCCESSFUL IN {time.time()-start_time} seconds!\n########################\n########################\n")


def test_synonym_search():
    start_time = time.time()
    hits = search_index("Synonym", "Jan", num_records=1000)
    expected_output=["JANECEK", "JANEIK", "JANEK", "JANKO", "JANNIK"]
    i=0
    assert len(hits) == len(expected_output)
    for hit in hits:
        name = searcher.doc(hit.doc).get("Name")
        assert str(name) == expected_output[i]
        i+=1

    print(f"########################\n########################\nSYNONYM SEARCH TEST WAS SUCCESSFUL IN {time.time()-start_time} seconds!\n########################\n########################\n")


def main_loop():
    while(True):
        print("Methods you can search by:\n")
        for field in SearchMethods:
            print(field.value)
        print("! You can also trigger unit tests by typing 'unit tests'")
        menu_option_input = input("Enter the method to search by(type exit to exit): \n")
        menu_option_input = menu_option_input.lower()
        if menu_option_input == "exit":
            print("Exiting the function\n")
            break
        if menu_option_input == "unit tests":
            test_base_search()
            test_synonym_search()
        elif menu_option_input not in SearchMethods:
            print("Incorrect field name passed\n")
            continue
        if menu_option_input == "base search":
            base_search_loop()
        if menu_option_input == "synonym search":
            synonym_search_loop()
        
            

main_loop()

index_reader.close()