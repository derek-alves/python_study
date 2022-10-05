
from dotenv import load_dotenv,find_dotenv
from pymongo import MongoClient
import os
import pprint

load_dotenv(find_dotenv())
password = os.environ.get("MONGO_PASSWORD")
user = os.environ.get("MONGO_USER")
connection_string = f"mongodb+srv://{user}:{password}@cnpjdata.bearwwl.mongodb.net/?retryWrites=true&w=majority"

printer = pprint.PrettyPrinter()

client = MongoClient(connection_string)
dbs = client.list_database_names()
print(f"database:{dbs}")
test_db = client.test
collections = test_db.list_collection_names()
print(f"collection:{collections}")

def insert_test_doc():
    collection = test_db.test
    test_document = {
        "name":"derek",
        "type": "test"
    }
    id = collection.insert_one(test_document).inserted_id
    print(f'inserted_id:{id}')


production = client.production
person_collection = production.person_collection

def create_documents():
    first_names = ["marcio", "paulo","cavalo","bode"]
    last_names = ["marcola", "paulão", "cruzado","bodao"]
    ages=[21,30,40,10]
    
    data = []
    
    for first_name, last_name, age in zip(first_names,last_names,ages):
        doc = {"first_name": first_name,"last_names":last_name, "age": age}
        data.append(doc)
    person_collection.insert_many(data)
    

def find_all_people():
    people = person_collection.find()
    
    for person in people:
        printer.pprint(person)
        
def find_person(name:str):
    people = person_collection.find_one({"first_name":name})
    printer.pprint(people)  


def count_people():
    count = person_collection.count_documents(filter={})
    print("Number of people",count)  
    
count_people()