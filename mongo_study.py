
from ast import Str
from tokenize import String
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
    

def get_person_by_id(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id":_id})
    printer.pprint(person)
    

def get_age_range(min_age:int,max_age:int):
    query={"$and":[
                {"age":{"$gte":min_age,}},
                {"age":{"$lte":max_age,}}
            ]}
    
    people = person_collection.find(query).sort('age')
    for person in people:
        printer.pprint(person)
    

def columns():
    columns = {"_id":0, "first_name":1, "last_names":1}
    people = person_collection.find({}, columns)
    for person in people:
        printer.pprint(person)

def update_person_by_id(person_id:Str):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
 # add new field and rename   
    # all_updates = {
    #     "$set":{"new_field": True},
    #     "$inc":{'age':1},
    #     "$rename":{"first_name": "first", "last_names":"last"}
    # }
    # person_collection.update_one({"_id":_id}, all_updates)
    
    person_collection.update_one({"_id":_id},{"$unset":{"new_field": ""}}, )
    
    
def replace_one(person_id:Str):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    data = {"first_name": "novo nome", "last_names":"nome ultimo nome", "age":100}
    
    person_collection.replace_one({"_id":_id}, data)
    

def delete_one(person_id:Str):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    person_collection.delete_one({"_id":_id})
    
#----------------------------------------------------------------------------------------------------------
address = {
    "street": "Street bey",
    "number": 2706,
    "city": "City bey",
    "countery":"Countery bay",
    "zip": "94107",
}
def add_adress_embed(address, person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    
    person_collection.update_one({"_id": _id}, {"$addToSet": {"address": address}})


def add_adress_relations(person_id,address):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    
    address = address.copy()
    address["owner_id"] = person_id
    
    address_collection = production.address
    address_collection.insert_one(address)
    
    
add_adress_embed(address,"633d063273dd640529707815")
