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
production = client.production