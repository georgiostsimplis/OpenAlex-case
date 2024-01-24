from elasticsearch import Elasticsearch
from pymongo import MongoClient
from bson import ObjectId
import json

# Connect to MongoDB
mongo_client = MongoClient('localhost', 27017)
mongo_db = mongo_client["openalex_db"]
mongo_collection = mongo_db["dataset"]

es = Elasticsearch('http://localhost:9200', verify_certs=False)
print(es.ping())

index_name = "bulk19"


es.indices.create(index=index_name)

# Index each document from MongoDB into Elasticsearch
for document in mongo_collection.find():
    
    document['_id'] = str(document['_id'])

    # Remove MongoDB-specific _id field
    if '_id' in document:
        del document['_id']

    
    es.index(index=index_name, body=document)

# Udpdate the index to make the documents available for search
es.indices.refresh(index=index_name)

mongo_client.close()
