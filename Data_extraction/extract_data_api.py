import requests
import json
from pymongo import MongoClient

response = requests.get("https://api.openalex.org/works?page=1&filter=institutions.country_code:DK,publication_year:2011-2024&sort=cited_by_count:desc&per_page=1000")

print(response.status_code)

json_object = response.json()

#print(json.dumps(response.json(), indent=3)[0][0])

# Connect to MongoDB on localhost
client = MongoClient('localhost', 27017)  # Assuming MongoDB is running on the default port 27017

# Create or access a database (replace 'your_database' with the desired database name)
db = client.your_database

# Create or access a collection (replace 'your_collection' with the desired collection name)
collection = db.your_collection

result = collection.insert_one(json_object)
print(f"Inserted document with ID: {result.inserted_id}")

