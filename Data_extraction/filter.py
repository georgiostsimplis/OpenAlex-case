from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('localhost', 27017)  # Update the connection string with your MongoDB server details
db = client.openalex_db  # Replace 'openalex_db' with your actual database name
collection = db.dataset  # Replace 'dataset' with your actual collection name

# Specify the fields to keep
fields_to_keep = ["id", "doi", "title", "publication_year", "publication_date",
                  "primary_location.is_oa", "primary_location.source.host_organization_name",
                  "type", "authorships", "related_works", "updated_date", "created_date"]

# Create an aggregation pipeline to project only the specified fields
pipeline = [
    {"$project": {field: 1 for field in fields_to_keep}},
    {"$out": "dataset"}  # Overwrite the existing collection
]

# Use the aggregation pipeline to update the collection
collection.aggregate(pipeline, allowDiskUse=True)

# Close the MongoDB connection
client.close()
