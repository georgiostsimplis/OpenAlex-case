from pymongo import MongoClient

    
def update_data(updated_collection):    
    
    client = MongoClient('localhost', 27017)
    database = client['openalex_db']
    collection1 = database['dataset']
    collection2 = database[updated_collection]

    try:
        # Get all distinct titles from collection2
        distinct_titles = collection2.distinct("title")

        # Delete documents in collection1 with matching titles
        collection1.delete_many({"title": {"$in": distinct_titles}})

        # Get all documents from collection2 and insert into collection1
        documents_to_insert = list(collection2.find())
        if documents_to_insert:
            collection1.insert_many(documents_to_insert)

        print('Merge operation completed successfully.')

    finally:
        client.close()