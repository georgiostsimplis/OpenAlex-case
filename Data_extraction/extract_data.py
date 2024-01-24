import requests
import json
from pymongo import MongoClient




def extract_and_store(api_url, database_name, collection_name):    
    # Specify the cursor parameter to start cursor pagination
    params_cursor_paging = {
        'cursor': '*',
    }

    # Connect to MongoDB on localhost |  running on the default port 27017
    client = MongoClient('localhost', 27017)  

    # Create or access a database | 
    db = client[database_name]

    # Create or access a collection | 
    collection = db[collection_name]

    total_results = 0
    next_cursor = params_cursor_paging

    while next_cursor is not None and total_results < 1000:
        response_cursor_paging = requests.get(api_url, params=params_cursor_paging)
        data_cursor_paging = response_cursor_paging.json()


        data = data_cursor_paging["results"]


        result = collection.insert_many(data)
        
        next_cursor = data_cursor_paging['meta']['next_cursor']
        
        # 200 results by page
        total_results += 200 

        params_cursor_paging['cursor'] = next_cursor
        

    client.close()


if __name__ == "__main__":
    api_url = "https://api.openalex.org/works?page=1&filter=publication_year:2011-2024,institutions.country_code:DK,from_updated_date:2024-01-20&api_key=DFP64JtmA3mNych5RLEHHC&sort=cited_by_count:desc&per_page=200"
    database_name = 'openalex_db'
    collection_name = 'updated_data'
    extract_and_store(api_url, database_name, collection_name)

