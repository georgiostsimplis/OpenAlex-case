from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch('http://localhost:9200', verify_certs=False)

# Aggregation query to find the top 20 Danish authors
result = es.search(index="first_bulk", body={
  "size": 0,
  "query": {
    "match": {"authorships.institutions.country_code": "DK"}
  },
  "aggs": {
    "top_authors": {
      "terms": {
        "field": "authorships.author.display_name.keyword",
        "size": 20
      }
    }
  }
})

# Display the top Danish authors and their affiliations
for bucket in result['aggregations']['top_authors']['buckets']:
    author_name = bucket['key']
    print(f"Author: {author_name}")
    
    # Extract and display affiliations
    affiliations = set()
    for hit in result['hits']['hits']:
        authorships = hit['_source']['authorships']
        for authorship in authorships:
            if authorship['author']['display_name'] == author_name:
                for institution in authorship['institutions']:
                    affiliations.add(institution['display_name'])
    
    print(f"Affiliations: {', '.join(affiliations)}")
    print("-" * 30)