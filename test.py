import pandas as pd
from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection, utility

# uri = '127.0.0.1'
uri = 'https://in03-4deca087036f63f.api.gcp-us-west1.zillizcloud.com'
token = '33a6ca21413fd126785a65baa24f2f09afd4976ab61004a60429ebb43d66e5fb6b19d80effac2f93d3a1f5a60d06c0121ff50010'
connections.connect("default",uri=uri, token=token)
# connections.connect(host=uri, port='19530', token=token)

collection_name = 'search_article_in_medium'
print(utility.has_collection(collection_name))


fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=500),
        FieldSchema(name="title_vector", dtype=DataType.FLOAT_VECTOR, dim=768),
        FieldSchema(name="link", dtype=DataType.VARCHAR, max_length=500),
        FieldSchema(name="reading_time", dtype=DataType.INT64),
        FieldSchema(name="publication", dtype=DataType.VARCHAR, max_length=500),
        FieldSchema(name="claps", dtype=DataType.INT64),
        FieldSchema(name="responses", dtype=DataType.INT64)
]
schema = CollectionSchema(fields=fields, description='search text')
collection = Collection(name=collection_name, schema=schema)
collection.load()
print(print(collection.num_entities))
result = collection.query(expr="id >= 0", output_fields=["id", "title"])
print(result)
