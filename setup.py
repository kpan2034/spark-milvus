import pandas as pd

# Ref: https://github.com/towhee-io/examples/blob/main/nlp/text_search/search_article_in_medium.ipynb
# You can download the dataset from Kaggle here:
# https://www.kaggle.com/datasets/shiyu22chen/cleaned-medium-articles-dataset

# Load the csv file
df = pd.read_csv('New_Medium_Data.csv', converters={'title_vector': lambda x: eval(x)})
df.head()

# Connect to milvus
from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection, utility

uri = "https://in03-4deca087036f63f.api.gcp-us-west1.zillizcloud.com"
token = "33a6ca21413fd126785a65baa24f2f09afd4976ab61004a60429ebb43d66e5fb6b19d80effac2f93d3a1f5a60d06c0121ff50010"
connections.connect("default", uri=uri, token=token)
# connections.connect(host='127.0.0.1', port='19530')

# Create milvus connection

def create_milvus_collection(collection_name, dim):
    if utility.has_collection(collection_name):
        utility.drop_collection(collection_name)
    
    fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
            FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=500),   
            FieldSchema(name="title_vector", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="link", dtype=DataType.VARCHAR, max_length=500),
            FieldSchema(name="reading_time", dtype=DataType.INT64),
            FieldSchema(name="publication", dtype=DataType.VARCHAR, max_length=500),
            FieldSchema(name="claps", dtype=DataType.INT64),
            FieldSchema(name="responses", dtype=DataType.INT64)
    ]
    schema = CollectionSchema(fields=fields, description='search text')
    collection = Collection(name=collection_name, schema=schema)
    
    index_params = {
        'metric_type': "L2",
        'index_type': "IVF_FLAT",
        'params': {"nlist": 2048}
    }
    collection.create_index(field_name='title_vector', index_params=index_params)
    return collection

collection = create_milvus_collection('search_article_in_medium', 768)
collection.insert(df)
# # Insert to the collection
# from towhee import ops, pipe, DataCollection
#
# insert_pipe = (pipe.input('df')
#                    .flat_map('df', 'data', lambda df: df.values.tolist())
#                    .map('data', 'res', ops.ann_insert.milvus_client(host='127.0.0.1',
#                                                                     port='19530',
#                                                                     collection_name='search_article_in_medium'))
#                    .output('res')
# )
#
# _ = insert_pipe(df)
#
# # Load collection and view number of entitites
# collection.load()
# print(collection.num_entities)
#
# # Verify everything is gucci by performing example search
# import numpy as np
#
# search_pipe = (pipe.input('query')
#                     .map('query', 'vec', ops.text_embedding.dpr(model_name="facebook/dpr-ctx_encoder-single-nq-base"))
#                     .map('vec', 'vec', lambda x: x / np.linalg.norm(x, axis=0))
#                     .flat_map('vec', ('id', 'score'), ops.ann_search.milvus_client(host='127.0.0.1',
#                                                                                    port='19530',
#                                                                                    collection_name='search_article_in_medium'))
#                     .output('query', 'id', 'score')
#                )
# res = search_pipe('funny python demo')
# DataCollection(res).show()
