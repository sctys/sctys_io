import pymongo
import json

class MongoDBIO(object):

    DB_URL = "mongodb://localhost:27017/"

    def __init__(self, project, logger):
        self.project = project
        self.logger = logger
        self.redis = None
        self.client = None
    
    def __enter__(self):
        self.create_db_connection()
        return self.client
    
    def __exit__(self, exc_type, exc_val, traceback):
        self.close_connection()
    
    def create_db_connection(self):
        if self.client is None:
            try:
                self.client = pymongo.MongoClient(self.DB_URL)
                self.logger.debug('Connected to MongoDB server')
            except Exception as e:
                self.logger.error('Error in connecting to MongoDB server. {}'.format(e))
                raise
    
    def close_connection(self):
        if self.client is not None:
            self.logger.debug('Closing MongoDB connection')
            self.client.close()
    
    def obtain_database(self):
        if self.client is None:
            self.create_db_connection()
        db = self.client[self.project]
        return db
    
    def obtain_collection(self, db, collection_name):
        collection = db[collection_name]
        return collection
    
    def count_file_in_redis(self, collection_name):
        if self.redis is None:
            self.init_redis()
        self.redis.incr(collection_name)
    
    def insert_document(self, collection, document, keys, count_file=True):
        collection.insert_one(document)
        document_key = json.dumps({key: document[key] for key in keys})
        self.logger.debug('Document for {} inserted to {}'.format(document_key, collection.full_name))
        if count_file:
            self.count_file_in_redis(collection.name)
    
    def find_documents(self, collection, query, return_keys=None):
        projection = {"_id": 0}
        if return_keys is not None:
            if isinstance(return_keys, str):
                return_keys = [return_keys]
            projection = {**projection, **{key: 1 for key in return_keys}}
        documents = collection.find(query, projection)
        return documents
    
    def count_documents(self, collection, query):
        count = collection.count_documents(query)
        return count
    
    def get_collection_index(self, collection):
        return list(collection.index_information().keys())
    
    def check_index_exist(self, collection, index):
        existing_index = self.get_collection_index(collection)
        return index in existing_index
    
    def create_index(self, collection, index, ascending=True, unique=False):
        if not self.check_index_exist(collection, index):
            self.logger.debug("Index {} not exist in collection {}. Creating index {}".format(index, collection.full_name, index))
            sort_order = pymongo.ASCENDING if ascending else pymongo.DESCENDING
            collection.create_index([(index, sort_order)], unique=unique)
        return collection