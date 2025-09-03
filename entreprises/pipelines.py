import pymongo
from pymongo import MongoClient

class MongoDBPipeline:
    def __init__(self, mongo_uri="mongodb://localhost:27017", mongo_db="entreprises"):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    def open_spider(self, spider):
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        self.collection = self.db["entreprises"]

    def close_spider(self, spider):
        # s'assurer que toutes les opérations sont finies avant de fermer
        self.client.close()

    def process_item(self, item, spider):
        # Prends le numero depuis generalites si présent
        generalites = item["kbo"].get("generalites", {})
        numero = generalites.get("numero_officiel")
        if numero:
            self.collection.update_one(
                {"_id": numero},
                {"$set": dict(item)},
                upsert=True
            )
        else:
            self.collection.insert_one(dict(item))

        return item

