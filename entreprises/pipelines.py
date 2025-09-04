from pymongo import MongoClient
import re

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
        generalites = item["kbo"].get("generalites", {})

        numero_brut = generalites.get("numero_dentreprise")
        numero_clean = None

        if numero_brut:
            # Extraire les 10 chiffres du numéro officiel (première occurrence)
            match = re.search(r"\d{4}\.\d{3}\.\d{3}", numero_brut)
            if match:
                numero_clean = match.group(0).replace(".", "")

        if numero_clean:
            self.collection.update_one(
                {"_id": numero_clean},
                {"$set": dict(item)},
                upsert=True
            )
        else:
            spider.logger.warning(f"⚠️ Aucun numéro valide trouvé dans: {numero_brut}")
            self.collection.insert_one(dict(item))

        return item


