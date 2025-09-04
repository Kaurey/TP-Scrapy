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
        self.client.close()

    def process_item(self, item, spider):
        # Extraire le numéro propre
        numero_brut = item.get("kbo", {}).get("generalites", {}).get("numero_dentreprise")
        numero_clean = None
        if numero_brut:
            match = re.search(r"(\d{4})[.]?(\d{3})[.]?(\d{3})", numero_brut)
            if match:
                numero_clean = "".join(match.groups())

        if not numero_clean:
            spider.logger.warning(f"⚠️ Aucun numéro valide trouvé dans: {numero_brut}")
            self.collection.insert_one(dict(item))
            return item

        if spider.name == "kbo_spider":
            # --- Item KBO : mettre à jour TOUTES les infos ---
            update_data = item.copy()
            publications = update_data.pop("publications", [])
            self.collection.update_one(
                {"_id": numero_clean},
                {
                    "$setOnInsert": {"_id": numero_clean},
                    "$set": update_data,
                    "$push": {"publications": {"$each": publications}}
                },
                upsert=True
            )

        elif spider.name == "ejustice":
            # --- Item EJustice : ne jamais écraser les infos KBO, juste ajouter les publications ---
            publications = item.get("publications", [])
            if publications:
                self.collection.update_one(
                    {"_id": numero_clean},
                    {"$push": {"publications": {"$each": publications}}},
                    upsert=True
                )

        return item
