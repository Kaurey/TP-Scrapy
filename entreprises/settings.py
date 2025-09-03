ITEM_PIPELINES = {
    "entreprises.pipelines.MongoDBPipeline": 300,
}

MONGO_URI = "mongodb://localhost:27017"
MONGO_DATABASE = "entreprises_db"