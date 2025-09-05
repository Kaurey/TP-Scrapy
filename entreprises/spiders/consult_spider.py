import scrapy
from pymongo import MongoClient

class ConsultSpider(scrapy.Spider):
    name = "consult"

    custom_settings = {
        "USER_AGENT": "Mozilla/5.0",
        "DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS": 2,
        "ROBOTSTXT_OBEY": False,
        "LOG_LEVEL": "INFO"
    }

    def start_requests(self):
        client = MongoClient("mongodb://localhost:27017")
        db = client["entreprises"]
        collection = db["entreprises"]

        for doc in collection.find({}, {"_id": 1}):
            numero = doc["_id"]
            url = (
                f"https://consult.cbso.nbb.be/api/rs-consult/published-deposits?"
                f"page=0&size=10&enterpriseNumber={numero}&"
                f"sort=periodEndDate,desc&sort=depositDate,desc"
            )
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={"numero": numero, "page": 0},
                dont_filter=True
            )

    def parse(self, response):
        numero = response.meta["numero"]
        page = response.meta["page"]
        data = response.json()

        comptes = []
        for depot in data.get("content", []):
            comptes.append({
                "titre": depot.get("modelName"),
                "reference": depot.get("reference"),
                "date_depot": depot.get("depositDate"),
                "date_fin_exercice": depot.get("periodEndDate"),
                "langue": depot.get("language"),
            })

        yield {
            "kbo": {"generalites": {"numero_dentreprise": numero}},
            "comptes": comptes
        }

        # Pagination si plusieurs pages
        total_pages = data.get("totalPages", 1)
        if page + 1 < total_pages:
            next_page = page + 1
            next_url = (
                f"https://consult.cbso.nbb.be/api/rs-consult/published-deposits?"
                f"page={next_page}&size=10&enterpriseNumber={numero}&"
                f"sort=periodEndDate,desc&sort=depositDate,desc"
            )
            yield scrapy.Request(
                url=next_url,
                callback=self.parse,
                meta={"numero": numero, "page": next_page},
                dont_filter=True
            )
