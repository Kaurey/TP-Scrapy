import scrapy
import pandas as pd
from entreprises.items import EntrepriseItem

class ConsultSpider(scrapy.Spider):
    name = "consult"

    def start_requests(self):
        df = pd.read_csv("entreprises.csv")
        for numero in df["numero"]:
            url = f"https://consult.cbso.nbb.be/consult-enterprise/{numero}"
            yield scrapy.Request(url, callback=self.parse, meta={"numero": numero})

    def parse(self, response):
        numero = response.meta["numero"]
        item = EntrepriseItem()
        item["numero"] = numero

        comptes = []
        for row in response.css("div.deposit"):
            comptes.append({
                "titre": row.css("h3::text").get(),
                "reference": row.css(".ref::text").get(),
                "date_depot": row.css(".date-depot::text").get(),
                "fin_exercice": row.css(".fin-exercice::text").get(),
                "langue": row.css(".langue::text").get()
            })

        item["comptes"] = comptes
        yield item
