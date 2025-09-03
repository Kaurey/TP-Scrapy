import scrapy
import pandas as pd
from entreprises.items import EntrepriseItem

class EJusticeSpider(scrapy.Spider):
    name = "ejustice"

    def start_requests(self):
        df = pd.read_csv("entreprises.csv")
        for numero in df["numero"]:
            url = f"https://www.ejustice.just.fgov.be/cgi_tsv/list.pl?btw={numero}"
            yield scrapy.Request(url, callback=self.parse, meta={"numero": numero})

    def parse(self, response):
        numero = response.meta["numero"]
        item = EntrepriseItem()
        item["numero"] = numero

        publications = []
        for row in response.css("table tr"):
            publications.append({
                "numero_pub": row.css("td:nth-child(1)::text").get(),
                "titre": row.css("td:nth-child(2)::text").get(),
                "type": row.css("td:nth-child(3)::text").get(),
                "date": row.css("td:nth-child(4)::text").get(),
                "reference": row.css("td:nth-child(5)::text").get(),
                "url_image": row.css("td img::attr(src)").get()
            })

        item["publications"] = publications
        yield item
