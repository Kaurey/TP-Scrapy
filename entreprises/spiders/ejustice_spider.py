import re
import scrapy
from urllib.parse import urljoin
from pymongo import MongoClient


class EJusticeSpider(scrapy.Spider):
    name = "ejustice"

    custom_settings = {
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "DOWNLOAD_DELAY": 1.5,
        "CONCURRENT_REQUESTS": 1,
        "ROBOTSTXT_OBEY": False,
        "LOG_LEVEL": "INFO"
    }

    def start_requests(self):
        client = MongoClient("mongodb://localhost:27017")
        db = client["entreprises"]
        collection = db["entreprises"]

        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
            "Referer": "https://www.ejustice.just.fgov.be/"
        }

        for doc in collection.find({}, {"_id": 1}):
            numero = doc["_id"]
            url = f"https://www.ejustice.just.fgov.be/cgi_tsv/list.pl?language=fr&sum_date=&page=1&btw={numero}"
            self.logger.info(f"Création de la requête pour KBO {numero}")
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={"numero": numero},
                headers=headers,
                dont_filter=True
            )

    def parse(self, response):
        numero = response.meta["numero"]
        blocs = response.xpath('//div[contains(@class,"list-item--content")]')

        publications = []
        for pub in blocs:
            # --- TITRE propre ---
            titre_raw = pub.xpath('.//p[contains(@class,"list-item--subtitle")]//text()').getall()
            titre = " ".join(t.strip() for t in titre_raw if t.strip())
            titre = re.sub(r'^\d+\)\s*', '', titre).strip()

            # --- Lignes principales ---
            lines = [
                l.strip()
                for l in pub.xpath('.//a[contains(@class,"list-item--title")]//text()').getall()
                if l.strip()
            ]

            adresse = code = type_pub = date_ref = reference_pub = ""

            for l in lines:
                if re.match(r"\d{3}\.\d{3}\.\d{3}", l):  # Numéro entreprise
                    code = l
                elif re.match(r"\d{4}-\d{2}-\d{2}", l):  # Date et référence
                    parts = l.split("/")
                    if len(parts) == 2:
                        date_ref, reference_pub = parts[0].strip(), parts[1].strip()
                    else:
                        date_ref = l.strip()
                elif "KBO" in l.upper() or "BENOEMING" in l.upper() or "ONT" in l.upper():
                    type_pub = l
                elif not adresse:
                    adresse = l

            # --- URL image/PDF ---
            url_image = pub.xpath('.//a[@class="standard"]/@href').get(default="").strip()
            url_image = urljoin(response.url, url_image) if url_image else ""

            publications.append({
                "titre": titre,
                "adresse": adresse,
                "code": code,
                "type_publication": type_pub,
                "date_publication": date_ref,
                "reference_publication": reference_pub,
                "url_image": url_image
            })

        yield {
            "kbo": {"generalites": {"numero_dentreprise": numero}},
            "publications": publications
        }

