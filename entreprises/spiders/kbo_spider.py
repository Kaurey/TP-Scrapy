import scrapy
import pandas as pd
from entreprises.items import EntrepriseItem


class KboSpider(scrapy.Spider):
    name = "kbo_spider"

    # Chemin vers ton CSV
    csv_file = "entreprises.csv"

    # Nombre d’entreprises à tirer aléatoirement
    n_entreprises = 10

    async def start(self):
        """Nouvelle méthode asynchrone recommandée par Scrapy 2.13+"""
        # Lecture du CSV
        try:
            df = pd.read_csv(self.csv_file, sep=";", dtype=str)
        except FileNotFoundError:
            self.logger.error(f"Fichier CSV introuvable : {self.csv_file}")
            return
        except Exception as e:
            self.logger.error(f"Erreur lecture CSV : {e}")
            return

        # Normalisation du nom de colonne 'numero'
        df.columns = [col.strip().lower() for col in df.columns]

        if "numero" not in df.columns:
            self.logger.error("La colonne 'numero' est introuvable dans le CSV.")
            return

        # Nettoyer les numéros → enlever les points
        df["numero"] = df["numero"].str.replace(".", "", regex=False)

        # Tirage aléatoire de N entreprises différentes à chaque lancement
        for numero in df.sample(n=self.n_entreprises)["numero"]:
            url = f"https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html?lang=fr&ondernemingsnummer={numero}"
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={"numero": numero},
                headers={"Accept-Language": "fr"}
            )

    def parse(self, response):

        def clean_text(text):
            if text:
                return " ".join(text.replace("\n", " ").replace("\r", " ").split())
            return ""

        generalites = {}

        # Trouver le tr "Généralités"
        tr_start = response.xpath('//tr[td/h2[contains(text(), "Généralités")]]')
        if tr_start:
            # Boucler sur tous les tr suivants
            for tr in tr_start.xpath('./following-sibling::tr'):
                # Si on atteint un autre <h2>, on stoppe
                if tr.xpath('.//h2'):
                    break

                tds = tr.xpath('./td')
                if len(tds) < 2:
                    continue

                key = clean_text(tds[0].xpath('string(.)').get())
                key_normalized = key.lower().replace("’", "").replace("'", "").replace(" ", "_").replace(":", "")

                value_texts = []
                for td in tds[1:]:
                    value_texts.extend(td.xpath('.//text()').getall())
                value = clean_text(" ".join(value_texts))

                if key_normalized:
                    generalites[key_normalized] = value

        item = EntrepriseItem()
        item['kbo'] = {
            'generalites': generalites
        }
        yield item

