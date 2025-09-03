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
        # Fonction utilitaire pour nettoyer le texte
        def clean_text(text):
            if text:
                return " ".join(text.replace("\n", " ").replace("\r", " ").split())
            return ""

        # Fonction générique pour extraire un champ à partir du label
        def extract_field(label):
            try:
                xpath_expr = f"//td[contains(text(), '{label}')]/following-sibling::td[1]//text()"
                texts = response.xpath(xpath_expr).getall()
                if texts:
                    return clean_text(texts[0])  # <- on prend juste le premier résultat
                return ""
            except Exception as e:
                print(f"Erreur extraction pour {label}: {e}")
                return ""



        item = EntrepriseItem()
        item["kbo"] = {
        "generalites": {
            "numero_officiel": extract_field("Numéro"),
            "statut": extract_field("Statut"),
            "situation_juridique": extract_field("Situation juridique"),
            "date_debut": extract_field("Date de début"),
            "denomination": extract_field("Dénomination"),
            "adresse": extract_field("Adresse du siège"),
            "telephone": extract_field("Numéro de téléphone"),
            "fax": extract_field("Numéro de fax"),
            "email": extract_field("E-mail"),
            "site_web": extract_field("Adresse web"),
            "type_entite": extract_field("Type d'entité"),
            "forme_legale": extract_field("Forme légale"),
            "nb_unites_etablissement": extract_field("Nombre d'unités d'établissement"),
        },
        # "fonctions": {},
        # "capacites_entrepreneuriales": {},
        # "qualites": {},
        # "autorisations": {},
        # "nace": {},
        # "donnees_financieres": {},
        # "liens_entre_entites": {},
        # "liens_externes": {}
    }

        yield item

