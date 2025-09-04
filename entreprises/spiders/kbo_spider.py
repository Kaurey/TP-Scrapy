import re
import unicodedata
import scrapy
import pandas as pd
from entreprises.items import EntrepriseItem


class KboSpider(scrapy.Spider):
    name = "kbo_spider"

    csv_file = "entreprises.csv"
    n_entreprises = 10

    async def start(self):
        try:
            df = pd.read_csv(self.csv_file, sep=";", dtype=str)
        except FileNotFoundError:
            self.logger.error(f"Fichier CSV introuvable : {self.csv_file}")
            return
        except Exception as e:
            self.logger.error(f"Erreur lecture CSV : {e}")
            return

        df.columns = [col.strip().lower() for col in df.columns]

        if "numero" not in df.columns:
            self.logger.error("La colonne 'numero' est introuvable dans le CSV.")
            return

        df["numero"] = df["numero"].str.replace(".", "", regex=False)

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
            if not text:
                return ""
            return " ".join(text.replace("\n", " ").replace("\r", " ").split())

        def slug_key(s: str) -> str:
            s = (s or "").strip().lower().replace("’", "'")
            s = unicodedata.normalize("NFKD", s)
            s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
            s = s.replace(":", "")
            s = re.sub(r"\s+", "_", s)
            s = s.replace("d'entreprise", "dentreprise")
            return s

        generalites = {}
        fonctions = []
        capacites = []
        qualites = []
        autorisations = []
        nace = {}
        nace_2025 = []
        nace_2008 = []
        nace_2003 = []
        donnees_financieres = []
        liens_entre_entites = []
        liens_externes = []

        section = None

        for tr in response.xpath('//tr'):
            h2_text = tr.xpath('.//h2/text()').get()
            if h2_text:
                titre = clean_text(h2_text).lower()
                if "généralités" in titre:
                    section = "generalites"
                elif "fonctions" in titre:
                    section = "fonctions"
                elif "capacités entrepreneuriales" in titre:
                    section = "capacites"
                elif "qualités" in titre:
                    section = "qualites"
                elif "autorisations" in titre:
                    section = "autorisations"
                elif "activités tva code nacebel version 2025" in titre:
                    section = "nace_2025"
                elif "activités onss code nacebel version 2025" in titre:
                    section = "nace_2025"
                elif "données financières" in titre:
                    section = "donnees_financieres"
                elif "liens entre entités" in titre:
                    section = "liens_entre_entites"
                elif "liens externes" in titre:
                    section = "liens_externes"
                else:
                    section = None
                continue

            tds = tr.xpath('./td')
            if not tds:
                continue

            # Généralités
            if section == "generalites" and len(tds) >= 2:
                raw_label = clean_text(tds[0].xpath('string(.)').get())
                if ":" in raw_label:
                    key = slug_key(raw_label)
                    fragments = [clean_text(t) for t in tds[1].xpath('.//text()').getall()]
                    value = " ".join(f for f in fragments if f)
                    value = re.sub(r"(.*)(Depuis.*)", r"\1 \2", value)
                    if key and value:
                        generalites[key] = value

            # Fonctions
            elif section == "fonctions" and len(tds) >= 2:
                fragments = [" ".join(clean_text(x) for x in td.xpath('.//text()').getall()) for td in tds[:3]]
                fonctions.append({
                    "fonction": fragments[0] if len(fragments) > 0 else "",
                    "personne": fragments[1] if len(fragments) > 1 else "",
                    "info": fragments[2] if len(fragments) > 2 else ""
                })

            # Capacités entrepreneuriales
            elif section == "capacites":
                fragments = [clean_text(t) for t in tds[0].xpath('.//text()').getall()]
                texte = " ".join(f for f in fragments if f)
                if texte and "pas de données" not in texte.lower():
                    capacites.append(texte)

            # Qualités
            elif section == "qualites":
                fragments = [clean_text(t) for t in tds[0].xpath('.//text()').getall()]
                texte = " ".join(f for f in fragments if f)
                texte = re.sub(r"(.*)(Depuis.*)", r"\1 \2", texte)
                if texte and "pas de données" not in texte.lower():
                    qualites.append(texte)

            # Autorisations
            elif section == "autorisations":
                fragments = [clean_text(t) for t in tds[0].xpath('.//text()').getall()]
                texte = " ".join(f for f in fragments if f)
                if texte and "pas de données" not in texte.lower():
                    autorisations.append(texte)

            elif section == "nace_2025":
                fragments = [clean_text(t) for t in tds[0].xpath('.//text()').getall()]
                texte = " ".join(f for f in fragments if f)
                # Ignorer les liens "Montrez / Masquer"
                if texte and not texte.lower().startswith("montrez") and not texte.lower().startswith("masquer"):
                    nace_2025.append(texte)

        # --- NACE 2008 (TVA + ONSS) ---
        for tr in response.xpath('//table[contains(@id,"toonbtw2008")]//tr'):
            fragments = [clean_text(t) for t in tr.xpath('.//td//text()').getall()]
            texte = " ".join(f for f in fragments if f)
            # ignorer les lignes vides ou Montrez/Masquer
            if texte and not texte.lower().startswith("montrez") and not texte.lower().startswith("masquer"):
                # ajouter uniquement les lignes TVA ou ONSS
                if "TVA 2008" in texte or "ONSS2008" in texte:
                    texte = texte.replace("TVA2008", "TVA 2008 ")
                    texte = texte.replace("ONSS2008", "ONSS 2008")
                    nace_2008.append(texte)

        # --- NACE 2003 (TVA + ONSS) ---
        for tr in response.xpath('//table[contains(@id,"toonbtw")]//tr'):
            fragments = [clean_text(t) for t in tr.xpath('.//td//text()').getall()]
            texte = " ".join(f for f in fragments if f)
            if texte and not texte.lower().startswith("montrez") and not texte.lower().startswith("masquer"):
                if "TVA2003" in texte or "ONSS2003" in texte:
                    texte = texte.replace("TVA2003", "TVA 2003 ")
                    texte = texte.replace("ONSS2003", "ONSS 2003")
                    nace_2003.append(texte)


        nace = {
            "2025": nace_2025,
            "2008": nace_2008,
            "2003": nace_2003
        }

        # --- Données financières ---
        h2 = response.xpath('//h2[contains(text(),"Données financières")]')
        donnees_financieres = []

        if h2:
            tr_h2 = h2.xpath('./ancestor::tr')[0]  # tr contenant le h2
            for tr in tr_h2.xpath('./following-sibling::tr'):
                # arrêter si on tombe sur un autre h2
                if tr.xpath('.//h2'):
                    break
                tds = tr.xpath('./td')
                if len(tds) >= 2:
                    key = clean_text(tds[0].xpath('string(.)').get())
                    value = clean_text(tds[1].xpath('string(.)').get())
                    if key and value:
                        donnees_financieres.append({
                            "key": key,
                            "value": value
                        })



        # --- Liens entre entités ---
        h2 = response.xpath('//h2[contains(text(),"Liens entre entités")]')
        if h2:
            tr_h2 = h2.xpath('./ancestor::tr')[0]
            for tr in tr_h2.xpath('./following-sibling::tr'):
                if tr.xpath('.//h2'):
                    break
                texte = clean_text(" ".join(tr.xpath('.//text()').getall()))
                if texte and texte != "&nbsp;" and "pas de données" not in texte.lower():
                    liens_entre_entites.append(texte)


        # --- Liens externes ---
        for tr in response.xpath('//h2[contains(text(),"Liens externes")]/ancestor::table//tr'):
            links = tr.xpath('.//a/@href').getall()
            liens_externes.extend(links)

        item = EntrepriseItem()
        item["kbo"] = {
            "generalites": generalites,
            "fonctions": fonctions,
            "capacites": capacites,
            "qualites": qualites,
            "autorisations": autorisations,
            "nace": nace,
            "donnees_financieres": donnees_financieres,
            "liens_entre_entites": liens_entre_entites,
            "liens_externes": liens_externes
        }

        yield item
