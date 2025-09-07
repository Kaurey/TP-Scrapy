# Entreprises Scrapy Project

Ce projet Scrapy permet de collecter et d'enrichir des données sur les entreprises belges à partir de plusieurs sources publiques (KBO, EJustice, NBB Consult) et de les stocker dans une base MongoDB.

## Fonctionnalités

- Extraction des données générales depuis le site KBO.
- Récupération des publications légales via EJustice.
- Collecte des comptes annuels via NBB Consult.
- Stockage structuré dans MongoDB, avec enrichissement progressif des documents.

## Prérequis

- Python 3.8+
- MongoDB (en local ou distant)
- [Scrapy](https://scrapy.org/)
- [pandas](https://pandas.pydata.org/)
- [pymongo](https://pymongo.readthedocs.io/)

## Installation

1. Clonez ce dépôt :
   ```bash
   git clone <url-du-repo>
   cd entreprises
   ```
2. Assurez-vous que MongoDB tourne sur `localhost:27017` (ou modifiez l'URI dans les settings).

3. Placez un fichier `entreprises.csv` à la racine du projet, contenant au moins une colonne `numero` (numéro d'entreprise).

## Lancement

Pour lancer la collecte complète (KBO → EJustice → Consult) :

```bash
python run_spider.py
```

Les données seront stockées dans la base MongoDB `entreprises`, collection `entreprises`.

## Structure des données

Chaque document MongoDB contient :

- `kbo`: Données générales (généralités, fonctions, capacités, etc.)
- `publications`: Liste des publications légales (EJustice)
- `comptes`: Liste des comptes annuels (NBB Consult)

## Personnalisation

- Modifiez `n_entreprises` dans `kbo_spider.py` pour ajuster le nombre d'entreprises traitées.
- Adaptez les paramètres MongoDB dans `settings.py` si besoin.

## Dépendances principales

- Scrapy
- pandas
- pymongo

## Auteurs

- [Votre nom]

## Licence

MIT
