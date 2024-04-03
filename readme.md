# Realestate Scraper

## Activate Virtual Environment

    source ../environments/realestate_scraper/bin/activate

## Execute scraper
    scrapy crawl PortalInmobiliario

## Execute with crontab
    0 6 * * * /bin/bash /home/choribread/realestate_scraper/run.sh

## Use Data Base MongoDB

### Init MongoDB
    mongosh

### Init Data Base
    use portalinmobiliario

### Documentos
- snapshot
- incremental
- details

### Examples for queries in portalinmob table
    db.snapshot.countDocuments()
    db.snapshot.find()
    db.incremental.distinct("fecha_hora")
    db.incremental.deleteMany({'fecha_hora': '12-03-2024 04:47:12'})