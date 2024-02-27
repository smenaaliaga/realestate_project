# Realestate Scraper

## Execute scraper
    scrapy crawl PortalInmobiliario

## Use Data Base MongoDB

### Init MongoDB
    mongosh

### Init Data Base
    use portalinmob

### Documentos
- snapshot
- incremental

### Examples for queries in portalinmob table
    db.snapshot.countDocuments()
    db.snapshot.find()
    db.snapshot.deleteMany({'fecha_hora': '20-02-2024 02:10:09'})