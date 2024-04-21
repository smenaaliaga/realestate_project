# Realestate Scraper

## Activate Virtual Environment

    source ../environments/realestate_scraper/bin/activate

## Execute scraper
    scrapy crawl PortalInmobiliario -a tipo_operacion=venta -a tipo_propiedad=departamento -a modalidad=propiedades-usadas -a region=metropolitana -a comuna=nunoa -a barrio=plaza-nunoa -a tipo_url=1

    bash run.sh

## Execute with crontab
    0 6 * * * /bin/bash /home/choribread/realestate_scraper/run.sh

## Use Data Base MongoDB

### Init MongoDB
    mongosh

### Init Data Base
    use portalinmobiliario

### Documentos
- propiedades
- log

### Examples for queries in portalinmob table
    db.propiedades.countDocuments()
    db.propiedades.find()
    db.propiedades.distinct("fecha_obtencion")
    db.propiedades.deleteMany({'fecha_obtencion': '12-03-2024 04:47:12'})