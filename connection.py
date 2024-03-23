from pymongo import MongoClient

### Conexion al servidor MongoDB

client = MongoClient('mongodb://X.X.X.X:27017/')
db = client['portalinmobiliario']
collection = db['propiedades']

### Lectura de Base de datos

# 1. Obtiene el primer resultado de la colección
primer_documento = collection.find_one()
print(primer_documento)

# 2. Obtiene todos los registros con fecha máxima de proceso
pipeline = [
    {"$group": {
        "_id": None,
        "maxDate": {"$max": "$dt_process"}
    }}
]
max_date_result = list(collection.aggregate(pipeline))
max_date = max_date_result[0]['maxDate']
print("Fecha máxima:", max_date)
records_with_max_date = list(collection.find({"dt_process": max_date}))
for record in records_with_max_date:
    print(record)

# 3. Obtiene todos los registros que cumplan uno o más filtros
criterios = {"tipo_operacion": "venta", "precio": "4.300"}
registros = collection.find(criterios)
for registro in registros:
    print(registro)

criterios = {"comuna": "providencia", "caracteristicas.Principales.Dormitorios": "3"}
registros = collection.find(criterios)
for registro in registros:
    print(registro)
