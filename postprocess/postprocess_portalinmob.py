from pymongo import MongoClient

def postprocess_portalinmob():
    # Conexión a MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['portalinmobiliario']
    
    # Colecciones
    snapshot_collection = db['snapshot']
    incremental_collection = db['incremental']
    
    print('=== INICIA POST-PROCESAMIENTO ===')

    # Operación de agregación para encontrar documentos en `snapshot` sin correspondencia en `incremental`
    pipeline = [
        {
            '$lookup': {
                'from': 'incremental',  # Nombre de la colección contra la cual realizar el "join"
                'localField': 'url',  # Campo en la colección de origen (snapshot)
                'foreignField': 'url',  # Campo en la colección de destino (incremental)
                'as': 'url_match'  # Nombre del nuevo campo que contiene el resultado del "join"
            }
        }, 
        {
            '$match': {
                'url_match': { '$size': 0 }  # Filtra los documentos que no tienen correspondencia
            }
        },
        {
            '$project': {
                'url_match': 0  # Opcional: Excluye el campo url_match del resultado
            }
        }
    ]

    # Ejecutar la pipeline de agregación
    result = snapshot_collection.aggregate(pipeline)

    # Insertar los documentos resultantes en `incremental`
    new_documents = list(result)  # Convertir el cursor a una lista para poder reutilizarlo
    
    print(f"Total de items con url unicas: {len(new_documents)}")
    
    if new_documents:  # Verificar si hay documentos para insertar
        incremental_collection.insert_many(new_documents)

    # Cierra la conexión a MongoDB
    client.close()
