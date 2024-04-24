import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from scrapy.utils.project import get_project_settings
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def send_status_email():
    # Configura los parámetros del correo electrónico
    sender_email = os.getenv('EMAIL_RS')
    password = os.getenv('PSWD_RS')
    lista_emails = os.getenv('DIST_RS')

    # Comprobar que las credenciales necesarias están disponibles
    if not sender_email or not password:
        print("Error: Falta configurar la dirección de correo electrónico del remitente o la contraseña.")
        return  # Retorna prematuramente si faltan credenciales

    # Si no se proporciona una lista de destinatarios, busca una variable de entorno
    if lista_emails:
        receiver_emails = lista_emails.split(',')
    else:
        print("No se encontró la variable de entorno DIST_RS")
        return
    
    # Configura parametros de MongoDB
    client = MongoClient(get_project_settings().get('MONGO_URI'))
    db = client[get_project_settings().get('MONGO_DATABASE')]
    collection_log = db[get_project_settings().get('MONGO_COLLECTION_LOG')]
    
    # Obtiene estadisticas de MongoDB
    n_exitosos = collection_log.count_documents({"tag.email": 0, "resultado": "exitoso"})
    n_fallidos = collection_log.count_documents({"tag.email": 0, "resultado": "fallido"})
    
    if n_exitosos + n_fallidos > 0:
    
        uuid_fallidos = collection_log.find({'resultado': 'fallido', 'tag.email': 0}, {'uuid': 1, '_id': 0})
        uuid_fallidos = [doc['uuid'] for doc in uuid_fallidos]
        
        pipeline_fechas = [
            {"$match": {"tag.email": 0}},
            {"$group": {"_id": None,"fecha_inicio_min": {"$min": "$fecha_inicio"},"fecha_fin_max": {"$max": "$fecha_fin"}}}
        ]
        result_fechas = list(collection_log.aggregate(pipeline_fechas))
        
        if result_fechas:
            # Obtener las fechas del resultado
            fecha_inicio_min = result_fechas[0]['fecha_inicio_min']
            fecha_fin_max = result_fechas[0]['fecha_fin_max']
            
            # Formatear fechas
            fecha_inicio_str = fecha_inicio_min.strftime("%d-%m-%Y %H:%M:%S")
            fecha_fin_str = fecha_fin_max.strftime("%d-%m-%Y %H:%M:%S")
            
            # Calcular el tiempo total del proceso
            tiempo_total = fecha_fin_max - fecha_inicio_min
            tiempo_total_str = str(tiempo_total).split('.')[0]

        # Cuerpo
        html = f"""
        <html>
        <body style="color: black; font-family: Arial, sans-serif;">
        <h2>Status Realestate Project:</h2>
        <p><strong>Inicio del proceso:</strong> {fecha_inicio_str}</p>
        <p><strong>Termino del proceso:</strong> {fecha_fin_str}</p>
        <p><strong>Tiempo total proceso:</strong> {tiempo_total_str}</p>
        <p><strong>Total de scrapeos exitosos:</strong> {n_exitosos}</p>
        <p><strong>Total de scrapeos fallidos:</strong> {n_fallidos}</p>
        {f'<p><strong>UUIDs fallidos:</strong><br>{"".join(f"<span>{uuid}</span><br>" for uuid in uuid_fallidos)}</p>' if n_fallidos > 0 else ''}
        <p>Saludos,<br>Quicksort System</p>
        </body>
        </html>
        """
    else:
        # Cuerpo
        html = f"""
        <html>
        <body style="color: black; font-family: Arial, sans-serif;">
        <h2>Status Realestate Project:</h2>
        <p><strong>No hay novedades</strong></p>
        <p>Saludos,<br>Quicksort System</p>
        </body>
        </html>
        """
    
    # Crea el objeto mensaje
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = ", ".join(receiver_emails)
    message["Subject"] = "Proceso terminado exitosamente" if n_fallidos == 0 else "Proceso terminado con fallas"

    # Añade el cuerpo al correo
    message.attach(MIMEText(html, "html"))

    # Inicia la sesión SMTP y envía el correo
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()  # Activa la encriptación TLS
        server.login(sender_email, password)
        text = message.as_string()
        server.sendmail(sender_email, receiver_emails, text)
        print("Correo enviado con éxito")
    except smtplib.SMTPAuthenticationError:
        print("Error: Fallo en la autenticación. Verifica tu nombre de usuario y contraseña.")
    except smtplib.SMTPException as e:
        print(f"Error SMTP: {e}")
    except Exception as e:
        print(f"Error al enviar el correo: {e}")
    finally:
        server.quit()
        
    # Actualiza documentos tag.email = 0
    collection_log.update_many({"tag.email": 0}, {"$set": {"tag.email": 1}})

