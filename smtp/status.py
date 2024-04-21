import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Configura los parámetros del correo electrónico
sender_email = os.getenv('EMAIL')
password = os.getenv('PSWE')

lista_emails = os.getenv('LIST_RECIV')
if lista_emails:
    # Convertir la cadena en una lista
    receiver_emails = lista_emails.split(',')
else:
    print("No se encontró la variable de entorno LIST_RECIV")

subject = "Test subject"
body = "Test body"

# Crea el objeto mensaje
message = MIMEMultipart()
message["From"] = sender_email
message["To"] = ", ".join(receiver_emails)
message["Subject"] = subject

# Añade el cuerpo al correo
message.attach(MIMEText(body, "plain"))

# Inicia la sesión SMTP y envía el correo
try:
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()  # Activa la encriptación TLS
    server.login(sender_email, password)
    text = message.as_string()
    server.sendmail(sender_email, receiver_emails, text)
    print("Correo enviado con éxito")
except Exception as e:
    print(f"Error al enviar el correo: {e}")
finally:
    server.quit()
