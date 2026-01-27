import smtplib
from email.mime.text import MIMEText

GMAIL_SERVER = 'smtp.gmail.com'
SERVER_PORT = 587
SENDER_EMAIL = 'sstie720@gmail.com'
APP_PASSWORD = "ycrndfhcuxqefcis" # used for apps that don't have access to interactive sign in option

def send_email(receiver_email, subject, message):
    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['From'] = SENDER_EMAIL
    msg['To'] = receiver_email

    with smtplib.SMTP(GMAIL_SERVER, SERVER_PORT) as server:
      server.starttls()
      server.login(SENDER_EMAIL, APP_PASSWORD)
      server.send_message(msg)