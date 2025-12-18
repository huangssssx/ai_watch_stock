import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

class AlertService:
    def __init__(self):
        self.smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.sender_email = os.getenv("SENDER_EMAIL", "")
        self.sender_password = os.getenv("SENDER_PASSWORD", "")
        self.receiver_email = os.getenv("RECEIVER_EMAIL", "")

    def send_email(self, subject: str, body: str):
        if not self.sender_email or not self.sender_password:
            print(f"[MOCK EMAIL] To: {self.receiver_email}, Subject: {subject}, Body: {body}")
            return

        try:
            msg = MIMEMultipart()
            msg["From"] = self.sender_email
            msg["To"] = self.receiver_email
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain"))

            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.sender_email, self.sender_password)
            server.send_message(msg)
            server.quit()
            print(f"Email sent to {self.receiver_email}")
        except Exception as e:
            print(f"Failed to send email: {e}")

alert_service = AlertService()
