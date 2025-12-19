import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import json
from database import SessionLocal
import models

class AlertService:
    def get_email_config(self):
        db = SessionLocal()
        try:
            config = db.query(models.SystemConfig).filter(models.SystemConfig.key == "email_config").first()
            if config and config.value:
                return json.loads(config.value)
        finally:
            db.close()
        return None

    def send_email(self, subject: str, body: str):
        config = self.get_email_config()
        
        # Fallback to env vars if not in DB (for backward compatibility or testing)
        smtp_server = config.get("smtp_server") if config else os.getenv("SMTP_SERVER", "smtp.gmail.com")
        smtp_port = int(config.get("smtp_port")) if config else int(os.getenv("SMTP_PORT", "587"))
        sender_email = config.get("sender_email") if config else os.getenv("SENDER_EMAIL", "")
        sender_password = config.get("sender_password") if config else os.getenv("SENDER_PASSWORD", "")
        receiver_email = config.get("receiver_email") if config else os.getenv("RECEIVER_EMAIL", "")

        if not sender_email or not sender_password:
            print(f"[MOCK EMAIL] To: {receiver_email}, Subject: {subject}, Body: {body}")
            return {"ok": True, "mocked": True, "receiver_email": receiver_email, "error": None}

        try:
            msg = MIMEMultipart()
            msg["From"] = sender_email
            msg["To"] = receiver_email
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain"))

            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
            server.quit()
            print(f"Email sent to {receiver_email}")
            return {"ok": True, "mocked": False, "receiver_email": receiver_email, "error": None}
        except Exception as e:
            print(f"Failed to send email: {e}")
            return {"ok": False, "mocked": False, "receiver_email": receiver_email, "error": str(e)}

alert_service = AlertService()
