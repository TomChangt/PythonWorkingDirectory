import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders


class EmailSender:
    def __init__(
        self,
        sender_email: str,
        password: str,
        smtp_server: str = "smtp.exmail.qq.com",
        smtp_port: int = 465,
    ):
        self.sender_email = sender_email
        self.password = password
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port

    def send_email(
        self,
        file_path: str,
        receiver_email: str,
        cc_email: str,
        subject: str,
        body: str,
    ) -> None:
        message = MIMEMultipart()
        message["From"] = self.sender_email
        message["To"] = receiver_email
        message["Cc"] = cc_email
        message["Subject"] = subject

        message.attach(MIMEText(body, "plain"))

        with open(file_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())

        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename= {file_path}")
        message.attach(part)

        with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port) as server:
            server.login(self.sender_email, self.password)
            server.send_message(
                message, to_addrs=[receiver_email] + cc_email.split(",")
            )
