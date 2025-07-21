from flask import Flask, request, jsonify
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

app = Flask(__name__)

@app.route('/sendEmail', methods=['POST'])
def send_email():
    data = request.json
    recipient = data.get("to")
    link = data.get("link")

    smtp_server = "smtp.office365.com"
    smtp_port = 587
    sender_email = "your_email@yourdomain.com"
    sender_password = "your_app_password"

    subject = "Your Report is Ready"
    html = f"""<p>Hello,<br><br>
               Click <a href="{link}">here</a> to view your report.<br><br>
               Regards,<br>Team</p>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = recipient
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient, msg.as_string())
        return jsonify({"message": "Email sent successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500



 "error": "(535, b'5.7.139 Authentication unsuccessful, SmtpClientAuthentication is disabled for the Tenant. Visit https://aka.ms/smtp_auth_disabled for more information. [SG2PR02CA0040.apcprd02.prod.outlook.com 2025-07-21T10:26:08.277Z 08DDC79214586544]')",
