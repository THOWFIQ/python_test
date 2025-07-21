from flask import Flask, request, jsonify
import win32com.client

app = Flask(__name__)

@app.route('/forwardEmailWithLink', methods=['POST'])
def forward_email_with_link():
    try:
        data = request.get_json()
        subject_filter = data.get('subject')  # e.g. "TSS Report"
        recipient_email = data.get('to')      # e.g. "someone@example.com"
        link = data.get('link')               # e.g. "https://example.com/report"

        if not all([subject_filter, recipient_email, link]):
            return jsonify({'error': 'Missing subject, to, or link'}), 400

        outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
        inbox = outlook.GetDefaultFolder(6)  # 6 = inbox
        messages = inbox.Items
        messages.Sort("[ReceivedTime]", True)

        for message in messages:
            if subject_filter.lower() in message.Subject.lower():
                forward = message.Forward()
                forward.To = recipient_email

                # Add link in HTML body
                html_link = f'<p>Hello,<br><br>Please check the report using the link below:<br>' \
                            f'<a href="{link}">{link}</a><br><br>' \
                            f'--- Original Message Below ---<br></p>'
                forward.HTMLBody = html_link + message.HTMLBody

                forward.Subject = f"FWD: {message.Subject}"
                forward.Send()

                return jsonify({'message': f'Email forwarded to {recipient_email}'}), 200

        return jsonify({'error': 'No matching email found'}), 404

    except Exception as e:
        return jsonify({'error': str(e)}), 500

