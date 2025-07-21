from flask import Flask, request, jsonify
import win32com.client
import pythoncom  # ← Needed to initialize COM in Flask context

app = Flask(__name__)

@app.route('/forwardEmailWithLink', methods=['POST'])
def forward_email_with_link():
    try:
        # Initialize COM for this thread
        pythoncom.CoInitialize()

        data = request.get_json()
        subject_filter = data.get('subject')
        recipient_email = data.get('to')
        link = data.get('link')

        if not all([subject_filter, recipient_email, link]):
            return jsonify({'error': 'Missing subject, to, or link'}), 400

        outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
        inbox = outlook.GetDefaultFolder(6)  # 6 = Inbox
        messages = inbox.Items
        messages.Sort("[ReceivedTime]", True)

        for message in messages:
            if subject_filter.lower() in message.Subject.lower():
                forward = message.Forward()
                forward.To = recipient_email

                forward.HTMLBody = (
                    f"<p>Hello,<br><br>"
                    f"Please check the report: <a href='{link}'>{link}</a><br><br>"
                    f"--- Original Message Below ---<br></p>"
                    + message.HTMLBody
                )

                forward.Subject = f"FWD: {message.Subject}"
                forward.Send()

                return jsonify({'message': f'Email forwarded to {recipient_email}'}), 200

        return jsonify({'error': 'No matching email found'}), 404

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        pythoncom.CoUninitialize()
