import win32com.client

# === Configuration ===
subject_filter = "Your Subject Keyword"
recipient_email = "recipient@example.com"
link_to_include = "https://example.com/report/view?id=123456"

# === Connect to Outlook ===
outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")

# Inbox: 6 = inbox folder
inbox = outlook.GetDefaultFolder(6)
messages = inbox.Items
messages.Sort("[ReceivedTime]", True)

# === Search for Email to Forward ===
forwarded = False
for message in messages:
    if subject_filter in message.Subject:
        # Create forward
        forward = message.Forward()
        forward.To = recipient_email
        
        # Insert link in the top of the body
        original_body = message.Body
        custom_body = f"Hello,\n\nPlease check the report using the link below:\n{link_to_include}\n\n--- Original Message Below ---\n\n{original_body}"
        
        forward.Body = custom_body
        forward.Subject = f"FWD: {message.Subject}"
        forward.Send()

        print(f"Email forwarded to {recipient_email}")
        forwarded = True
        break

if not forwarded:
    print("No email found with the specified subject.")
