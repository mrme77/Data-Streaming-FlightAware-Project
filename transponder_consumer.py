'''
Auhtor: Pasquale Salomone
Date: Septmber 26, 2023
'''
import pika
import time
import csv
import smtplib
from email.mime.text import MIMEText
from collections import deque
import configparser

# Load the configuration parameters from a file
config = configparser.ConfigParser()
config.read('config.ini')

# Get the configuration parameters
rabbit_host = config['RabbitMQ']['rabbit_host']
rabbit_port = int(config['RabbitMQ']['rabbit_port'])
smtp_port = config['Gmail']['smtp_port']
smtp_password = config['Gmail']['smtp_password']
sender = config['Gmail']['sender']
recipients = config['Gmail']['recipients']



# Transponder configuration
transponder_queue = 'transponder_queue'
TRANSPODER_DEQUE_MAX_LENGTH = 100000

# Create a deque to store transponder readings
transponder_deque = deque(maxlen=TRANSPODER_DEQUE_MAX_LENGTH)

# CSV file configuration
csv_filename = 'transponder_messages.csv'

# Create a set to store unique message keys (aircraft ICAO ID and transponder code)
unique_message_keys = set()

# Email configuration
smtp_server = 'smtp.gmail.com'
smtp_port = smtp_port
smtp_username = sender
smtp_password = smtp_password
sender_email = sender
recipient_email = recipients

def send_email_alert(subject, message):
    """Sends an email alert with the specified subject and message.

    Args:
        subject: The subject of the email alert.
        message: The message of the email alert.

    Returns:
        None.
    """
    try:
        # Create a secure SMTP connection
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_username, smtp_password)

        # Create the email message
        msg = MIMEText(message)
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject

        # Send the email
        server.sendmail(sender_email, recipient_email, msg.as_string())
        server.quit()
    except Exception as e:
        print(f"Error sending email: {str(e)}")

def show_transponder_alert(timestamp, transponder):
    """Prints a transponder alert message to the console and sends an email alert.

    Args:
        timestamp: The timestamp of the transponder alert.
        transponder: The transponder code that triggered the alert.

    Returns:
        None.
    """
    print(f"Transponder Alert at: {timestamp}, Transponder: {transponder}")
    send_email_alert(f"Transponder Alert: {transponder} received", f"Timestamp: {timestamp}, Transponder: {transponder}")

def transponder_callback(ch, method, properties, body):
    try:
        # Decode the message from bytes to a string
        body_str = body.decode('utf-8')
        # Check if the message is a heartbeat message
        if body_str == "Heartbeat Message":
            # Ignore heartbeat messages
            return
        fields = body_str.split(',')

        # Extract relevant information
        type_msg = fields[0]
        aircraft_icao_id = fields[1]
        first_date = fields[2]
        first_timestamp = fields[3]
        transponder = fields[4]

        # Create a unique message key using aircraft ICAO ID and transponder code
        message_key = f"{aircraft_icao_id}-{transponder}"

        # Check if this message key has already been processed
        if message_key not in unique_message_keys:
            unique_message_keys.add(message_key)  # Add the message key to the set of unique keys

            # Add CSV headers
            csv_headers = ['type_msg', 'aircraft_icao_id', 'first_date', 'first_timestamp', 'transponder']

            # Add the transponder reading to the deque
            transponder_deque.append(transponder)

            print(f"Received data for aircraft ICAO ID: {aircraft_icao_id} / {transponder}")

            # Write the message to the CSV file
            csv_exists = False
            try:
                with open(csv_filename, 'r') as csv_file:
                    csv_exists = True
            except FileNotFoundError:
                pass

            with open(csv_filename, mode='a', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)

                # Write headers if the file is newly created
                if not csv_exists:
                    csv_writer.writerow(csv_headers)

                csv_writer.writerow([type_msg, aircraft_icao_id, first_date, first_timestamp, transponder])

            print(f"Received transponder code: {transponder}")

            # Check if the transponder value is one of 7600, 7500, or 7700, and trigger the alert
            # added 0621 for testing the alert
            if transponder in ['7600', '7500', '7700','0621']:
                current_time = time.strftime('%Y-%m-%d %H:%M:%S')
                show_transponder_alert(current_time, transponder)
    except ValueError:
        print("Invalid transponder value in message body.")
    except Exception as e:
        print(f"Error processing message: {str(e)}")
def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_host, port=rabbit_port,heartbeat=600))
    channel = connection.channel()

    channel.queue_declare(queue=transponder_queue, durable=True)

    channel.basic_consume(queue=transponder_queue, on_message_callback=transponder_callback, auto_ack=True)

    print("Transponder Consumer is waiting for messages. To exit, press Ctrl+C")
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\nExiting peacefully...")
