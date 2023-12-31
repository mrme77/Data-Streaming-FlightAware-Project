"""
Author: Pasquale Salomone
Date: September 26, 2023
"""

import pika
import csv
import configparser

# Load the configuration parameters from a file
config = configparser.ConfigParser()
config.read('config.ini')

# Get the configuration parameters
rabbit_host = config['RabbitMQ']['rabbit_host']
rabbit_port = int(config['RabbitMQ']['rabbit_port'])

# CSV file configuration
csv_filename = 'nav_data_messages.csv'

# CSV headers
csv_headers = ['type_msg', 'aircraft_icao_id', 'first_date', 'first_timestamp', 'speed', 'heading']

# Queue name
queue_name = 'nav_data'
def nav_data_callback(ch, method, properties, body):
    """
    Callback function for handling NAV data messages received from RabbitMQ.

    Args:
        ch (pika.Channel): The channel where the message was received.
        method (pika.spec.Basic.Deliver): The method used to deliver the message.
        properties (pika.spec.BasicProperties): The properties of the message.
        body (bytes): The message body as bytes.

    Returns:
        None.
    """    
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
        speed = fields[4]
        heading = fields[5]

        # Check if the CSV file exists, and create it with headers if not
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

            csv_writer.writerow([type_msg, aircraft_icao_id, first_date, first_timestamp, speed, heading])

        print(f"Received ADSB data (speed, heading) for aircraft ICAO ID: {aircraft_icao_id} / {speed} / {heading}")

    except Exception as e:
        print(f"Error processing message: {str(e)}")

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_host, port=rabbit_port,heartbeat=600))
    channel = connection.channel()

    channel.queue_declare(queue= queue_name, durable=True)

    channel.basic_consume(queue= queue_name, on_message_callback=nav_data_callback, auto_ack=True)

    print("NAV Data Consumer is waiting for messages. To exit, press Ctrl+C")
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\nExiting peacefully...")
