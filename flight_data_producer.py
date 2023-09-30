'''
Auhtor: Pasquale Salomone
Date: Septmber 29, 2023
'''

import socket
import pika
import time
import logging
import configparser
import threading
from queue import Queue

# Define the names of the queues
my_queues = {1: 'transponder_queue',
             2: 'adsb_data_queue',
             3: 'aircraft_icao_id_queue',
             4: 'nav_data'
             }

# Define the msg_type
MSG_TYPE_TRANSPONDER = 1
MSG_TYPE_ADSB = 2
MSG_TYPE_AIRCRAFT_ICAO_ID = 3
MSG_NAV_DATA = 4

# Load the configuration parameters from a file
config = configparser.ConfigParser()
config.read('config.ini')

# Get the configuration parameters
piaware_ip = config['PiAware']['IP']
piaware_port = int(config['PiAware']['Port'])
rabbitmq_host = config['RabbitMQ']['Host']

# Set up the logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
logger.addHandler(handler)

def extract_info(text, typemsg):
    """
    Extracts relevant information from a line of text based on the provided message type (typemsg).

    Args:
        text (str): A string containing the line to extract information from.
        typemsg (str): A string indicating the message type (e.g., 'MSG3', 'MSG4', 'MSG6', 'MSG1').

    Returns:
        list: A list containing the extracted information based on the message type. The content of the list
              varies depending on the message type.

    The `extract_info` function parses a line of text, typically representing a specific message type,
    and extracts relevant data fields based on the provided message type. The extracted data is returned
    as a list with different elements depending on the message type.

    For example, if `typemsg` is 'MSG3', the function extracts altitude, latitude, and longitude
    information along with common data like message type, aircraft ICAO ID, date, and timestamp.

    If the message type is unknown or unsupported, the function returns ['unknown'].

    """
    

    fields = text.split(',')

    type_msg = fields[0] + fields[1]  # Message type is always the same for a given line
    # Extract the aircraft ICAO ID.
    aircraft_icao_id = fields[4]
    # Extract the first date.
    first_date = fields[6]
    # Extract the first timestamp.
    first_timestamp = fields[7]

    if typemsg == 'MSG3':

        # Extract the altitude.
        altitude = fields[11]

        # Extract the latitude.
        latitude = fields[14]

        # Extract the longitude.
        longitude = fields[15]

        results = [type_msg, aircraft_icao_id, first_date, first_timestamp, altitude, latitude, longitude]

    elif typemsg == 'MSG4':
        # Extract speed.
        speed = fields[12]

        # Extract heading.
        heading = fields[13]

        results = [type_msg, aircraft_icao_id, first_date, first_timestamp, speed, heading]

    elif typemsg == 'MSG6':
        # Extract IFF (Identification Friend or Foe).
        transponder = fields[17]

        results = [type_msg, aircraft_icao_id, first_date, first_timestamp, transponder]

    elif typemsg == 'MSG1':
        # Extract company ID.
        company_id = fields[10][0:3]

        results = [type_msg, aircraft_icao_id, first_date, first_timestamp, company_id]

    else:
        # Handle unknown message types here if needed
        results = ['unknown']
   
    return results


# Create a buffer queue to hold messages temporarily
message_buffer = Queue(maxsize=100)  # Adjust the buffer size as needed
# Define a lock to ensure thread-safe access to the buffer
buffer_lock = threading.Lock()

def publish_message_to_queue(channel, message_type, body_content):
    """
    Publishes a message to the specified RabbitMQ queue.

    Args:
        channel: A RabbitMQ channel object.
        message_type (int): An integer indicating the message type.
        body_content (str): The content of the message to be sent.

    This function attempts to publish a message to a RabbitMQ queue, logs the message content,
    and handles any AMQP connection errors.
    """

    try:
        channel.basic_publish(exchange='', routing_key=my_queues[message_type], body=body_content)
        logger.info(body_content)
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Error sending message to queue: {str(e)}")
    
    

def process_buffered_messages(channel):
    """
    Processes messages from the buffer and publishes them to RabbitMQ.

    Args:
        channel: A RabbitMQ channel object.

    This function processes messages from the message buffer and publishes them to the appropriate
    RabbitMQ queue. It ensures that messages are sent in the correct order.
    """
    while not message_buffer.empty():
        message = message_buffer.get()
        message_type, body_content = message
        publish_message_to_queue(channel, message_type, body_content)

def send_heartbeat(channel, queue_name):
    """
    Sends heartbeat messages to RabbitMQ at regular intervals.

    Args:
        channel: A RabbitMQ channel object.
        queue_name (str): The name of the RabbitMQ queue for heartbeat messages.

    This function sends heartbeat messages to a specified RabbitMQ queue at regular intervals.
    """
    while True:
        time.sleep(30)  # Send a heartbeat message every 30 seconds

        # Create a heartbeat message (adjust the format as needed)
        heartbeat_message = "Heartbeat Message"

        try:
            channel.basic_publish(exchange='', routing_key=queue_name, body=heartbeat_message)
            logger.info("Sent heartbeat message")
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Error sending heartbeat message: {str(e)}")



def extract_and_send_adsb_data(piaware_ip, piaware_port, rabbitmq_host):
    """
    Connects to PiAware, retrieves aircraft data, and sends it to RabbitMQ queues.

    Args:
        piaware_ip (str): The IP address of the PiAware device.
        piaware_port (int): The port number for the PiAware connection.
        rabbitmq_host (str): The hostname or IP address of the RabbitMQ server.

    This function establishes connections to both PiAware and RabbitMQ, retrieves aircraft data
    from PiAware, processes and sends it to the appropriate RabbitMQ queues, and manages
    the sending of heartbeat messages.
    """
    connection = None
    try:
        # Create a socket to connect to the PiAware device
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Set a timeout for the socket
        sock.settimeout(1500)

        # Connect to the PiAware device
        sock.connect((piaware_ip, piaware_port))
        logger.info(f"Connected to {piaware_ip}:{piaware_port}")

        # Create a connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
        channel = connection.channel()
        channel.queue_declare(queue=my_queues[1], durable=True)
        channel.queue_declare(queue=my_queues[2], durable=True)
        channel.queue_declare(queue=my_queues[3], durable=True)
        channel.queue_declare(queue=my_queues[4], durable=True)

        # Start the heartbeat thread
        heartbeat_thread = threading.Thread(target=send_heartbeat, args=(channel, my_queues[2]))
        heartbeat_thread.daemon = True  # Allow the thread to exit when the main program exits
        heartbeat_thread.start()

        while True:
            try:
                data = sock.recv(4096)

                if not data:
                    logger.info("No data received.")
                    break

                for line in data.decode('utf-8', 'ignore').strip().split('\n'):
                    if line[0:5] == 'MSG,3' and len(line) == 104:
                        message_type = MSG_TYPE_ADSB
                        body_content = ','.join(extract_info(line, line[0:5].replace(',', '')))

                        # Add messages to the buffer queue instead of directly sending them
                        with buffer_lock:
                            message_buffer.put((message_type, body_content))

                    if line[0:5] == "MSG,6" and len(line) >= 86:
                        message_type = MSG_TYPE_TRANSPONDER
                        body_content = ','.join(extract_info(line, line[0:5].replace(',', '')))

                        with buffer_lock:
                            message_buffer.put((message_type, body_content))

                    if line[0:5] == "MSG,1" and len(line) >= 86:
                        message_type = MSG_TYPE_AIRCRAFT_ICAO_ID
                        body_content = ','.join(extract_info(line, line[0:5].replace(',', '')))

                        with buffer_lock:
                            message_buffer.put((message_type, body_content))

                    if line[0:5] == "MSG,4" and len(line) >= 86:
                        message_type = MSG_NAV_DATA
                        body_content = ','.join(extract_info(line, line[0:5].replace(',', '')))

                        with buffer_lock:
                            message_buffer.put((message_type, body_content))

                # Process buffered messages
                process_buffered_messages(channel)

            except socket.timeout:
                logger.info("No data received for 15 seconds. Closing the connection.")
                break
    
            except pika.exceptions.AMQPConnectionError as e:
                for i in range(10):
                    try:
                        channel.basic_publish(exchange='', routing_key=my_queues, body=body_content)
                        # Message sent successfully, no need to continue retrying
                        break
                    except pika.exceptions.AMQPConnectionError:
                          logger.error("AMQP Connection Error: Retrying...")
                          time.sleep(1)
                else:
                  # This part is executed if the loop completes without a successful send
                 logger.error("AMQP Connection Error: Failed to send message after 10 retries")

    
    except ConnectionRefusedError as e:
            logger.error(f"Connection to {piaware_ip}:{piaware_port} refused. Make sure PiAware is running and the IP address is correct: {str(e)}")
    except Exception as e:
             logger.error(f"Error: {str(e)}")
    finally:
        try:
            if 'sock' in locals() and sock is not None:
                sock.close()
        except Exception as e:
            logger.error(f"Error closing socket: {str(e)}")

        try:
            if connection is not None and connection.is_open:
                connection.close()
                connection = None  # Set connection to None to avoid further closing attempts
        except Exception as e:
            logger.error(f"Error closing RabbitMQ connection: {str(e)}")
    

try:
    if __name__ == '__main__':
        # Start extracting and sending filtered ADS-B data to the appropriate queues
        extract_and_send_adsb_data(piaware_ip, piaware_port, rabbitmq_host)
except KeyboardInterrupt:
    print("\nExiting peacefully...")
