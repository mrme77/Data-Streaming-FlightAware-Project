# Data_Streaming_FlightAware_Project
- Author : Pasquale Salomone
- Date: September 26, 2023

## Overview

The Module7-FlightAwareProject is a streaming analytics project designed to process and analyze flight data in real-time. This project aims to provide insights into flight-related information and demonstrate the use of message brokers and consumers to handle streaming data.

## Data Sources

The project utilizes data from FlightAware, a leading provider of aviation data and flight tracking information. The original data sources include various flight-related information such as aircraft identifiers, timestamps, altitude, latitude, longitude, speed, heading, and more.

- [FlightAware](https://www.flightaware.com/)

## Process

### Producers

1. **flight_data_producer.py**: This producer script fetches flight data from FlightAware or a similar source and publishes it to RabbitMQ message brokers. It simulates the continuous generation of flight data for real-time processing.

### Consumers

1. **adsb_data_consumer.py**: This consumer script listens to the "adsb_data_queue" and processes Automatic Dependent Surveillance–Broadcast (ADS-B) data, including fields like aircraft ICAO ID, timestamp, altitude, latitude, longitude, speed, and heading. It stores this data in a CSV file.

2. **nav_data_consumer.py**: This consumer script listens to the "nav_data_queue" and processes navigation-related data, including speed and heading. It also stores this data in a CSV file.

3. **aircraft_icao_id_consumer.py**: This consumer script listens to the "aircraft_icao_id_queue" and processes aircraft ICAO ID data, including type messages and timestamps. It stores this data in a CSV file.

4. **transponder_consumer.py**: This consumer script listens to the "transponder_queue" and processes transponder data, including type messages, aircraft ICAO ID, timestamps, and transponder values. It stores this data in a CSV file.

## Output

The output of this streaming analytics project includes several CSV files, each containing specific flight-related information:

- **adsb_data_messages.csv**: Contains ADS-B data, including altitude, latitude, longitude, speed, and heading.

- **nav_data_messages.csv**: Contains navigation data, including speed and heading.

- **aircraft_icao_id_messages.csv**: Contains aircraft ICAO ID data, including type messages and timestamps.

- **transponder_messages.csv**: Contains transponder data, including type messages, aircraft ICAO ID, timestamps, and transponder values.

## How to Run

Readers don't need to download or execute Python code to verify the results. Instead, the project outputs are provided in CSV files for easy access and analysis.

Screenshots of RabbitMQ queues, execution of consumer scripts in separate terminals, and sample data are included in this repository to illustrate the project's functionality.

For a step-by-step guide on running the project and viewing the results, please refer to the documentation and screenshots provided in the repository.

## Screenshots

![RabbitMQ Queue - adsb_data_queue](screenshots/rabbitmq_adsb_data_queue.png)

![Terminal - Running adsb_data_consumer.py](screenshots/terminal_adsb_data_consumer.png)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---
