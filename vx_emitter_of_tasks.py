"""
    BR was able to receive the second message
  Creating a program that will run all of the tasks from the .csv file 

"""

import pika
import sys
import webbrowser
import csv
import os
from util_logger import setup_logger

# Setup logger for logging
logger, logname = setup_logger(__file__)

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    logger.info("User response to monitoring RabbitMQ queues: %s", ans)
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        logger.info("Opened RabbitMQ Admin website")

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        logger.info("Sent message to queue %s: %s", queue_name, message)
        print(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        logger.error("Error: Connection to RabbitMQ server failed: %s", e)
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

def main():
    # ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site()

    # Construct the full path to the tasks.csv file
    tasks_file = os.path.join(os.path.dirname(__file__), "tasks.csv")
    logger.info("Path to tasks.csv file: %s", tasks_file)

    # Load tasks from CSV file
    with open(tasks_file, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            message = " ".join(row)
            # send the message to the queue
            send_message("localhost", "task_queue2", message)

if __name__ == "__main__":
    main()
