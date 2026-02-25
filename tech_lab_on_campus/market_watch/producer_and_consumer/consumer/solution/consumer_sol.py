import os
import pika

from consumer_interface import mqConsumerInterface

class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_key: str, exchange_name: str, queue_name: str):
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.setupRMQConnection()

    def setupRMQConnection(self):
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        
        #channel
        self.connection = pika.BlockingConnection(parameters=conParams)
        self.channel = self.connection.channel()
        
        #exchange
        self.exchange = self.channel.exchange_declare(exchange=self.exchange_name)
        
        #queue
        self.channel.queue_declare(queue=self.queue_name)
        self.channel.queue_bind(queue=self.queue_name, routing_key=self.binding_key, exchange=self.exchange_name)
        self.channel.basic_consume(self.queue_name, self.on_message_callback,auto_ack=False)

    def on_message_callback(self, channel, method_frame, header_frame, body):
        self.channel.basic_ack(method_frame.delivery_tag, False)
        print(body)
        self.connection.close()

    def startConsuming(self):
        print(" [*] Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()

    def __del__(self):
        print("Closing RMQ connection on destruction")
        self.channel.close()
        self.connection.close()