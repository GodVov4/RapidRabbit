from configparser import ConfigParser
from json import loads
from pathlib import Path
from pika import PlainCredentials, BlockingConnection, ConnectionParameters
from time import sleep

from producer import Contact, connect


def send_sms(message: dict) -> None:
    sleep(1)
    contact = Contact.objects(id=message.get('ID'))
    contact.update(ack=True)


def main():
    path = Path(__file__).parent.joinpath('config.ini')
    config = ConfigParser()
    config.read(path)
    user = config.get('DB', 'user')
    password = config.get('DB', 'pass')
    db = config.get('DB', 'db_name')
    domain = config.get('DB', 'domain')
    host = f'mongodb+srv://{user}:{password}@{domain}/{db}?retryWrites=true&w=majority'

    connect('HW9', host=host, ssl=True)

    credentials = PlainCredentials('guest', 'guest')
    connection = BlockingConnection(ConnectionParameters(host='localhost', port=5672, credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue='sms_queue', durable=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')

    def callback(ch, method, properties, body):
        message = loads(body.decode())
        print(f" [x] Received: {message}")
        send_sms(message)
        print(f" [+] Done: {method.delivery_tag}")
        ch.basic_ack(delivery_tag=method.delivery_tag)


    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='sms_queue', on_message_callback=callback)
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e)
