from configparser import ConfigParser
from faker import Faker
from json import dumps
from mongoengine import connect, Document, StringField, EmailField, BooleanField, IntField
from pathlib import Path
from pika import PlainCredentials, BlockingConnection, ConnectionParameters, BasicProperties, spec
from random import choice


fake = Faker('uk_UA')


class Contact(Document):
    fullname = StringField(max_length=120)
    phone = IntField()
    email = EmailField(max_length=120)
    priority = StringField(max_length=5, default='email')
    ack = BooleanField(default=False)
    meta = {'collection': 'contacts'}


def create_contact(count: int = 1) -> None:
    for _ in range(count):
        Contact(fullname=fake.name(), phone=fake.msisdn(), email=fake.ascii_free_email(),
                priority=choice(('phone', 'email'))).save()


def send_message(count: int = 1) -> None:
    credentials = PlainCredentials('guest', 'guest')
    connection = BlockingConnection(ConnectionParameters(host='localhost', port=5672, credentials=credentials))
    channel = connection.channel()

    channel.exchange_declare(exchange='task_mock', exchange_type='direct')
    channel.queue_declare(queue='sms_queue', durable=True)
    channel.queue_bind(exchange='task_mock', queue='sms_queue')
    channel.queue_declare(queue='email_queue', durable=True)
    channel.queue_bind(exchange='task_mock', queue='email_queue')

    contacts = Contact.objects.all()
    for contact in contacts:
        message = {'ID': str(contact.id), 'name': contact.fullname}

        match contact.priority:
            case 'email':
                channel.basic_publish(
                    exchange='task_mock',
                    routing_key='email_queue',
                    body=dumps(message).encode(),
                    properties=BasicProperties(delivery_mode=spec.PERSISTENT_DELIVERY_MODE)
                )
            case 'phone':
                channel.basic_publish(
                    exchange='task_mock',
                    routing_key='sms_queue',
                    body=dumps(message).encode(),
                    properties=BasicProperties(delivery_mode=spec.PERSISTENT_DELIVERY_MODE)
                )

        print(f' [x] Sent {message}')
    connection.close()


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

    Contact.drop_collection()
    number_contacts = 3
    create_contact(number_contacts)
    send_message(number_contacts)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e)
