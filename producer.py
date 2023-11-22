from configparser import ConfigParser
from faker import Faker
from mongoengine import connect, Document, StringField, EmailField, BooleanField
from pathlib import Path
from pika import PlainCredentials, BlockingConnection, ConnectionParameters, BasicProperties, spec


fake = Faker('uk_UA')


class Contact(Document):
    fullname = StringField(max_length=120)
    email = EmailField()
    ack = BooleanField(default=False)
    meta = {'collection': 'contacts'}


def create_contact(count: int = 1) -> None:
    for _ in range(count):
        Contact(fullname=fake.name(), email=fake.ascii_free_email()).save()


def send_message(count: int = 1) -> None:
    credentials = PlainCredentials('guest', 'guest')
    connection = BlockingConnection(ConnectionParameters(host='localhost', port=5672, credentials=credentials))
    channel = connection.channel()

    channel.exchange_declare(exchange='task_mock', exchange_type='direct')
    channel.queue_declare(queue='task_queue', durable=True)
    channel.queue_bind(exchange='task_mock', queue='task_queue')

    contacts = Contact.objects.all()
    for contact in contacts:
        message = f'ID: {contact.id}, name: {contact.fullname}'

        channel.basic_publish(
            exchange='task_mock',
            routing_key='task_queue',
            body=message.encode(),
            properties=BasicProperties(
                delivery_mode=spec.PERSISTENT_DELIVERY_MODE
            ))

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

    connect('HW9', host=f'mongodb+srv://{user}:{password}@{domain}/{db}?retryWrites=true&w=majority', ssl=True)

    Contact.drop_collection()
    number_contacts = 3
    create_contact(number_contacts)
    send_message(number_contacts)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e)
