from pika import PlainCredentials, BlockingConnection, ConnectionParameters
from time import sleep


def send_email():
    sleep(1)
    return ' [x] Sent email'



def main():
    credentials = PlainCredentials('guest', 'guest')
    connection = BlockingConnection(ConnectionParameters(host='localhost', port=5672, credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue='task_queue', durable=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')

    def callback(ch, method, properties, body):
        message = body.decode()
        print(f" [x] Received: {message}")
        print(send_email())
        print(f" [+] Done: {method.delivery_tag}")
        ch.basic_ack(delivery_tag=method.delivery_tag)


    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='task_queue', on_message_callback=callback)
    channel.start_consuming()



if __name__ == '__main__':
    main()
