url = 'amqp://guest:guest@localhost:5672//'

from kombu.pools import producers
from kombu.connection import Connection
from kombu import Exchange, Queue

routing_key = "r_tenant"
exchange = Exchange('data-view.dev.notification', type='direct')
tssm_queues = [Queue('tenant', exchange, durable=False, routing_key=routing_key, queue_arguments=None)]

def send_config_state_change_notification(name, value):
    rabbitmq_url = url
    connection = Connection(rabbitmq_url)
    kwargs = {'name':name, 'value':value}
    payload = {'kwargs':kwargs}
    routingkey = routing_key

    with producers[connection].acquire(block=True) as producer:
        producer.publish(payload, exchange=exchange, declare=[exchange], routing_key=routingkey)
    return

if __name__ == '__main__':
     print "send notification"
     send_config_state_change_notification('HAPPY', 'PET')