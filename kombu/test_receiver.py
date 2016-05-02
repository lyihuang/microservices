from kombu.mixins import ConsumerMixin
import kombu
import logging
logger = logging.getLogger(__name__)

hapi_exchange = kombu.Exchange('data-view.dev.notification', 'direct')
routing_key = "r_tenant"
class DbEventNotificationConsumer(ConsumerMixin):
    def __init__(self, connection, job_upd_exchange, call_back=None, q_name='q_tenant'):
        self.connection = connection
        self.qname = q_name
        self._queue = kombu.Queue(self.qname, job_upd_exchange, routing_key=routing_key,
                                  queue_arguments=None)
        self.on_notification = call_back if call_back else self.notify

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self._queue],
                         callbacks=[self.on_notification])]

    def notify(self, body, message): # pragma: no cover
        try:
            print 'receive event %s', str(body)
            logger.debug('receive event %s', str(body))

        finally:
            message.ack()



def main(urls, q_callback=None, exchange=None, q_name=None):
    #import eventlet
    #from eventlet.event import Event
    if not exchange:
        exchange = hapi_exchange

    #_latch = Event()
    if urls and isinstance(urls, str):
        urls = [urls]
    logger.debug('rabbitmq urls %s', str(urls))
    with kombu.Connection(urls) as conn:
        summary_con = DbEventNotificationConsumer(conn, exchange, q_callback, q_name)
        import threading
        d = threading.Thread(name='daemon', target=summary_con.run)
        #d.setDaemon(True)
        d.start()
        #eventlet.spawn(summary_con.run) # pragma: no cover

        #_latch.wait() # pragma: no cover
        return summary_con

if __name__ == '__main__': # pragma: no cover
    url = 'pyamqp://guest:guest@localhost:5672//'
    job_upd_exchange = kombu.Exchange('data-view.dev.notification', 'direct')
    main(url, exchange=job_upd_exchange)
