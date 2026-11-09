from celery import Celery
from kombu import Exchange, Queue

app = Celery('ifk_celery')

# ----- Exchanges -----
# Direct exchange (default routing) untuk sample awal (process_data, generate_report)
direct_exchange = Exchange('direct_exchange', type='direct')

# Fan-Out exchange: broadcast ke semua queue yang ter-bind, routing_key diabaikan
fanout_exchange = Exchange('notifications_fanout', type='fanout')

# Topic exchange: routing berdasarkan pola key ber-titik (foo.bar.baz)
topic_exchange = Exchange('logs_topic', type='topic')

# Wildcard menggunakan exchange tipe topic, namun pola binding-nya
# menggunakan '*' (tepat satu kata) dan '#' (nol atau lebih kata).
wildcard_exchange = Exchange('events_wildcard', type='topic')


# ----- Queue definitions -----
app.conf.task_queues = (
    # Queue untuk sample awal (direct routing by queue name)
    Queue('data_processing', direct_exchange, routing_key='data_processing'),
    Queue('report_generation', direct_exchange, routing_key='report_generation'),

    # ---------- Fan-Out ----------
    # Semua consumer yang bind ke exchange ini akan menerima pesan yang sama.
    Queue('fanout_email', fanout_exchange),
    Queue('fanout_sms', fanout_exchange),
    Queue('fanout_push', fanout_exchange),

    # ---------- Topic ----------
    # Routing key berpola: <severity>.<service>  (mis. "info.auth", "error.payment")
    Queue('topic_info', topic_exchange, routing_key='info.*'),
    Queue('topic_error', topic_exchange, routing_key='error.*'),
    Queue('topic_payment', topic_exchange, routing_key='*.payment'),

    # ---------- Wildcard ----------
    # Routing key berpola: <region>.<env>.<service>
    # '*'  = tepat satu kata
    # '#'  = nol atau lebih kata
    Queue('wc_asia_all', wildcard_exchange, routing_key='asia.#'),
    Queue('wc_prod_any_region', wildcard_exchange, routing_key='*.prod.*'),
    Queue('wc_all_db', wildcard_exchange, routing_key='#.database'),
)

app.conf.update(
    broker_url='amqp://guest@localhost//',
    result_backend='rpc://',
    task_default_exchange='direct_exchange',
    task_default_exchange_type='direct',
    task_default_routing_key='data_processing',

    # Routing utk tasks di tasks.py (sample awal)
    task_routes={
        'tasks.process_data':
            {'queue': 'data_processing', 'routing_key': 'data_processing'},
        'tasks.generate_report':
            {'queue': 'report_generation', 'routing_key': 'report_generation'},

        # Fan-Out: routing_key diabaikan oleh fanout exchange, tapi tetap diisi.
        'messaging_tasks.notify_broadcast':
            {'exchange': 'notifications_fanout', 'exchange_type': 'fanout',
             'routing_key': 'broadcast'},

        # Topic: routing_key di-set oleh producer (lihat producer_topic.py).
        'messaging_tasks.log_event':
            {'exchange': 'logs_topic', 'exchange_type': 'topic',
             'routing_key': 'info.generic'},

        # Wildcard: routing_key di-set oleh producer (lihat producer_wildcard.py).
        'messaging_tasks.handle_event':
            {'exchange': 'events_wildcard', 'exchange_type': 'topic',
             'routing_key': 'asia.prod.api'},
    },

    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Jakarta',
    enable_utc=True,
)
