"""
demo_routing.py
===============
Demonstrasi SEMANTIK routing Fan-Out, Topic, dan Wildcard secara
in-process menggunakan kombu memory transport (tanpa RabbitMQ).

Sama persis definisi exchange & queue yg dipakai Celery di
celery_config.py, tapi di sini kita publish langsung lewat kombu
lalu drain pesan dari tiap queue untuk melihat BERAPA & PESAN APA
yang masuk ke masing-masing queue.
"""
from kombu import Connection, Exchange, Queue, Producer


def drain(conn, queues, timeout=0.3):
    """Baca semua pesan dari list of Queue, kembalikan dict {qname: [msgs]}."""
    result = {q.name: [] for q in queues}
    with conn.SimpleQueue(queues[0].name) as _:
        pass
    for q in queues:
        with conn.channel() as ch:
            q_bound = q.bind(ch)
            while True:
                msg = q_bound.get(no_ack=True)
                if msg is None:
                    break
                result[q.name].append(msg.payload)
    return result


def demo_fanout():
    print("\n" + "="*60)
    print(" DEMO 1: FAN-OUT")
    print("="*60)
    with Connection('memory://') as conn:
        ex = Exchange('notifications_fanout', type='fanout')
        qs = [
            Queue('fanout_email', ex),
            Queue('fanout_sms',   ex),
            Queue('fanout_push',  ex),
        ]
        # declare dulu agar broker tahu binding sebelum publish
        with conn.channel() as ch:
            for q in qs:
                q(ch).declare()

            producer = Producer(ch, exchange=ex)
            producer.publish({'msg': 'Server maintenance 23:00'},
                             routing_key='ignored_in_fanout')

        received = drain(conn, qs)
    for name, msgs in received.items():
        print(f"  {name:<18} menerima {len(msgs)} pesan: {msgs}")
    print("Kesimpulan: 1 publish -> 3 queue menerima salinan yg SAMA "
          "(routing_key diabaikan).")


def demo_topic():
    print("\n" + "="*60)
    print(" DEMO 2: TOPIC")
    print("="*60)
    with Connection('memory://') as conn:
        ex = Exchange('logs_topic', type='topic')
        qs = [
            Queue('topic_info',    ex, routing_key='info.*'),
            Queue('topic_error',   ex, routing_key='error.*'),
            Queue('topic_payment', ex, routing_key='*.payment'),
        ]
        with conn.channel() as ch:
            for q in qs:
                q(ch).declare()
            producer = Producer(ch, exchange=ex)
            for rk, payload in [
                ('info.auth',     {'msg': 'login ok'}),
                ('info.payment',  {'msg': 'checkout started'}),
                ('error.auth',    {'msg': 'pw salah'}),
                ('error.payment', {'msg': 'kartu ditolak'}),
                ('debug.auth',    {'msg': 'trace'}),
            ]:
                producer.publish(payload, routing_key=rk)

        received = drain(conn, qs)
    for name, msgs in received.items():
        print(f"  {name:<18} menerima {len(msgs)} pesan:")
        for m in msgs:
            print(f"      - {m}")
    print("Kesimpulan: 'info.payment' & 'error.payment' masuk ke 2 queue,")
    print("            'debug.auth' tidak match binding -> DI-DROP.")


def demo_wildcard():
    print("\n" + "="*60)
    print(" DEMO 3: WILDCARD ('*' & '#')")
    print("="*60)
    with Connection('memory://') as conn:
        ex = Exchange('events_wildcard', type='topic')
        qs = [
            Queue('wc_asia_all',        ex, routing_key='asia.#'),
            Queue('wc_prod_any_region', ex, routing_key='*.prod.*'),
            Queue('wc_all_db',          ex, routing_key='#.database'),
        ]
        with conn.channel() as ch:
            for q in qs:
                q(ch).declare()
            producer = Producer(ch, exchange=ex)
            for rk, payload in [
                ('asia.prod.api',              {'d': 'req api asia prod'}),
                ('asia.dev.database',          {'d': 'db asia dev'}),
                ('eu.prod.database',           {'d': 'db eu prod'}),
                ('us.prod.payment.api',        {'d': '4 segmen'}),
                ('asia.staging.worker.database', {'d': '4 segmen asia'}),
            ]:
                producer.publish(payload, routing_key=rk)

        received = drain(conn, qs)
    for name, msgs in received.items():
        print(f"  {name:<22} menerima {len(msgs)} pesan:")
        for m in msgs:
            print(f"      - {m}")
    print("Kesimpulan:")
    print("  - '*' = tepat 1 kata  => 'us.prod.payment.api' TIDAK match '*.prod.*'")
    print("  - '#' = 0+ kata       => 'asia.#' & '#.database' menerima input 4 segmen.")


if __name__ == '__main__':
    demo_fanout()
    demo_topic()
    demo_wildcard()
