"""
messaging_tasks.py
==================
Task-task konsumer (worker-side) untuk tiga skema message handling:
    - Fan-Out  : notify_broadcast
    - Topic    : log_event
    - Wildcard : handle_event  (sub-tipe dari topic exchange, bedanya pada pola bind)

Setiap task akan dieksekusi oleh worker yang mengkonsumsi queue
yang di-bind pada exchange terkait. Lihat celery_config.py untuk
definisi exchange dan queue-nya.
"""
from celery_config import app
import time


# ============================================================
# 1. FAN-OUT
# ============================================================
# Exchange tipe 'fanout' akan mem-broadcast setiap pesan ke SEMUA
# queue yang ter-bind pada exchange tersebut. Routing key diabaikan.
#
# Pada celery_config.py kita bind 3 queue:
#   - fanout_email, fanout_sms, fanout_push
# Sehingga 1 panggilan notify_broadcast akan memicu 3 kali eksekusi
# (satu per queue) jika worker untuk ketiga queue tersebut aktif.
# ============================================================
@app.task(bind=True, name='messaging_tasks.notify_broadcast')
def notify_broadcast(self, message):
    """
    Dipanggil oleh seluruh queue yang di-bind pada fanout exchange.
    Task yang sama akan dieksekusi berkali-kali, sekali untuk
    tiap queue.
    """
    queue = getattr(self.request, 'delivery_info', {}).get('routing_key', '-')
    consumer_queue = getattr(self.request, 'delivery_info', {}).get('exchange', '-')
    print(f"[FANOUT|{consumer_queue}|{queue}] Broadcast diterima: {message}")
    time.sleep(0.5)
    return {
        'pattern': 'fanout',
        'exchange': consumer_queue,
        'routing_key': queue,
        'message': message,
    }


# ============================================================
# 2. TOPIC
# ============================================================
# Exchange tipe 'topic' me-route pesan berdasarkan POLA routing_key
# yang mengandung segmen ber-titik. Tiap queue di-bind dengan pola
# berbeda, mis.:
#   - topic_info    -> 'info.*'
#   - topic_error   -> 'error.*'
#   - topic_payment -> '*.payment'
#
# Jadi pesan dgn routing_key 'error.payment' akan masuk BOTH:
# topic_error (match 'error.*') DAN topic_payment (match '*.payment').
# ============================================================
@app.task(bind=True, name='messaging_tasks.log_event')
def log_event(self, event):
    """
    Konsumen untuk logs_topic exchange. Severity dan service dapat
    dibaca dari routing_key yang dikirim producer.
    """
    rk = getattr(self.request, 'delivery_info', {}).get('routing_key', '-')
    print(f"[TOPIC|rk={rk}] Event diterima: {event}")
    time.sleep(0.3)
    return {
        'pattern': 'topic',
        'routing_key': rk,
        'event': event,
    }


# ============================================================
# 3. WILDCARD
# ============================================================
# Pattern yang didukung topic exchange:
#   '*'  = TEPAT SATU kata (satu segmen antar titik)
#   '#'  = NOL atau lebih kata (segmen apa pun, bisa banyak)
#
# Contoh binding (lihat celery_config.py):
#   - wc_asia_all        -> 'asia.#'           (apa pun yang diawali 'asia.')
#   - wc_prod_any_region -> '*.prod.*'         (satu kata, 'prod', satu kata)
#   - wc_all_db          -> '#.database'       (apa pun yg diakhiri '.database')
#
# Contoh routing_key producer:
#   'asia.prod.api'      -> cocok wc_asia_all & wc_prod_any_region
#   'asia.dev.database'  -> cocok wc_asia_all & wc_all_db
#   'eu.prod.database'   -> cocok wc_prod_any_region & wc_all_db
# ============================================================
@app.task(bind=True, name='messaging_tasks.handle_event')
def handle_event(self, payload):
    """
    Konsumen untuk events_wildcard exchange. Satu pesan dapat
    masuk ke banyak queue sekaligus karena pola binding wildcard
    yang beririsan.
    """
    rk = getattr(self.request, 'delivery_info', {}).get('routing_key', '-')
    print(f"[WILDCARD|rk={rk}] Event diterima: {payload}")
    time.sleep(0.3)
    return {
        'pattern': 'wildcard',
        'routing_key': rk,
        'payload': payload,
    }
