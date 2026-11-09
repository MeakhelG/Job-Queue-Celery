"""
producer_topic.py
=================
Men-dispatch sejumlah event ke exchange tipe 'topic' dengan beberapa
routing_key berbeda, agar bisa mengamati bagaimana pesan tersortir
ke queue yang sesuai.

Binding queue (lihat celery_config.py):
    topic_info     -> 'info.*'
    topic_error    -> 'error.*'
    topic_payment  -> '*.payment'

Jalankan worker pada 3 terminal terpisah:
    celery -A celery_config worker -Q topic_info    -n info@%h    -l info
    celery -A celery_config worker -Q topic_error   -n error@%h   -l info
    celery -A celery_config worker -Q topic_payment -n payment@%h -l info

Lalu jalankan skrip ini:
    python producer_topic.py
"""
from messaging_tasks import log_event


EVENTS = [
    # (routing_key, payload) -> queue tujuan yang di-match:
    ('info.auth',      {'msg': 'User X berhasil login'}),            # topic_info
    ('info.payment',   {'msg': 'Checkout dimulai oleh user X'}),     # topic_info + topic_payment
    ('error.auth',     {'msg': 'Gagal login: password salah'}),      # topic_error
    ('error.payment',  {'msg': 'Pembayaran ditolak: saldo kurang'}), # topic_error + topic_payment
    ('debug.auth',     {'msg': 'Debug trace auth'}),                 # (tidak match binding -> tidak diterima)
]


def main():
    for routing_key, payload in EVENTS:
        result = log_event.apply_async(args=[payload], routing_key=routing_key)
        print(f"Topic dispatched. rk={routing_key:<16} task_id={result.id}")

    print("\nCek log worker 'info', 'error', dan 'payment':")
    print("- 'info.payment'  -> diterima worker info DAN payment (double consume).")
    print("- 'error.payment' -> diterima worker error DAN payment.")
    print("- 'debug.auth'    -> tidak ada binding yg match, pesan di-drop.")


if __name__ == '__main__':
    main()
