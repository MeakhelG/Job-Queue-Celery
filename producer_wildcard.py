"""
producer_wildcard.py
====================
Demonstrasi pattern wildcard '*' dan '#' pada topic exchange.

Binding queue (lihat celery_config.py):
    wc_asia_all          -> 'asia.#'        (apapun yang diawali 'asia.')
    wc_prod_any_region   -> '*.prod.*'      (satu kata . 'prod' . satu kata)
    wc_all_db            -> '#.database'    (apa pun yang diakhiri '.database')

Jalankan worker pada 3 terminal terpisah:
    celery -A celery_config worker -Q wc_asia_all        -n asia@%h -l info
    celery -A celery_config worker -Q wc_prod_any_region -n prod@%h -l info
    celery -A celery_config worker -Q wc_all_db          -n db@%h   -l info

Lalu jalankan skrip ini:
    python producer_wildcard.py
"""
from messaging_tasks import handle_event


EVENTS = [
    # rk -> queue tujuan yang di-match
    ('asia.prod.api',       {'desc': 'Request API dari region asia (prod)'}),
    # match: wc_asia_all (asia.#), wc_prod_any_region (*.prod.*)

    ('asia.dev.database',   {'desc': 'Query DB dari region asia (dev)'}),
    # match: wc_asia_all (asia.#), wc_all_db (#.database)

    ('eu.prod.database',    {'desc': 'Query DB dari region eu (prod)'}),
    # match: wc_prod_any_region (*.prod.*), wc_all_db (#.database)

    ('us.prod.payment.api', {'desc': 'Payment API us (prod) - segmen >3'}),
    # match: wc_prod_any_region? -> TIDAK, karena '*.prod.*' = tepat 3 segmen
    # TIDAK ada binding yang match -> pesan di-drop

    ('asia.staging.worker.database',
        {'desc': 'Worker DB staging asia - segmen 4'}),
    # match: wc_asia_all (asia.# -> # cocok 1+ kata),
    #        wc_all_db   (#.database -> # cocok 2+ kata)
]


def main():
    for routing_key, payload in EVENTS:
        result = handle_event.apply_async(args=[payload], routing_key=routing_key)
        print(f"Wildcard dispatched. rk={routing_key:<32} task_id={result.id}")

    print("\nCatatan penting:")
    print("- '*'  = TEPAT SATU kata (satu segmen)")
    print("- '#'  = NOL atau lebih kata (fleksibel jumlah segmen)")
    print("- 'us.prod.payment.api' -> TIDAK match '*.prod.*' karena ada 4 segmen,")
    print("  sedangkan '*.prod.*' hanya menerima tepat 3 segmen.")


if __name__ == '__main__':
    main()
