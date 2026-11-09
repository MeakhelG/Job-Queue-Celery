"""
producer.py
===========
Producer utama / input-generator untuk sistem job queue Celery.

Peran:
    1. Men-dispatch pekerjaan sample (process_data + generate_report)
       baik via orchestrator maupun chain primitive.
    2. Berperan sebagai GENERATOR INPUT berkala (lihat --stream).
       Setiap input baru dibangkitkan lalu dikirim ke broker,
       untuk kemudian dikerjakan oleh worker Celery.

Pemakaian:
    # mode single-shot (default): kirim 1x chain + 1x orchestrator
    python producer.py

    # mode input-generator: bangkitkan N input dan kirim dgn delay
    python producer.py --stream --count 10 --interval 1.5

    # kirim juga demo message handling (fanout/topic/wildcard)
    python producer.py --demo-messaging

Pastikan RabbitMQ berjalan dan worker telah dinyalakan (lihat README).
"""
import argparse
import json
import random
import time
from datetime import datetime

from tasks import process_and_report
from run_chain import create_task_chain


# -------- Dataset dummy utk input-generator --------
TOPIK = [
    'Analisis sentimen tweet pemilu',
    'Ringkasan artikel berita teknologi',
    'Ekstraksi entitas dari dokumen hukum',
    'Klasifikasi email spam vs ham',
    'Transkripsi rekaman wawancara',
    'Deteksi anomali trafik jaringan',
    'Rekomendasi produk e-commerce',
    'Prediksi harga saham sektor energi',
]


def generate_input(i: int) -> str:
    """Membangkitkan 1 kalimat sebagai input task."""
    topik = random.choice(TOPIK)
    stamp = datetime.now().strftime('%H:%M:%S')
    return f"[{stamp}] Request #{i+1}: {topik} (prioritas={random.randint(1,5)})"


# -------- Mode 1: eksekusi sample (orchestrator + chain) --------
def run_sample_once():
    raw_input_text = "Ini adalah data contoh yang harus diproses dan dilaporkan."

    print("=== Cara 1: Orchestrator task ===")
    result = process_and_report.delay(raw_input_text)
    final_output = result.get(timeout=30)
    print(f"Final result: {json.dumps(final_output, indent=2, default=str)}")

    print("\n=== Cara 2: Chain primitive milik Celery ===")
    chain_result = create_task_chain(raw_input_text)
    chain_output = chain_result.get(timeout=30)
    print(f"Chain result: {json.dumps(chain_output, indent=2, default=str)}")


# -------- Mode 2: input-generator stream --------
def run_stream(count: int, interval: float):
    """
    Bangkitkan `count` input, kirim ke broker via chain, jangan tunggu
    hasilnya satu per satu — biarkan worker yang bekerja asinkron.
    Kita hanya menampilkan task_id untuk tiap request.
    """
    print(f"Streaming {count} input ke broker (interval={interval}s)...")
    pending = []
    for i in range(count):
        payload = generate_input(i)
        async_result = create_task_chain(payload)
        print(f"  dispatched #{i+1:>3}  task_id={async_result.id}  payload={payload}")
        pending.append(async_result)
        time.sleep(interval)

    print("\nMenunggu semua task selesai...")
    for i, ar in enumerate(pending):
        try:
            out = ar.get(timeout=60)
            print(f"  done #{i+1:>3}  report_id={out.get('report_id')}")
        except Exception as exc:
            print(f"  FAIL #{i+1:>3}  err={exc}")


# -------- Mode 3: demo messaging (fanout/topic/wildcard) --------
def run_messaging_demo():
    """Import lokal agar tidak wajib jika user cuma mau mode utama."""
    import producer_fanout
    import producer_topic
    import producer_wildcard

    print("\n### FAN-OUT ###")
    producer_fanout.main()

    print("\n### TOPIC ###")
    producer_topic.main()

    print("\n### WILDCARD ###")
    producer_wildcard.main()


def parse_args():
    p = argparse.ArgumentParser(description="Producer / input-generator Celery")
    p.add_argument('--stream', action='store_true',
                   help='Bangkitkan N input secara berkala (input-generator mode).')
    p.add_argument('--count', type=int, default=5,
                   help='Jumlah input saat --stream (default: 5).')
    p.add_argument('--interval', type=float, default=1.0,
                   help='Jeda antar input dalam detik (default: 1.0).')
    p.add_argument('--demo-messaging', action='store_true',
                   help='Jalankan demo fanout/topic/wildcard.')
    return p.parse_args()


if __name__ == '__main__':
    args = parse_args()

    if args.stream:
        run_stream(args.count, args.interval)
    elif args.demo_messaging:
        run_messaging_demo()
    else:
        run_sample_once()


# =============================================================
# Cara menjalankan worker (jalankan di terminal terpisah):
#
#   # Worker untuk sample awal
#   celery -A celery_config worker -Q data_processing   -n dp@%h  -l info
#   celery -A celery_config worker -Q report_generation -n rg@%h  -l info
#   celery -A celery_config worker                      -n gen@%h -l info
#
#   # Worker untuk FAN-OUT
#   celery -A celery_config worker -Q fanout_email -n email@%h -l info
#   celery -A celery_config worker -Q fanout_sms   -n sms@%h   -l info
#   celery -A celery_config worker -Q fanout_push  -n push@%h  -l info
#
#   # Worker untuk TOPIC
#   celery -A celery_config worker -Q topic_info    -n info@%h    -l info
#   celery -A celery_config worker -Q topic_error   -n error@%h   -l info
#   celery -A celery_config worker -Q topic_payment -n payment@%h -l info
#
#   # Worker untuk WILDCARD
#   celery -A celery_config worker -Q wc_asia_all        -n asia@%h -l info
#   celery -A celery_config worker -Q wc_prod_any_region -n prod@%h -l info
#   celery -A celery_config worker -Q wc_all_db          -n db@%h   -l info
#
#   # Monitoring (opsional)
#   celery -A celery_config flower
# =============================================================
