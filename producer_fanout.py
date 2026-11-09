"""
producer_fanout.py
==================
Men-dispatch 1 pesan ke exchange tipe 'fanout'. Pesan yang sama
akan diterima oleh SEMUA queue yang ter-bind (fanout_email, fanout_sms,
fanout_push) sehingga task notify_broadcast dieksekusi 3x (satu per
queue), asalkan ketiga worker aktif.

Jalankan worker pada 3 terminal terpisah:
    celery -A celery_config worker -Q fanout_email -n email@%h -l info
    celery -A celery_config worker -Q fanout_sms   -n sms@%h   -l info
    celery -A celery_config worker -Q fanout_push  -n push@%h  -l info

Lalu jalankan skrip ini:
    python producer_fanout.py
"""
from messaging_tasks import notify_broadcast


def main():
    pesan = {
        'judul': 'Pemeliharaan Server',
        'isi': 'Akan ada maintenance pada 23:00 WIB malam ini.',
    }

    # Karena exchange sudah bertipe fanout (dikonfigurasi di celery_config),
    # routing_key akan diabaikan oleh broker. Kita panggil .apply_async()
    # agar jelas eksplisit (bisa juga .delay()).
    result = notify_broadcast.apply_async(args=[pesan])

    print(f"Fan-Out dispatched. task_id={result.id}")
    print("Cek log pada ketiga worker (email, sms, push) — ketiganya "
          "akan menerima pesan yang sama.")


if __name__ == '__main__':
    main()
