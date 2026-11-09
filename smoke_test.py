"""
smoke_test.py
=============
Verifikasi bahwa seluruh modul dapat di-load dan task sample (chain)
dieksekusi end-to-end TANPA perlu RabbitMQ.

Dijalankan dalam mode eager -> Celery menjalankan task secara sinkron
di dalam proses ini (tidak kirim ke broker). Berguna untuk unit-test
dan CI.
"""
from celery_config import app

# Paksa eager mode SEBELUM import task-task
app.conf.update(
    task_always_eager=True,
    task_eager_propagates=True,
    broker_url='memory://',
    result_backend='cache+memory://',
)

import json
from tasks import process_data, generate_report, process_and_report
from run_chain import create_task_chain
from messaging_tasks import notify_broadcast, log_event, handle_event


def section(title):
    print(f"\n{'='*60}\n {title}\n{'='*60}")


# 1. Sample awal — orchestrator
section("1. Orchestrator: process_and_report")
raw = "Data uji smoke-test untuk sistem job queue Celery."
out = process_and_report.delay(raw).get()
print(json.dumps(out, indent=2, default=str))

# 2. Sample awal — chain primitive
section("2. Chain primitive: process_data -> generate_report")
out = create_task_chain(raw).get()
print(json.dumps(out, indent=2, default=str))

# 3. Fan-Out (eager: hanya mengeksekusi sekali karena tanpa broker
#    tidak ada fan-out beneran, tapi task dapat dipanggil normal)
section("3. Fan-Out task invocation (notify_broadcast)")
out = notify_broadcast.delay({'judul': 'Maintenance',
                              'isi': 'Server down 23:00 WIB'}).get()
print(json.dumps(out, indent=2, default=str))

# 4. Topic (demonstrasi pemanggilan — routing tidak diuji di eager mode)
section("4. Topic task invocation (log_event)")
for rk in ['info.auth', 'error.payment']:
    out = log_event.apply_async(args=[{'msg': f'Contoh event dgn rk={rk}'}],
                                routing_key=rk).get()
    print(f"[rk={rk}]  -> {json.dumps(out, default=str)}")

# 5. Wildcard (sda — hanya menguji pemanggilan task)
section("5. Wildcard task invocation (handle_event)")
for rk in ['asia.prod.api', 'eu.prod.database']:
    out = handle_event.apply_async(args=[{'desc': f'Event rk={rk}'}],
                                   routing_key=rk).get()
    print(f"[rk={rk}]  -> {json.dumps(out, default=str)}")

# 6. Verifikasi definisi exchange & queue di konfigurasi
section("6. Definisi exchange & queue di celery_config")
for q in app.conf.task_queues:
    print(f"  queue={q.name:<22} exchange={q.exchange.name:<22} "
          f"type={q.exchange.type:<7} routing_key={q.routing_key!r}")

print("\nSmoke test selesai: semua task dapat di-load dan dipanggil.")
