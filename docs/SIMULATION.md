# Gambaran Simulasi & Peta Kode

Dokumen ini menggambarkan **alur simulasi**, **arsitektur** sistem job queue
Celery yang dibangun, dan **di file mana** setiap bagian kode berada
beserta **contoh hasil** eksekusinya.

---

## 1. Arsitektur Umum

```
                              ┌──────────────────────────┐
                              │        PRODUCER          │
                              │  producer.py (main)      │
                              │  producer_fanout.py      │
                              │  producer_topic.py       │
                              │  producer_wildcard.py    │
                              └───────────┬──────────────┘
                                          │  .apply_async(args, routing_key=…)
                                          ▼
                              ┌──────────────────────────┐
                              │          BROKER          │
                              │    RabbitMQ (AMQP)       │
                              │                          │
                              │  ┌────────┐  ┌────────┐  │
                              │  │Exchange│→ │ Queue  │  │
                              │  └────────┘  └────────┘  │
                              └───────────┬──────────────┘
                                          │  worker consume dari queue
                                          ▼
                              ┌──────────────────────────┐
                              │         WORKER           │
                              │  celery -A celery_config │
                              │  worker -Q <queue>       │
                              │                          │
                              │  Tasks di:               │
                              │  - tasks.py              │
                              │  - messaging_tasks.py    │
                              └──────────────────────────┘
```

Ada **4 jenis exchange** yang dipakai (lihat `celery_config.py`):

| Exchange              | Tipe     | Dipakai oleh                            |
|-----------------------|----------|------------------------------------------|
| `direct_exchange`     | direct   | Sample awal (process_data, report)       |
| `notifications_fanout`| fanout   | Fan-Out demo (notify_broadcast)          |
| `logs_topic`          | topic    | Topic demo (log_event)                   |
| `events_wildcard`     | topic    | Wildcard demo (handle_event)             |

---

## 2. Alur Simulasi — Sample Awal (Chain)

File: `tasks.py` + `run_chain.py` + `producer.py`

```
   producer.py
   ───────────
       │
       │  create_task_chain("Ini adalah data contoh ...")
       ▼
   run_chain.py
   ────────────
       │  chain(process_data.s(data), generate_report.s())
       │  .apply_async()
       ▼
   ╔═══════════════════════════════════════════════════════════╗
   ║                      BROKER                               ║
   ║   direct_exchange ─► data_processing queue ─► Worker A    ║
   ║                            │                              ║
   ║                            ▼ (hasil diteruskan otomatis)  ║
   ║   direct_exchange ─► report_generation queue ─► Worker B  ║
   ╚═══════════════════════════════════════════════════════════╝
       │                                             │
       ▼                                             ▼
   process_data()                              generate_report()
   ──────────────                              ──────────────────
   input:  "Ini adalah ..."                    input:  processed_data (dict)
   output: {                                   output: {
     original: "...",                            report_id: "RPT_1794204516",
     word_count: 8,                              data_summary: {...},
     status: "processed"                         generated_at: ...,
   }                                             status: "completed"
                                               }
```

**Contoh hasil eksekusi** (dari `python smoke_test.py`):
```json
{
  "report_id": "RPT_1794204516",
  "data_summary": {
    "original_data": "Data uji smoke-test untuk sistem job queue Celery.",
    "word_count": 8,
    "processed_at": 1794204513.215845
  },
  "generated_at": 1794204516.220484,
  "status": "completed"
}
```

---

## 3. Alur Simulasi — FAN-OUT

File: `producer_fanout.py` + `messaging_tasks.py` (task `notify_broadcast`)

```
                          producer_fanout.py
                          ──────────────────
                                   │
                                   │  notify_broadcast.apply_async(
                                   │      args=[{judul: "...", isi: "..."}])
                                   ▼
                     ╔═══════════════════════════════╗
                     ║  notifications_fanout         ║
                     ║  (type = FANOUT)              ║
                     ║  routing_key = IGNORED        ║
                     ╚═════════╦═════════╦═══════════╝
                               │         │         │
                 ┌─────────────┘         │         └─────────────┐
                 ▼                       ▼                       ▼
         ┌───────────────┐       ┌───────────────┐       ┌───────────────┐
         │ fanout_email  │       │ fanout_sms    │       │ fanout_push   │
         │ queue         │       │ queue         │       │ queue         │
         └───────┬───────┘       └───────┬───────┘       └───────┬───────┘
                 ▼                       ▼                       ▼
            Worker email            Worker sms              Worker push
            (eksekusi               (eksekusi               (eksekusi
             notify_broadcast)       notify_broadcast)       notify_broadcast)
```

**Contoh hasil** (dari `python demo_routing.py`):
```
fanout_email   menerima 1 pesan: [{'msg': 'Server maintenance 23:00'}]
fanout_sms     menerima 1 pesan: [{'msg': 'Server maintenance 23:00'}]
fanout_push    menerima 1 pesan: [{'msg': 'Server maintenance 23:00'}]
```

**Interpretasi:** 1 publish → 3 queue menerima salinan **yang persis sama**.
Ini pola **broadcast** klasik (satu pengirim, banyak penerima).

---

## 4. Alur Simulasi — TOPIC

File: `producer_topic.py` + `messaging_tasks.py` (task `log_event`)

```
                          producer_topic.py
                          ─────────────────
                                   │
                                   │  log_event.apply_async(
                                   │      args=[payload],
                                   │      routing_key='info.payment')
                                   ▼
                     ╔═══════════════════════════════╗
                     ║  logs_topic                   ║
                     ║  (type = TOPIC)               ║
                     ║  match routing_key vs binding ║
                     ╚═════════╦═════════╦═══════════╝
                               │         │         │
             binding='info.*'  │         │  binding='*.payment'
                               │         │
           ┌───────────────────┘         └──────────────────┐
           │                    binding='error.*'          │
           ▼                             │                  ▼
   ┌───────────────┐           ┌─────────┴──────┐  ┌───────────────┐
   │ topic_info    │           │ topic_error    │  │ topic_payment │
   └───────┬───────┘           └────────┬───────┘  └───────┬───────┘
           ▼                            ▼                   ▼
      Worker info                 Worker error         Worker payment

   Contoh routing_key -> queue tujuan:
   ───────────────────────────────────────────────
   'info.auth'      -> topic_info
   'info.payment'   -> topic_info + topic_payment   (DOUBLE-CONSUME)
   'error.auth'     -> topic_error
   'error.payment'  -> topic_error + topic_payment  (DOUBLE-CONSUME)
   'debug.auth'     -> (tidak match) -> DI-DROP
```

**Contoh hasil** (dari `python demo_routing.py`):
```
topic_info      menerima 2 pesan:
    - {'msg': 'login ok'}          <- dari rk=info.auth
    - {'msg': 'checkout started'}  <- dari rk=info.payment
topic_error     menerima 2 pesan:
    - {'msg': 'pw salah'}          <- dari rk=error.auth
    - {'msg': 'kartu ditolak'}     <- dari rk=error.payment
topic_payment   menerima 2 pesan:
    - {'msg': 'checkout started'}  <- dari rk=info.payment  (sama dgn topic_info!)
    - {'msg': 'kartu ditolak'}     <- dari rk=error.payment (sama dgn topic_error!)
```

**Interpretasi:**
- `info.payment` & `error.payment` masing-masing diterima **2 queue** sekaligus.
- `debug.auth` tidak match binding manapun → **di-drop oleh broker**.

---

## 5. Alur Simulasi — WILDCARD (`*` dan `#`)

File: `producer_wildcard.py` + `messaging_tasks.py` (task `handle_event`)

```
                        producer_wildcard.py
                        ────────────────────
                                 │
                                 │  handle_event.apply_async(
                                 │      args=[payload],
                                 │      routing_key='asia.prod.api')
                                 ▼
                   ╔═════════════════════════════════╗
                   ║  events_wildcard                ║
                   ║  (type = TOPIC + wildcard)      ║
                   ║   *  = TEPAT 1 kata             ║
                   ║   #  = NOL atau lebih kata      ║
                   ╚═════════╦═════════╦═════════════╝
                             │         │         │
           binding='asia.#'  │         │  binding='#.database'
                             │         │
           ┌─────────────────┘         └──────────────────┐
           │                binding='*.prod.*'            │
           ▼                           │                   ▼
   ┌───────────────┐          ┌────────┴────────────┐  ┌───────────────┐
   │ wc_asia_all   │          │ wc_prod_any_region  │  │ wc_all_db     │
   └───────┬───────┘          └──────────┬──────────┘  └───────┬───────┘
           ▼                             ▼                     ▼
     Worker asia                    Worker prod              Worker db

   Contoh routing_key -> queue tujuan:
   ───────────────────────────────────────────────────────────────────
   'asia.prod.api'                -> wc_asia_all + wc_prod_any_region
   'asia.dev.database'            -> wc_asia_all + wc_all_db
   'eu.prod.database'             -> wc_prod_any_region + wc_all_db
   'us.prod.payment.api'          -> (TIDAK match) *.prod.* kunci 3 segmen
   'asia.staging.worker.database' -> wc_asia_all + wc_all_db  (# = ≥0 kata)
```

**Contoh hasil** (dari `python demo_routing.py`):
```
wc_asia_all          menerima 3 pesan:
    - {'d': 'req api asia prod'}       <- asia.prod.api
    - {'d': 'db asia dev'}             <- asia.dev.database
    - {'d': '4 segmen asia'}           <- asia.staging.worker.database
wc_prod_any_region   menerima 2 pesan:
    - {'d': 'req api asia prod'}       <- asia.prod.api
    - {'d': 'db eu prod'}              <- eu.prod.database
wc_all_db            menerima 3 pesan:
    - {'d': 'db asia dev'}             <- asia.dev.database
    - {'d': 'db eu prod'}              <- eu.prod.database
    - {'d': '4 segmen asia'}           <- asia.staging.worker.database
```

**Jebakan penting yang terbukti:**
- `us.prod.payment.api` **TIDAK diterima** queue manapun karena:
  - `asia.#` → butuh prefix `asia.`, tidak match
  - `*.prod.*` → butuh **tepat 3** segmen, ini 4 → tidak match
  - `#.database` → butuh suffix `.database`, tidak match
- Jadi `*` benar-benar mengikat **tepat 1 kata**, sedangkan `#` fleksibel.

---

## 6. Peta Kode (Di Mana Nyari Apa)

| Yang dicari                              | File                   | Baris penting                                   |
|------------------------------------------|------------------------|--------------------------------------------------|
| Definisi Celery app                      | `celery_config.py`     | `app = Celery('ifk_celery')`                     |
| Definisi **4 exchange**                  | `celery_config.py`     | Bagian `# ----- Exchanges -----`                 |
| Definisi **11 queue** + binding          | `celery_config.py`     | `app.conf.task_queues = (…)`                     |
| Routing tasks → exchange                 | `celery_config.py`     | `task_routes={…}`                                |
| **Task sample awal** (process, report)   | `tasks.py`             | `@app.task process_data`, `generate_report`      |
| **Orchestrator** (chain manual)          | `tasks.py`             | `process_and_report` (dengan `allow_join_result`)|
| **Chain primitive**                      | `run_chain.py`         | `create_task_chain`                              |
| **Task Fan-Out**                         | `messaging_tasks.py`   | `notify_broadcast`                               |
| **Task Topic**                           | `messaging_tasks.py`   | `log_event`                                      |
| **Task Wildcard**                        | `messaging_tasks.py`   | `handle_event`                                   |
| **Producer utama** (3 mode)              | `producer.py`          | `run_sample_once`, `run_stream`, `run_messaging_demo` |
| Mode **input-generator** (stream)        | `producer.py`          | `run_stream(count, interval)` + `generate_input`  |
| Producer Fan-Out                         | `producer_fanout.py`   | `main()`                                         |
| Producer Topic (5 routing_key uji)       | `producer_topic.py`    | `EVENTS` list                                    |
| Producer Wildcard (5 routing_key uji)    | `producer_wildcard.py` | `EVENTS` list                                    |
| Verifikasi tanpa broker                  | `smoke_test.py`        | eager mode (`task_always_eager=True`)            |
| Demo routing tanpa broker                | `demo_routing.py`      | kombu `Connection('memory://')`                  |
| Perbandingan teori ketiga skema          | `docs/EXPLORATION.md`  | Tabel §1 + per-skema §2-§4                       |

---

## 7. Cara Reproduksi Hasil

**Tanpa broker (cepat, cocok untuk verifikasi):**
```bash
./myenv/Scripts/python smoke_test.py       # task Celery eager mode
./myenv/Scripts/python demo_routing.py     # pola routing fanout/topic/wildcard
```

**Dengan broker RabbitMQ (demo lengkap dgn worker beneran):**
```bash
# 1. Pastikan RabbitMQ aktif di localhost:5672
# 2. Jalankan worker di beberapa terminal (lihat README §5)
# 3. Jalankan producer:
./myenv/Scripts/python producer.py --stream --count 10 --interval 1.0
./myenv/Scripts/python producer.py --demo-messaging
```

---

## 8. Diagram Ringkas Keseluruhan Sistem

```
┌────────────────────────────────────────────────────────────────────────┐
│                        IFK-CELERY JOB QUEUE                            │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  PRODUCER SIDE                 BROKER SIDE              WORKER SIDE   │
│  ─────────────                 ───────────              ────────────   │
│                                                                        │
│  producer.py (main)      ┌───────────────────┐                        │
│    --stream          ───►│  direct_exchange  │──► data_processing ───►│tasks.
│    --demo-messaging      │  (sample awal)    │──► report_generation ─►│  py
│                          └───────────────────┘                        │
│                                                                        │
│  producer_fanout.py      ┌───────────────────┐──► fanout_email   ────►│
│                      ───►│notifications_fanout│──► fanout_sms     ────►│mess-
│                          │  (broadcast)      │──► fanout_push    ────►│aging
│                          └───────────────────┘                        │_tasks
│                                                                        │  .py
│  producer_topic.py       ┌───────────────────┐──► topic_info     ────►│
│                      ───►│   logs_topic      │──► topic_error    ────►│
│                          │  (pattern match)  │──► topic_payment  ────►│
│                          └───────────────────┘                        │
│                                                                        │
│  producer_wildcard.py    ┌───────────────────┐──► wc_asia_all     ───►│
│                      ───►│ events_wildcard   │──► wc_prod_any_reg ───►│
│                          │  (* / # pattern)  │──► wc_all_db       ───►│
│                          └───────────────────┘                        │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```
