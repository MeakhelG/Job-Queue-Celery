# IFK — Job Queue dengan Python Celery

Repositori ini merupakan pengerjaan tugas **T2-KBJ: Job Queue dengan Python Celery**
(mata kuliah *Komputasi Berbasis Jaringan*, S2 Informatika UNESA).

**Author:**
- MUHAMMAD AINURROFIQ ANWAR BUHANG — NIM: 26051905007
- MEAKHEL GUNAWAN — NIM: 26051905019

Isi tugas yang dikerjakan:
1. Modifikasi kode sumber contoh agar lebih bersih dan dapat dijalankan sebagai skrip
   (bukan package, perbaikan *broken relative-imports*).
2. Implementasi tiga skema **message handling** di atas RabbitMQ via Celery:
   **Fan-Out**, **Topic**, dan **Wildcard**.
3. Pembuatan **`producer.py`** sebagai *input generator* yang membangkitkan
   data masukan untuk diserahkan ke broker dan dikerjakan oleh worker.
4. Dokumentasi perbedaan ketiga skema pada
   [`docs/EXPLORATION.md`](docs/EXPLORATION.md).

---

## 1. Struktur Direktori

```
ifk-celery/
├── celery_config.py        # Konfigurasi Celery + definisi exchange & queue
├── tasks.py                # Task sample awal (process_data, generate_report, chain)
├── run_chain.py            # Helper chain primitive Celery
├── producer.py             # [UTAMA] Producer / input-generator
├── messaging_tasks.py      # Task konsumer untuk fanout/topic/wildcard
├── producer_fanout.py      # Producer untuk demo Fan-Out
├── producer_topic.py       # Producer untuk demo Topic
├── producer_wildcard.py    # Producer untuk demo Wildcard
├── smoke_test.py           # Verifikasi task eager-mode (tanpa broker)
├── demo_routing.py         # Demo routing in-process via kombu memory transport
├── requirements.txt        # Dependensi (celery, flower)
├── README.md               # Dokumen ini
└── docs/
    └── EXPLORATION.md      # Perbandingan Fan-Out vs Topic vs Wildcard
```

---

## 2. Quick Verification Tanpa Broker

Jika hanya ingin **mem-verifikasi** kode & semantik routing tanpa menyalakan
RabbitMQ, dua skrip berikut cukup:

```bash
# 1. Verifikasi task dieksekusi end-to-end (eager mode Celery)
python smoke_test.py

# 2. Verifikasi pola routing Fan-Out / Topic / Wildcard
#    (kombu memory transport, in-process)
python demo_routing.py
```

Hasil `demo_routing.py` membuktikan:
- **Fan-Out**: 1 publish → 3 queue menerima salinan yang sama.
- **Topic**: `info.payment` masuk ke `topic_info` **dan** `topic_payment`
  (match ganda); `debug.auth` di-drop (tidak match binding).
- **Wildcard**: `us.prod.payment.api` TIDAK match `*.prod.*` karena `*`
  mengikat tepat 1 kata, sedangkan routing key-nya punya 4 segmen.

---

## 3. Persiapan Lingkungan

### 3.1 Buat virtual environment

Opsi menggunakan **venv**:

```bash
python -m venv myenv
# Linux / macOS
source myenv/bin/activate
# Windows (Git Bash)
source myenv/Scripts/activate
```

Opsi menggunakan **uv** (lebih cepat):

```bash
uv venv myenv --python 3.10
source myenv/bin/activate  # atau myenv/Scripts/activate di Windows
```

### 3.2 Install dependensi

```bash
pip install -r requirements.txt
# atau:
uv pip install -r requirements.txt
```

Isi `requirements.txt`:

```
celery>=5.3
flower>=2.0
```

### 3.3 Jalankan RabbitMQ (opsional, untuk demo via broker asli)

Pastikan RabbitMQ sudah berjalan di `localhost:5672`.
Cara paling cepat dengan Docker:

```bash
docker run -d --name rabbit \
  -p 5672:5672 -p 15672:15672 \
  rabbitmq:3-management
```

RabbitMQ Management UI: <http://localhost:15672> (guest/guest).

---

## 4. Menjalankan Sample Awal (process_data → generate_report)

### 4.1 Worker

Buka 3 terminal (atau 2 jika tidak butuh orchestrator):

```bash
# Terminal A: queue data_processing
celery -A celery_config worker -Q data_processing   -n dp@%h  -l info

# Terminal B: queue report_generation
celery -A celery_config worker -Q report_generation -n rg@%h  -l info

# Terminal C: general worker (menangani task orchestrator)
celery -A celery_config worker                      -n gen@%h -l info
```

### 4.2 Producer (single-shot)

```bash
python producer.py
```

Keluaran yang diharapkan:
- Cara 1 (orchestrator) mengeksekusi `process_and_report`, yang di dalamnya
  memanggil `process_data` lalu `generate_report`.
- Cara 2 menggunakan `chain(process_data.s(data), generate_report.s())`
  — output task pertama otomatis menjadi input task kedua.

### 4.3 Producer sebagai Input-Generator (stream)

```bash
python producer.py --stream --count 10 --interval 1.0
```

Setiap 1 detik, producer membangkitkan satu kalimat input baru (topik acak),
mengirimkannya ke broker sebagai `chain(process_data → generate_report)`, dan
mencetak `task_id`. Pada akhir, producer menunggu seluruh task selesai.

---

## 5. Demo Message Handling (Fan-Out, Topic, Wildcard)

### 5.1 Fan-Out

Fan-Out = **broadcast**. Satu pesan masuk ke exchange → disalin ke setiap queue
yang ter-bind. *Routing key diabaikan.*

**Worker (3 terminal):**

```bash
celery -A celery_config worker -Q fanout_email -n email@%h -l info
celery -A celery_config worker -Q fanout_sms   -n sms@%h   -l info
celery -A celery_config worker -Q fanout_push  -n push@%h  -l info
```

**Producer:**

```bash
python producer_fanout.py
```

Pesan yang sama akan muncul di **ketiga** log worker.

### 5.2 Topic

Topic exchange me-route berdasarkan pola segmen ber-titik pada `routing_key`.

Binding yang dipakai:

| Queue           | Pola binding | Contoh routing_key yang cocok |
|-----------------|--------------|-------------------------------|
| `topic_info`    | `info.*`     | `info.auth`, `info.payment`   |
| `topic_error`   | `error.*`    | `error.auth`, `error.payment` |
| `topic_payment` | `*.payment`  | `info.payment`, `error.payment` |

**Worker (3 terminal):**

```bash
celery -A celery_config worker -Q topic_info    -n info@%h    -l info
celery -A celery_config worker -Q topic_error   -n error@%h   -l info
celery -A celery_config worker -Q topic_payment -n payment@%h -l info
```

**Producer:**

```bash
python producer_topic.py
```

Perhatikan pesan `info.payment` masuk ke worker *info* **dan** *payment*
sekaligus (double-consume), sementara `debug.auth` di-drop karena tidak
ada binding yang cocok.

### 5.3 Wildcard

Topic exchange mendukung dua simbol wildcard:

- `*` = **tepat satu** segmen.
- `#` = **nol atau lebih** segmen.

Binding yang dipakai:

| Queue                | Pola binding    | Contoh cocok                          |
|----------------------|------------------|---------------------------------------|
| `wc_asia_all`        | `asia.#`         | `asia.prod.api`, `asia.dev.database`  |
| `wc_prod_any_region` | `*.prod.*`       | `asia.prod.api`, `eu.prod.database`   |
| `wc_all_db`          | `#.database`     | `asia.dev.database`, `us.database`    |

**Worker (3 terminal):**

```bash
celery -A celery_config worker -Q wc_asia_all        -n asia@%h -l info
celery -A celery_config worker -Q wc_prod_any_region -n prod@%h -l info
celery -A celery_config worker -Q wc_all_db          -n db@%h   -l info
```

**Producer:**

```bash
python producer_wildcard.py
```

### 5.4 Jalankan Seluruh Demo Sekaligus

```bash
python producer.py --demo-messaging
```

Pastikan worker untuk ketiga skema sudah dinyalakan.

---

## 6. Monitoring dengan Flower

```bash
celery -A celery_config flower
```

Buka <http://localhost:5555> untuk melihat worker aktif, task yang sedang
berjalan, dan statistik queue.

---

## 7. Troubleshooting

| Gejala                               | Kemungkinan penyebab & solusi                                    |
|--------------------------------------|------------------------------------------------------------------|
| `ImportError: attempted relative import …` | Pastikan menjalankan skrip langsung (`python producer.py`), bukan sebagai package. Kode telah diperbaiki untuk menggunakan import absolut. |
| Task tidak pernah selesai            | Worker untuk queue terkait belum dinyalakan, atau RabbitMQ mati. |
| Pesan topic tidak diterima           | Routing key tidak cocok dengan pola binding. Cek tabel di §5.2.  |
| `Received unregistered task`         | Worker lupa men-import modul task. Jalankan worker dari direktori project agar `celery_config` dan `messaging_tasks` dapat di-import. |

---

## 8. Lihat Juga

- [`docs/SIMULATION.md`](docs/SIMULATION.md) — Gambaran arsitektur, diagram
  alur per-skema, peta kode (di file mana nyari apa), & contoh hasil.
- [`docs/EXPLORATION.md`](docs/EXPLORATION.md) — Perbandingan Fan-Out vs Topic
  vs Wildcard beserta kapan sebaiknya memilih yang mana.
- [Dokumentasi resmi Celery](https://docs.celeryq.dev/)
- [Dokumentasi Kombu Exchange & Queue](https://docs.celeryq.dev/projects/kombu/en/stable/userguide/examples.html)
