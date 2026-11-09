# Eksplorasi Message Handling di Celery / RabbitMQ

Dokumen ini merangkum hasil eksplorasi tiga skema *message handling* yang
dicoba pada tugas T2-KBJ: **Fan-Out**, **Topic**, dan **Wildcard**.

Ketiganya diimplementasikan di atas RabbitMQ melalui Celery (modul `kombu`
yang digunakan Celery untuk abstraksi AMQP). Yang berbeda adalah **tipe
exchange** dan **pola binding** antara exchange ↔ queue.

---

## 1. Ringkasan Perbandingan

| Aspek                          | **Fan-Out**                    | **Topic**                                       | **Wildcard** (sub-pola Topic)                            |
|--------------------------------|--------------------------------|-------------------------------------------------|----------------------------------------------------------|
| Tipe exchange                  | `fanout`                       | `topic`                                         | `topic`                                                  |
| Pakai `routing_key`?           | Tidak (diabaikan broker)       | Ya (persis / eksak per binding)                 | Ya (dengan simbol `*` dan `#`)                           |
| Cara memilih queue penerima    | Semua queue yang ter-bind      | Queue yang `routing_key`-nya match pola binding | Queue yang `routing_key`-nya match pola *berisi wildcard* |
| Jumlah queue per pesan         | **N** (semua ter-bind)         | 0, 1, atau beberapa — tergantung match          | 0, 1, atau beberapa — pola jauh lebih fleksibel           |
| Overhead broker                | Paling murah (plain copy)      | Butuh pencocokan string segment                 | Butuh pencocokan pattern (regex-like)                    |
| Contoh use-case                | Broadcast notifikasi, invalidasi cache | Logging multi-service, event-bus ber-kategori | Multi-tenant / multi-region routing, audit per-layer     |

---

## 2. Fan-Out

### 2.1 Karakteristik

- Semua konsumen yang men-subscribe exchange ini akan menerima **salinan**
  pesan yang sama.
- `routing_key` tidak berfungsi untuk pemilihan queue — dapat diisi apa saja
  (termasuk string kosong).
- Model komunikasi: **1 producer → N consumer**, mirip *pub/sub broadcast*.

### 2.2 Implementasi di repo

File:
- `celery_config.py` → `Exchange('notifications_fanout', type='fanout')` dan
  tiga queue yang ter-bind tanpa routing key: `fanout_email`, `fanout_sms`,
  `fanout_push`.
- `messaging_tasks.py` → `notify_broadcast`.
- `producer_fanout.py` → men-dispatch 1 pesan.

### 2.3 Pengamatan

Setelah mengirim 1 pesan, *ketiga* worker mencetak baris
`[FANOUT|notifications_fanout|...] Broadcast diterima: ...`. Artinya broker
men-duplikasi pesan secara independen ke tiap queue.

### 2.4 Cocok untuk

- Push *notification* ke banyak kanal (email / SMS / push) sekaligus.
- Invalidasi cache terdistribusi.
- Replikasi event log ke beberapa sistem downstream.

### 2.5 Tidak cocok untuk

- Routing bersyarat (mis. “kalau error saja kirim ke queue error”). Pakai
  topic exchange.

---

## 3. Topic

### 3.1 Karakteristik

- `routing_key` terstruktur sebagai kata-kata dipisah titik: `"info.auth"`,
  `"error.payment"`.
- Tiap queue di-bind dengan **pola** (string yang mungkin berisi `*` atau `#`
  — walaupun pada "topic murni" pola bisa literal, mis. `info.auth`).
- Broker mengirim pesan ke semua queue yang pola binding-nya *match*.

### 3.2 Implementasi di repo

Binding (lihat `celery_config.py`):
- `topic_info`    ↔ `info.*`
- `topic_error`   ↔ `error.*`
- `topic_payment` ↔ `*.payment`

### 3.3 Pengamatan

Eksperimen `producer_topic.py` mengirim lima *routing_key* berbeda:

| routing_key       | Diterima oleh                             |
|-------------------|--------------------------------------------|
| `info.auth`       | `topic_info`                               |
| `info.payment`    | `topic_info`, `topic_payment`              |
| `error.auth`      | `topic_error`                              |
| `error.payment`   | `topic_error`, `topic_payment`             |
| `debug.auth`      | *(tidak match binding manapun → di-drop)*  |

Catatan penting:

1. **Satu pesan bisa diterima lebih dari satu queue** jika beberapa binding
   match (`info.payment` masuk ke dua queue).
2. **Pesan yang tidak match binding manapun akan hilang** (tidak di-queue).
   Untuk mencegah hilang, bisa tambahkan queue "catch-all" dengan pola `#`,
   atau aktifkan *alternate exchange*.

### 3.4 Cocok untuk

- Logging multi-service dengan kategori (severity × service).
- Event-bus pub/sub di mana subscriber hanya tertarik topik tertentu.

---

## 4. Wildcard (`*` dan `#`)

### 4.1 Karakteristik

Wildcard **bukan tipe exchange tersendiri**, melainkan **kemampuan pola**
pada topic exchange. Dua simbol:

| Simbol | Arti                                   |
|--------|----------------------------------------|
| `*`    | **TEPAT SATU** segmen / kata           |
| `#`    | **NOL atau lebih** segmen / kata       |

### 4.2 Implementasi di repo

Routing key producer berpola `<region>.<env>.<service>` (mis.
`asia.prod.api`), dengan binding:

- `wc_asia_all`        ↔ `asia.#`
- `wc_prod_any_region` ↔ `*.prod.*`
- `wc_all_db`          ↔ `#.database`

### 4.3 Pengamatan

Dari `producer_wildcard.py`:

| routing_key                      | Diterima oleh                               |
|----------------------------------|----------------------------------------------|
| `asia.prod.api`                  | `wc_asia_all`, `wc_prod_any_region`          |
| `asia.dev.database`              | `wc_asia_all`, `wc_all_db`                   |
| `eu.prod.database`               | `wc_prod_any_region`, `wc_all_db`            |
| `us.prod.payment.api`            | *(tidak match)* — `*.prod.*` = tepat 3 segmen |
| `asia.staging.worker.database`   | `wc_asia_all`, `wc_all_db`                   |

Pelajaran penting dari baris ke-4: `*.prod.*` tampak longgar, padahal
*mengunci* jumlah segmen menjadi **tepat 3**. Untuk fleksibilitas jumlah
segmen gunakan `#`.

### 4.4 Cocok untuk

- Routing multi-dimensi (region × env × service, atau tenant × modul × aksi).
- Audit per-layer (`#.database` menangkap semua query ke DB).
- Pattern monitoring berbasis struktur hierarkis.

### 4.5 Jebakan

- Salah memilih antara `*` dan `#` adalah sumber bug paling umum.
- Hirarki routing_key perlu konsisten — ganti urutan segmen = seluruh
  binding harus di-update.

---

## 5. Kapan Memilih Yang Mana?

```
              Butuh semua konsumen dapat pesan yang SAMA?
                              │
                 ┌────────────┴────────────┐
                Ya                        Tidak
                 │                          │
             Fan-Out        Butuh routing berdasarkan atribut/kategori?
                                            │
                                 ┌──────────┴──────────┐
                                Ya                    Tidak
                                 │                      │
                   Pattern routing fleksibel?       Direct exchange
                   (banyak dimensi / level)         (routing_key eksak)
                        │
              ┌─────────┴─────────┐
             Ya                  Tidak
              │                   │
           Wildcard             Topic
       (*.prod.*, asia.#)   (info.auth literal)
```

---

## 6. Penutup

Fan-Out, Topic, dan Wildcard sebenarnya saling melengkapi — bukan mutually
exclusive. Pada sistem nyata lazim mengkombinasikan:
- Fan-Out untuk *notification bus* (mis. semua service perlu tahu user
  logout).
- Topic + wildcard untuk *logging / audit* ke queue terpisah per severity
  atau layer.

Pada kode di repositori ini, ketiganya di-set up berdampingan agar bisa
dibandingkan hanya dengan mengganti exchange target dan pola binding
tanpa mengubah worker.
