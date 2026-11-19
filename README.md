# spring-boot-outbox-pattern

Implementasi **Transactional Outbox Pattern** dengan Spring Boot 4.1, MySQL, dan Apache Kafka.

## Masalah yang diselesaikan

Service sering harus melakukan dua hal sekaligus: **menyimpan data ke database** dan **mempublish event ke message
broker**. Keduanya tidak bisa dibungkus dalam satu transaksi, sehingga muncul *dual write problem*:

- Data tersimpan tetapi publish ke Kafka gagal, sehingga event hilang.
- Event terkirim tetapi transaksi DB rollback, sehingga muncul event "hantu".

Outbox pattern menyelesaikannya dengan menulis event ke tabel `outbox_events` **di transaksi yang sama** dengan
perubahan data bisnis. Proses terpisah (relay) lalu membaca tabel itu dan mempublish ke Kafka.

## Arsitektur

```
 POST /api/orders
        │
        ▼
 ┌───────────────── 1 transaksi MySQL ─────────────────┐
 │  INSERT INTO orders ...                             │
 │  INSERT INTO outbox_events (status = 'PENDING') ... │
 └─────────────────────────────────────────────────────┘
        │
        ▼  @Scheduled (polling setiap 1 detik) + @SchedulerLock
 OutboxRelay   ← hanya 1 instance yang memegang lock "outbox-relay"
   SELECT ... WHERE status='PENDING' FOR UPDATE SKIP LOCKED
   kafkaTemplate.send(...).get()   ← tunggu ack broker
   UPDATE status = 'SENT'
        │
        ▼
 Kafka topic "order.events"  (key = orderId, header eventId/eventType)
        │
        ▼
 OrderEventListener  →  cek & simpan eventId di processed_events (idempotent)
```

| Komponen                                           | Tanggung jawab                                                                  |
|----------------------------------------------------|---------------------------------------------------------------------------------|
| `order.OrderService`                               | Menyimpan order dan event-nya dalam satu `@Transactional`                       |
| `outbox.OutboxEventWriter`                         | Serialisasi payload ke JSON dan insert ke outbox (`Propagation.MANDATORY`)      |
| `outbox.OutboxRelay`                               | Polling publisher ke Kafka, retry, menandai `FAILED`, cleanup event lama        |
| `consumer.OrderEventListener`                      | Contoh consumer idempotent memakai tabel `processed_events`                     |
| `config.ShedLockConfig`                            | Lock terdistribusi (tabel `shedlock`) agar relay jalan di satu instance         |
| `db/migration/V*.sql`                              | Skema Flyway: `outbox_events`, `orders`, `processed_events`, `shedlock`         |

## Keputusan desain

- **At-least-once delivery.** Status `SENT` disimpan setelah broker memberi ack. Kalau aplikasi mati di antara
  publish dan commit, event akan dipublish ulang, jadi consumer **wajib idempotent**. Header `eventId` (UUID baris
  outbox) dipakai sebagai kunci deduplikasi.
- **Multi-instance dengan ShedLock.** Job `relay` dan `cleanup` memakai `@SchedulerLock` dengan lock di tabel
  `shedlock` (MySQL, memakai jam database). Saat aplikasi di-scale ke banyak instance, hanya satu instance yang
  menjalankan relay pada satu waktu; instance lain otomatis skip dan mengambil alih kalau instance pemegang lock mati
  (setelah `lock-at-most-for`).
- **Urutan per aggregate.** Event dikirim dengan key `aggregateId`, sehingga semua event satu order masuk ke partisi
  yang sama. Karena relay hanya jalan di satu instance, batch tidak saling menyalip. Relay juga berhenti di event
  pertama yang gagal dalam satu batch supaya event berikutnya tidak mendahului.
- **`FOR UPDATE SKIP LOCKED`** (MySQL 8+) tetap dipakai sebagai pengaman: kalau lock ShedLock kedaluwarsa saat relay
  masih berjalan, dua instance tetap tidak akan mempublish baris yang sama. Untuk mencegah kondisi itu, satu kali
  relay dibatasi `app.outbox.max-run-duration` (default 1 menit), jauh di bawah `lock-at-most-for` (5 menit).
- **Retry & poison message.** Setiap kegagalan menambah `attempts` dan mencatat `last_error`. Setelah
  `app.outbox.max-attempts`, status menjadi `FAILED` dan event perlu ditangani manual.
- **Producer idempotent** (`enable.idempotence=true`, `acks=all`) mencegah duplikat akibat retry di level producer.
- **Cleanup.** Event `SENT` yang lebih lama dari `app.outbox.retention` dihapus setiap jam.

## Menjalankan

Prasyarat: Java 25, Docker.

```bash
# 1. Jalankan MySQL, Kafka (KRaft), dan Kafka UI
docker compose up -d

# 2. Jalankan aplikasi
./mvnw spring-boot:run
```

Kafka UI tersedia di http://localhost:8081.

### Contoh API

```bash
# Buat order → order + event OrderCreated tersimpan atomik
curl -i -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerName":"Hendi","product":"Keyboard","quantity":2,"price":150000}'

# Lihat order
curl http://localhost:8080/api/orders/1

# Batalkan order → event OrderCancelled
curl -X POST http://localhost:8080/api/orders/1/cancel \
  -H 'Content-Type: application/json' \
  -d '{"reason":"Salah pilih produk"}'
```

Cek isi outbox:

```sql
SELECT id, event_type, aggregate_id, status, attempts, created_at, sent_at
FROM outbox_events
ORDER BY created_at DESC;
```

Lihat pesan di Kafka:

```bash
docker exec -it outbox-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 --topic order.events --from-beginning \
  --property print.key=true --property print.headers=true
```

## Konfigurasi

| Property                          | Default        | Keterangan                                   |
|-----------------------------------|----------------|----------------------------------------------|
| `app.outbox.batch-size`           | `100`          | Event maksimum per polling                   |
| `app.outbox.max-attempts`         | `10`           | Batas percobaan sebelum `FAILED`             |
| `app.outbox.send-timeout`         | `10s`          | Waktu tunggu ack broker per event            |
| `app.outbox.retention`            | `7d`           | Umur event `SENT` sebelum dihapus            |
| `app.outbox.relay.enabled`        | `true`         | Aktif/nonaktifkan relay di instance ini      |
| `app.outbox.max-run-duration`     | `1m`           | Batas waktu satu kali relay berjalan         |
| `app.outbox.relay.poll-interval`  | `1000`         | Jeda antar polling (ms)                      |
| `app.outbox.relay.lock-at-most-for` | `PT5M`       | Lock relay dilepas paksa jika instance mati  |
| `app.outbox.relay.lock-at-least-for` | `PT0S`      | Lock relay minimal dipegang selama ini       |
| `app.outbox.cleanup.cron`         | `0 0 * * * *`  | Jadwal cleanup                               |

Koneksi bisa diatur lewat environment variable `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DATABASE`, `MYSQL_USER`,
`MYSQL_PASSWORD`, dan `KAFKA_BOOTSTRAP_SERVERS`.

## Testing

```bash
./mvnw test
```

Integration test memakai **Testcontainers** (MySQL 26.10 & Apache Kafka 4.3), sehingga hanya butuh Docker. Yang diuji:

- Order dan outbox event tersimpan atomik, lalu relay mempublish ke Kafka dan consumer memprosesnya.
- Urutan `OrderCreated` diikuti `OrderCancelled`.
- Request invalid tidak menghasilkan order maupun event.
- `OutboxEventWriter` menolak dipanggil di luar transaksi.
- Consumer idempotent terhadap pesan duplikat.
- Relay tidak mempublish selama instance lain memegang lock ShedLock, lalu lanjut setelah lock dilepas.
- Relay berhenti di kegagalan pertama dan menandai `FAILED` setelah batas percobaan (unit test).
