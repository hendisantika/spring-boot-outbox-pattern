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
        ▼  @Scheduled (polling setiap 1 detik)
 OutboxRelay
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
| `db/migration/V*.sql`                              | Skema Flyway: `outbox_events`, `orders`, `processed_events`                     |

## Keputusan desain

- **At-least-once delivery.** Status `SENT` disimpan setelah broker memberi ack. Kalau aplikasi mati di antara
  publish dan commit, event akan dipublish ulang, jadi consumer **wajib idempotent**. Header `eventId` (UUID baris
  outbox) dipakai sebagai kunci deduplikasi.
- **`FOR UPDATE SKIP LOCKED`** (MySQL 8+). Beberapa instance aplikasi bisa menjalankan relay bersamaan tanpa
  mempublish baris yang sama.
- **Urutan per aggregate.** Event dikirim dengan key `aggregateId`, sehingga semua event satu order masuk ke partisi
  yang sama. Relay berhenti di event pertama yang gagal dalam satu batch supaya event berikutnya tidak menyalip.
  Catatan: dengan banyak instance relay, dua batch berbeda bisa berisi event dari order yang sama. Kalau urutan
  global per aggregate sangat penting, jalankan relay di satu instance aktif (mis. dengan leader election / ShedLock).
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
| `app.outbox.relay.poll-interval`  | `1000`         | Jeda antar polling (ms)                      |
| `app.outbox.cleanup.cron`         | `0 0 * * * *`  | Jadwal cleanup                               |

Koneksi bisa diatur lewat environment variable `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DATABASE`, `MYSQL_USER`,
`MYSQL_PASSWORD`, dan `KAFKA_BOOTSTRAP_SERVERS`.

## Testing

```bash
./mvnw test
```

Integration test memakai **Testcontainers** (MySQL 8.4 & Apache Kafka 4.1), sehingga hanya butuh Docker. Yang diuji:

- Order dan outbox event tersimpan atomik, lalu relay mempublish ke Kafka dan consumer memprosesnya.
- Urutan `OrderCreated` diikuti `OrderCancelled`.
- Request invalid tidak menghasilkan order maupun event.
- `OutboxEventWriter` menolak dipanggil di luar transaksi.
- Consumer idempotent terhadap pesan duplikat.
- Relay berhenti di kegagalan pertama dan menandai `FAILED` setelah batas percobaan (unit test).
