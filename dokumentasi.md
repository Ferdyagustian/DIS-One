# Dokumentasi Proyek: IHSG Data Ingestion Pipeline 📈

## 1. Ikhtisar Proyek (Overview)
Proyek ini adalah sebuah *Enterprise Data Pipeline* otomatis yang dibangun menggunakan **Apache Airflow** (melalui Docker). Tujuan utama dari *pipeline* ini adalah untuk mengekstrak data saham harian dari Bursa Efek Indonesia (IHSG) menggunakan antarmuka Yahoo Finance, membersihkannya, memperkayanya dengan indikator teknikal, dan memuatnya ke dalam **Azure Blob Storage** secara terstruktur untuk keperluan analisis *Data Science* atau pelaporan tingkat lanjut.

## 2. Arsitektur Data (Medallion Architecture)
Sistem penyimpanan menganut standar *Medallion Architecture* (Pemisahan Lapisan Data) 3 tingkat untuk memisahkan data kotor, data bersih, dan data siap analisis, sehingga menjamin kualitas analisis *downstream*.

### 🥉 Lapisan Bronze (Raw Data)
- **Format:** `CSV`
- **Tujuan:** Menyimpan rekaman asli dari sumber (Yahoo Finance) tanpa diubah sedikit pun. Berfungsi ganda sebagai *Audit Trail* dan cadangan historis.
- **Lokasi Azure:** `bronze/stocks/ticker={kode_saham}/year={tahun}/month={bulan}/raw_{tanggal}.csv` (Skema partisi konsisten dengan Silver).

### 🥈 Lapisan Silver (Cleaned Data)
- **Format:** `Parquet` (Sangat efisien dalam kompresi ukuran dan sangat cepat dibaca oleh mesin analitik seperti Apache Spark / Pandas).
- **Tujuan:** Menyimpan data yang telah melewati pemeriksaan dan validasi kualitas otomatis.
- **Proses Validasi (Data Quality):**
  - **Pandera Schema Validation** (lazy mode): Memvalidasi tipe data, range, dan format secara otomatis menggunakan `SilverOHLCVSchema`. Baris yang gagal validasi di-*quarantine* (dibuang) tanpa menghentikan pipeline.
  - Menghapus baris yang memiliki nilai harga Kosong (*NaN*).
  - Menghapus hari libur bursa (membuang baris dimana nilai *Volume = 0*).
  - Memastikan *High* >= *Low*.
- **Lokasi Azure:** `silver/stocks/ticker={kode_saham}/year={tahun}/month={bulan}/data_{tahun}_{bulan}.parquet` (Skema partisi otomatis berdasarkan Tahun dan Bulan).

### 🥇 Lapisan Gold (Enriched Data — ML-Ready)
- **Format:** `Parquet`
- **Tujuan:** Menyimpan data yang telah diperkaya dengan indikator teknikal untuk keperluan analisis tren, momentum, dan *machine learning*.
- **Indikator Teknikal v1:**
  - **SMA-5** (Simple Moving Average 5 hari) — Tren jangka pendek.
  - **SMA-20** (Simple Moving Average 20 hari) — Tren jangka menengah.
  - **Daily Return (%)** — Persentase perubahan harga harian.
  - **RSI-14** (Relative Strength Index 14 hari) — Momentum *overbought/oversold*, dihitung menggunakan *Exponential Moving Average* (EMA).
- **Lokasi Azure:** `gold/stocks/ticker={kode_saham}/year={tahun}/month={bulan}/enriched_{tahun}_{bulan}.parquet`

## 3. Komponen Kode Utama (Modul `src/`)
Kode telah disusun secara modular (*Modular Programming*) agar mudah dikelola dan diuji secara independen.

1. **`extractors/yfinance_extractor.py`**
   Bertugas menghubungi API Yahoo Finance dan menarik rentang harga saham secara dinamis. Modul ini patuh pada siklus tanggal eksekusi (Idempotent).
2. **`transformers/parquet_transformer.py`**
   Bertugas merapikan struktur kolom, menjalankan validasi schema menggunakan **Pandera** (lazy mode), dan memadatkan file mentah `CSV` menjadi file `Parquet` berpartisi (Silver layer).
3. **`transformers/gold_aggregator.py`** *(BARU)*
   Bertugas menghasilkan data Gold layer dengan menambahkan indikator teknikal: SMA-5, SMA-20, Daily Return, dan RSI-14.
4. **`loaders/azure_blob_loader.py`**
   Bertugas menangani autentikasi aman (*Secure Auth*) dan memfasilitasi transfer *upload* data dari penyimpanan lokal *container* ke *cloud* (Azure Blob Storage).
5. **`schemas.py`** *(BARU)*
   Mendefinisikan *Pandera DataFrameModel* schema (`RawOHLCVSchema` dan `SilverOHLCVSchema`) untuk validasi data otomatis berbasis kontrak schema.

## 4. Orkestrasi Airflow (DAG)
File pusat yang mengontrol jadwal dan urutan proses berlokasi di: `dags/daily_ihsg_ingestion.py`

### Fitur Modern Airflow yang Digunakan:
- **TaskFlow API (`@task`):** Menulis *task* menggunakan fungsi *Python* murni alih-alih menggunakan pola kelas (*Class Operators*) bawaan versi lama, membuat kode lebih *Pythonic*.
- **Dynamic Task Mapping (`.expand()`):** Alih-alih membuat struktur pengulangan statis (*For-Loop*), *pipeline* ini bisa menciptakan 8 atau 100 buah sub-proses independen secara seketika (*parallel processing*) yang diekspansi secara dinamis sesuai jumlah saham. Jika 1 saham mengalami gangguan jaringan, tugas paralel saham lainnya tetap akan berjalan hingga sukses.
- **Idempotency:** *Pipeline* dijamin 100% menggunakan `logical_date` dari Airflow alih-alih menggunakan `datetime.now()` dari OS. Ini memastikan data historis yang lampau bisa diekstrak ulang (*Backfill*) kapan saja tanpa pernah salah rentang waktu pengambilan.
- **Cleanup Teardown Task** *(BARU)*: Task `cleanup_temp_data()` dengan `trigger_rule="all_done"` menjamin pembersihan file temporary di `temp_data/` setelah setiap run, bahkan jika ada task yang gagal. Mencegah akumulasi file lokal yang tidak terpakai.

## 5. Infrastruktur Docker

### Custom Docker Image *(BARU)*
Sistem sekarang menggunakan **custom Docker image** (`Dockerfile`) yang meng-*extend* `apache/airflow:2.9.2` dengan semua dependency yang sudah di-*bake-in*. Ini menggantikan `_PIP_ADDITIONAL_REQUIREMENTS` yang sebelumnya menyebabkan install ulang setiap container restart.

**Keuntungan:**
- Container start lebih cepat (tidak ada `pip install` saat startup).
- Environment reproducible (versi dependency di-pin di `requirements.txt`).
- Konsisten di semua node worker (Celery).

**Cara build ulang image setelah mengubah dependency:**
```bash
docker-compose build
docker-compose up -d
```

### Dependency yang di-Pin:
| Package | Versi | Fungsi |
|---------|-------|--------|
| `yfinance` | 0.2.43 | Sumber data saham |
| `pandas` | 2.2.2 | Manipulasi DataFrame |
| `pyarrow` | 16.1.0 | Engine Parquet |
| `azure-storage-blob` | 12.22.0 | Upload ke Azure |
| `pandera` | 0.20.4 | Schema validation |
| `pytest` | 8.3.2 | Unit testing |
| `pytest-mock` | 3.14.0 | Mock testing |

## 6. Panduan Konfigurasi (How to Run)

### A. Kredensial Keamanan (Azure Connection)
Sistem ini TIDAK menyimpan kata sandi (*Connection String*) secara *hardcode* di dalam *script* maupun *environment variables* demi standar keamanan *(Cybersecurity Hardening)*.
1. Buka **Airflow Web UI** -> menu atas **Admin** -> **Connections**.
2. Buat koneksi baru (*Add*).
3. Isi `Conn ID` dengan: `azure_blob_default`
4. Pilih `Conn Type`: **Azure Blob Storage**.
5. Tempelkan *Azure Connection String* Anda di kolom isian rahasia (*Password* / *Extra*).

### B. Menambah / Mengubah Daftar Saham (Dynamic Config)
Daftar saham (seperti BBCA, GOTO) yang diunduh bebas diubah kapan saja secara langsung di UI.
1. Buka **Airflow Web UI** -> menu atas **Admin** -> **Variables**.
2. Buat variabel baru.
3. Isi `Key`: `ihsg_tickers`.
4. Isi `Val` berformat struktur *JSON List*, misalnya: `["BBCA", "BBRI", "BMRI", "AMMN", "BREN"]`.
5. Pastikan menekan centang `Deserialize JSON` (jika muncul) lalu simpan.

## 7. Jadwal Eksekusi Default
- **Jadwal (*Cron Schedule*):** `0 18 * * 1-5`
- **Penjelasan:** Pukul 18:00 WIB, setiap hari Senin hingga Jumat. 
- Waktu ini merupakan *best-practice* karena seluruh aktivitas dan pencatatan nilai Bursa Efek Indonesia telah ditutup secara sempurna setiap sore hari.

## 8. Unit Testing *(BARU)*
Pipeline ini dilengkapi dengan *test suite* menggunakan `pytest` dan `pytest-mock`. Test berjalan tanpa koneksi jaringan (semua API call di-*mock*).

**Menjalankan Test:**
```bash
cd saham_ingestion
pytest tests/ -v
```

**Cakupan Test:**
| File Test | Modul yang Diuji | Jumlah Test |
|-----------|-------------------|-------------|
| `test_extractor.py` | `YFinanceExtractor` | 6 test |
| `test_transformer.py` | `ParquetTransformer` | 8 test |
| `test_loader.py` | `AzureBlobLoader` | 5 test |
| `test_gold_aggregator.py` | `GoldAggregator` | 8 test |

## 9. Changelog Perbaikan (v1.1)
Daftar perubahan yang diterapkan untuk meningkatkan kualitas, keandalan, dan kelengkapan *pipeline*:

| # | Perubahan | File yang Diubah/Ditambah |
|---|-----------|---------------------------|
| 1 | **Custom Docker Image** — Dependency di-*bake* ke dalam Docker image (`Dockerfile`). Menghilangkan `_PIP_ADDITIONAL_REQUIREMENTS` untuk startup lebih cepat dan environment reproducible. | `Dockerfile` *(baru)*, `docker-compose.yaml`, `requirements.txt` |
| 2 | **Pandera Schema Validation** — Validasi data otomatis menggunakan `DataFrameModel` (lazy mode) di boundary Bronze→Silver. Baris anomali di-*quarantine*, tidak menghentikan pipeline. | `src/schemas.py` *(baru)*, `src/transformers/parquet_transformer.py` |
| 3 | **Cleanup Teardown Task** — Task otomatis membersihkan `temp_data/` setelah setiap run (bahkan saat ada error) menggunakan `trigger_rule="all_done"`. | `dags/daily_ihsg_ingestion.py` |
| 4 | **Unit Test Suite** — 27 unit test dengan `pytest` + `pytest-mock`. Semua API call di-mock (tanpa dependency jaringan). | `tests/` *(baru)*, `pytest.ini` *(baru)* |
| 5 | **Bronze Path Partitioning** — Path Bronze di Azure diubah menjadi `ticker/year/month` agar konsisten dengan Silver. | `dags/daily_ihsg_ingestion.py` |
| 6 | **Gold Layer (v1)** — Lapisan baru dengan 4 indikator teknikal: SMA-5, SMA-20, Daily Return %, dan RSI-14 (EMA smoothed). | `src/transformers/gold_aggregator.py` *(baru)*, `dags/daily_ihsg_ingestion.py` |
| 7 | **Fix Logging** — Menghapus `logging.basicConfig()` di module-level yang bentrok dengan logging Airflow internal. | `src/extractors/yfinance_extractor.py` |
| 8 | **Disable Example DAGs** — `AIRFLOW__CORE__LOAD_EXAMPLES` diubah ke `false` agar UI Airflow bersih. | `docker-compose.yaml` |

## 10. Saran Pengembangan Lanjutan (Roadmap)
Berikut adalah rekomendasi pengembangan yang dapat dilakukan untuk meningkatkan *pipeline* ke tahap selanjutnya:

### 🔴 Prioritas Tinggi (Short-Term)
1. **CI/CD Pipeline** — Integrasikan `pytest` ke dalam GitHub Actions / GitLab CI agar setiap *push* otomatis menjalankan test. Ini mencegah regresi masuk ke production.
2. **Alerting & Monitoring** — Aktifkan `email_on_failure: True` di `default_args` dan hubungkan dengan Slack/Telegram webhook agar tim *data engineering* langsung tahu jika ada task gagal.
3. **Azure Connection String via Secret Manager** — Migrasi dari Airflow Connection UI ke Azure Key Vault atau Hashicorp Vault untuk pengelolaan *secret* yang lebih aman dan auditable di skala production.

### 🟡 Prioritas Menengah (Mid-Term)
4. **Gold Layer v2: Indikator Lanjutan** — Tambahkan MACD (*Moving Average Convergence Divergence*), Bollinger Bands, dan EMA-50/200 jika mulai membangun model prediksi harga.
5. **Data Lineage & Observability** — Integrasikan dengan OpenLineage / Apache Atlas untuk melacak asal-usul data dari Bronze ke Gold secara otomatis.
6. **Backfill Otomatis** — Ubah `catchup=True` dan buat mekanisme *idempotent backfill* untuk mengisi data historis yang terlewat (misal: saat server mati seminggu).
7. **Incremental Processing** — Alih-alih menarik 1 bulan penuh setiap run, implementasikan logika *delta/incremental* yang hanya menarik data baru sejak *last successful run*.

### 🟢 Prioritas Rendah (Long-Term)
8. **Migration ke KubernetesExecutor** — Jika skala bertambah (ratusan ticker), migrasi dari CeleryExecutor ke KubernetesExecutor agar setiap task berjalan di pod terisolasi dengan auto-scaling.
9. **Streaming Architecture** — Jika dibutuhkan data *near-real-time* (intraday), pertimbangkan Apache Kafka / Azure Event Hubs sebagai pengganti batch ingestion.
10. **ML Feature Store Integration** — Hubungkan Gold layer ke Feast atau Tecton sebagai *Feature Store* sehingga indikator teknikal bisa langsung dikonsumsi oleh model ML tanpa preprocessing ulang.

