# Dokumentasi Proyek: IHSG Data Ingestion Pipeline 📈

## 1. Ikhtisar Proyek (Overview)
Proyek ini adalah sebuah *Enterprise Data Pipeline* otomatis yang dibangun menggunakan **Apache Airflow** (melalui Docker). Tujuan utama dari *pipeline* ini adalah untuk mengekstrak data saham harian dari Bursa Efek Indonesia (IHSG) menggunakan antarmuka Yahoo Finance, membersihkannya, dan memuatnya ke dalam **Azure Blob Storage** secara terstruktur untuk keperluan analisis *Data Science* atau pelaporan tingkat lanjut.

## 2. Arsitektur Data (Medallion Architecture)
Sistem penyimpanan menganut standar *Medallion Architecture* (Pemisahan Lapisan Data) untuk memisahkan data kotor dan data bersih, sehingga menjamin kualitas analisis *downstream*.

### 🥉 Lapisan Bronze (Raw Data)
- **Format:** `CSV`
- **Tujuan:** Menyimpan rekaman asli dari sumber (Yahoo Finance) tanpa diubah sedikit pun. Berfungsi ganda sebagai *Audit Trail* dan cadangan historis.
- **Lokasi Azure:** `bronze/stocks/ticker={kode_saham}/raw_{tanggal}.csv`

### 🥈 Lapisan Silver (Cleaned Data)
- **Format:** `Parquet` (Sangat efisien dalam kompresi ukuran dan sangat cepat dibaca oleh mesin analitik seperti Apache Spark / Pandas).
- **Tujuan:** Menyimpan data yang telah melewati pemeriksaan dan validasi kualitas otomatis.
- **Proses Transformasi (Data Quality):**
  - Menghapus baris yang memiliki nilai harga Kosong (*NaN*).
  - Menghapus hari libur bursa (membuang baris dimana nilai *Volume = 0*).
  - Memastikan *High* >= *Low*.
- **Lokasi Azure:** `silver/stocks/ticker={kode_saham}/year={tahun}/month={bulan}/data_{tahun}_{bulan}.parquet` (Skema partisi otomatis berdasarkan Tahun dan Bulan).

## 3. Komponen Kode Utama (Modul `src/`)
Kode telah disusun secara modular (*Modular Programming*) agar mudah dikelola dan diuji secara independen.

1. **`extractors/yfinance_extractor.py`**
   Bertugas menghubungi API Yahoo Finance dan menarik rentang harga saham secara dinamis. Modul ini patuh pada siklus tanggal eksekusi (Idempotent).
2. **`transformers/parquet_transformer.py`**
   Bertugas merapikan struktur kolom, menjalankan logika *Data Quality* (kualitas data dasar), dan memadatkan file mentah `CSV` menjadi file `Parquet` berpartisi.
3. **`loaders/azure_blob_loader.py`**
   Bertugas menangani autentikasi aman (*Secure Auth*) dan memfasilitasi transfer *upload* data dari penyimpanan lokal *container* ke *cloud* (Azure Blob Storage).

## 4. Orkestrasi Airflow (DAG)
File pusat yang mengontrol jadwal dan urutan proses berlokasi di: `dags/daily_ihsg_ingestion.py`

### Fitur Modern Airflow yang Digunakan:
- **TaskFlow API (`@task`):** Menulis *task* menggunakan fungsi *Python* murni alih-alih menggunakan pola kelas (*Class Operators*) bawaan versi lama, membuat kode lebih *Pythonic*.
- **Dynamic Task Mapping (`.expand()`):** Alih-alih membuat struktur pengulangan statis (*For-Loop*), *pipeline* ini bisa menciptakan 8 atau 100 buah sub-proses independen secara seketika (*parallel processing*) yang diekspansi secara dinamis sesuai jumlah saham. Jika 1 saham mengalami gangguan jaringan, tugas paralel saham lainnya tetap akan berjalan hingga sukses.
- **Idempotency:** *Pipeline* dijamin 100% menggunakan `logical_date` dari Airflow alih-alih menggunakan `datetime.now()` dari OS. Ini memastikan data historis yang lampau bisa diekstrak ulang (*Backfill*) kapan saja tanpa pernah salah rentang waktu pengambilan.

## 5. Panduan Konfigurasi (How to Run)

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

## 6. Jadwal Eksekusi Default
- **Jadwal (*Cron Schedule*):** `0 18 * * 1-5`
- **Penjelasan:** Pukul 18:00 WIB, setiap hari Senin hingga Jumat. 
- Waktu ini merupakan *best-practice* karena seluruh aktivitas dan pencatatan nilai Bursa Efek Indonesia telah ditutup secara sempurna setiap sore hari.
