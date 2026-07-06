# DIS One 📈

Pipeline data otomatis buat narik harga saham harian dari Bursa Efek Indonesia (IHSG), dibersihkan, terus disimpan rapi di cloud. Dibangun di atas Apache Airflow dan jalan di Docker, jadi tinggal `docker compose up` dan biarkan dia bekerja sendiri tiap sore setelah bursa tutup.

Ide dasarnya sederhana: data mentah jangan langsung dipakai buat analisis, tapi dicuci dulu lewat beberapa lapisan biar makin ke atas makin bersih dan makin gampang dianalisis.

## Kenapa proyek ini ada

Kalau kamu pernah coba tarik data saham manual tiap hari, pasti tau capeknya: buka Yahoo Finance, download CSV, cek ada yang bolong atau nggak, baru bisa dianalisis. DIS One otomatisin semua proses itu — tinggal atur daftar saham yang mau dipantau, sisanya jalan sendiri setiap hari kerja.

## Arsitektur

Proyek ini pakai pendekatan **Medallion Architecture**, jadi data dipisah jadi beberapa lapisan biar jelas mana yang masih mentah dan mana yang udah siap pakai.

### 🥉 Bronze — data mentah
Ini persis kayak yang diambil dari sumbernya, tanpa diutak-atik. Fungsinya sebagai arsip dan jejak audit kalau suatu saat perlu ditelusuri ulang.

- Format: `CSV`
- Lokasi: `bronze/stocks/ticker={kode_saham}/raw_{tanggal}.csv`

### 🥈 Silver — data bersih
Di sini data udah melewati proses validasi dan pembersihan, siap dipakai buat analisis.

Yang dilakukan di tahap ini:
- Baris dengan harga kosong (NaN) dibuang
- Hari libur bursa dibuang (volume = 0)
- Dipastikan nilai High selalu >= Low

Formatnya Parquet karena jauh lebih ringkas dan cepat dibaca dibanding CSV, apalagi kalau nanti diolah pakai Spark atau Pandas.

- Format: `Parquet`
- Lokasi: `silver/stocks/ticker={kode_saham}/year={tahun}/month={bulan}/data_{tahun}_{bulan}.parquet`

Partisi berdasarkan tahun dan bulan ini dibuat otomatis, jadi query data periode tertentu jadi jauh lebih cepat.

## Struktur Kode

Semua logika dipecah jadi modul kecil di dalam `src/`, biar gampang dites dan dikembangin satu-satu tanpa saling ganggu.

| Modul | Tugasnya |
|---|---|
| `extractors/yfinance_extractor.py` | Narik data harga saham dari Yahoo Finance sesuai rentang tanggal eksekusi |
| `transformers/parquet_transformer.py` | Merapikan kolom, jalanin pengecekan kualitas data, ubah CSV jadi Parquet berpartisi |
| `loaders/azure_blob_loader.py` | Autentikasi dan upload hasil akhir ke Azure Blob Storage |

## Orkestrasi (DAG Airflow)

File utama yang ngatur jadwal dan urutan proses ada di `dags/daily_ihsg_ingestion.py`.

Beberapa fitur Airflow modern yang dipakai:

- **TaskFlow API (`@task`)** — task ditulis pakai fungsi Python biasa, bukan class operator model lama, jadi kodenya lebih enak dibaca.
- **Dynamic Task Mapping (`.expand()`)** — daripada bikin loop statis, DAG ini bisa nge-spawn proses paralel sebanyak jumlah saham yang dipantau, entah itu 8 atau 100. Kalau satu saham gagal karena masalah jaringan, saham lain tetap jalan terus sampai selesai.
- **Idempotency** — pipeline pakai `logical_date` dari Airflow, bukan `datetime.now()` dari OS. Artinya kalau perlu backfill data lama, hasilnya tetap akurat dan nggak bakal salah ambil rentang waktu.

## Cara Menjalankan

### 1. Setup koneksi Azure

Connection string sengaja **tidak** disimpan hardcode di script atau environment variable, demi keamanan. Semua diatur lewat Airflow UI:

1. Buka **Airflow Web UI** → **Admin** → **Connections**
2. Klik tambah koneksi baru
3. Isi `Conn ID`: `azure_blob_default`
4. Pilih `Conn Type`: **Azure Blob Storage**
5. Tempel Azure Connection String kamu di kolom rahasia (Password/Extra)

### 2. Atur daftar saham yang mau dipantau

Daftar saham (misalnya BBCA, GOTO) bisa diubah kapan saja tanpa perlu redeploy:

1. Buka **Airflow Web UI** → **Admin** → **Variables**
2. Tambah variabel baru
3. `Key`: `ihsg_tickers`
4. `Val`:
   ```json
   ["BBCA", "BBRI", "BMRI", "AMMN", "BREN"]
   ```
5. Centang `Deserialize JSON` kalau opsinya muncul, lalu simpan

## Jadwal Otomatis

```
0 18 * * 1-5
```

Artinya, tiap hari Senin sampai Jumat pukul 18:00 WIB. Waktu ini dipilih karena seluruh aktivitas dan pencatatan nilai bursa sudah selesai total di jam segitu, jadi data yang diambil pasti sudah final untuk hari itu.

---

Kalau ada saham baru yang mau dipantau atau ada gangguan pipeline, cek dulu Airflow UI — kemungkinan besar solusinya cuma soal koneksi atau daftar saham yang perlu diperbarui, bukan soal kodenya.
