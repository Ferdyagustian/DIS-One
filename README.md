# DIS One 

Pipeline data otomatis untuk mengekstrak harga saham harian dari Bursa Efek Indonesia (IHSG), membersihkannya, kemudian menyimpannya secara terstruktur di cloud. Dibangun di atas Apache Airflow dan berjalan melalui Docker, sehingga cukup dijalankan sekali dan pipeline akan bekerja secara otomatis setiap sore setelah bursa ditutup.

Konsep dasarnya sederhana: data mentah tidak langsung digunakan untuk analisis, melainkan diproses terlebih dahulu melalui beberapa lapisan agar kualitasnya semakin baik dan semakin siap untuk dianalisis.

## Latar Belakang

Proses pengambilan data saham secara manual setiap hari cenderung memakan waktu: membuka Yahoo Finance, mengunduh berkas CSV, memeriksa kelengkapan data, baru kemudian dapat dianalisis. DIS One dirancang untuk mengotomatiskan seluruh proses tersebut. Pengguna hanya perlu menentukan daftar saham yang ingin dipantau, dan pipeline akan berjalan secara mandiri setiap hari kerja.

## Arsitektur

Proyek ini menerapkan pendekatan **Medallion Architecture**, yaitu pemisahan data ke dalam beberapa lapisan agar terdapat perbedaan yang jelas antara data mentah dan data yang telah siap digunakan.

### 🥉 Bronze — Data Mentah
Lapisan ini menyimpan data persis seperti yang diperoleh dari sumbernya, tanpa perubahan apa pun. Fungsinya sebagai arsip historis sekaligus jejak audit apabila diperlukan penelusuran ulang di kemudian hari.

- Format: `CSV`
- Lokasi: `bronze/stocks/ticker={kode_saham}/raw_{tanggal}.csv`

### 🥈 Silver — Data Bersih
Pada lapisan ini, data telah melalui proses validasi dan pembersihan sehingga siap digunakan untuk keperluan analisis.

Proses yang dilakukan pada tahap ini meliputi:
- Penghapusan baris dengan nilai harga kosong (NaN)
- Penghapusan hari libur bursa (nilai volume = 0)
- Verifikasi agar nilai *High* selalu lebih besar atau sama dengan nilai *Low*

Format yang digunakan adalah Parquet, karena lebih ringkas dan lebih cepat dibaca dibandingkan CSV, terutama apabila data akan diolah lebih lanjut menggunakan Spark atau Pandas.

- Format: `Parquet`
- Lokasi: `silver/stocks/ticker={kode_saham}/year={tahun}/month={bulan}/data_{tahun}_{bulan}.parquet`

Skema partisi berdasarkan tahun dan bulan dibuat secara otomatis, sehingga pencarian data pada periode tertentu dapat dilakukan dengan lebih efisien.

## Struktur Kode

Seluruh logika program disusun secara modular di dalam direktori `src/`, sehingga setiap bagian dapat diuji dan dikembangkan secara independen.

| Modul | Fungsi |
|---|---|
| `extractors/yfinance_extractor.py` | Mengambil data harga saham dari Yahoo Finance sesuai rentang tanggal eksekusi |
| `transformers/parquet_transformer.py` | Merapikan struktur kolom, menjalankan proses validasi kualitas data, serta mengonversi berkas CSV menjadi Parquet berpartisi |
| `loaders/azure_blob_loader.py` | Menangani proses autentikasi dan mengunggah hasil akhir ke Azure Blob Storage |

## Orkestrasi (DAG Airflow)

Berkas utama yang mengatur jadwal dan urutan proses berada di `dags/daily_ihsg_ingestion.py`.

Beberapa fitur Airflow modern yang digunakan dalam proyek ini:

- **TaskFlow API (`@task`)** — Setiap task ditulis menggunakan fungsi Python murni, bukan menggunakan pola *class operator* pada versi lama, sehingga kode menjadi lebih ringkas dan mudah dipahami.
- **Dynamic Task Mapping (`.expand()`)** — Alih-alih menggunakan struktur perulangan statis, DAG ini dapat membentuk proses paralel secara dinamis sesuai jumlah saham yang dipantau, baik delapan maupun seratus saham sekaligus. Apabila terjadi gangguan jaringan pada satu saham, proses paralel untuk saham lainnya tetap berjalan hingga selesai.
- **Idempotency** — Pipeline menggunakan `logical_date` dari Airflow, bukan `datetime.now()` dari sistem operasi. Dengan demikian, proses pengambilan ulang data historis (*backfill*) dapat dilakukan kapan saja tanpa risiko kesalahan rentang waktu.

## Panduan Konfigurasi

### 1. Pengaturan Koneksi Azure

Demi keamanan, connection string tidak disimpan secara hardcode di dalam script maupun environment variable. Konfigurasi dilakukan melalui Airflow UI dengan langkah berikut:

1. Buka **Airflow Web UI** → **Admin** → **Connections**
2. Tambahkan koneksi baru
3. Isi `Conn ID` dengan: `azure_blob_default`
4. Pilih `Conn Type`: **Azure Blob Storage**
5. Masukkan Azure Connection String pada kolom yang bersifat rahasia (Password/Extra)

### 2. Pengaturan Daftar Saham

Daftar saham yang dipantau (misalnya BBCA, GOTO) dapat diubah kapan saja tanpa perlu melakukan deployment ulang.

1. Buka **Airflow Web UI** → **Admin** → **Variables**
2. Tambahkan variabel baru
3. Isi `Key` dengan: `ihsg_tickers`
4. Isi `Val` dalam format JSON List, misalnya:
   ```json
   ["BBCA", "BBRI", "BMRI", "AMMN", "BREN"]
   ```
5. Aktifkan opsi `Deserialize JSON` apabila tersedia, kemudian simpan

## Jadwal Eksekusi

```
0 18 * * 1-5
```

Pipeline dijadwalkan berjalan setiap hari Senin hingga Jumat pukul 18.00 WIB. Waktu tersebut dipilih karena seluruh aktivitas dan pencatatan nilai Bursa Efek Indonesia telah selesai secara penuh pada jam tersebut, sehingga data yang diambil dapat dipastikan sudah final untuk hari berjalan.

---

Apabila terdapat kebutuhan untuk menambah saham baru atau terjadi kendala pada pipeline, periksa terlebih dahulu konfigurasi pada Airflow UI. Sebagian besar permasalahan umumnya berkaitan dengan koneksi atau daftar saham yang perlu diperbarui, bukan pada kode itu sendiri.
