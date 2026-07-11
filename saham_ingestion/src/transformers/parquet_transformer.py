import pandas as pd
import logging
import os
import pandera as pa
from schemas import SilverOHLCVSchema

logger = logging.getLogger(__name__)

class ParquetTransformer:
    """
    Kelas untuk membersihkan data dan mengubahnya menjadi format Parquet.
    Menggunakan Pandera untuk schema validation (lazy mode).
    """
    def __init__(self, output_dir: str = "temp_data"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def validate_and_transform(self, ticker: str, df: pd.DataFrame) -> list:
        """
        Memvalidasi data menggunakan Pandera schema, lalu menyimpannya sebagai file
        Parquet terpartisi per bulan. Mengembalikan list of path file Parquet yang dihasilkan.
        """
        if df is None or df.empty:
            logger.warning(f"DataFrame kosong untuk ticker {ticker}. Skip transformasi.")
            return []

        initial_len = len(df)

        # 1. Pandera Schema Validation (lazy mode — kumpulkan semua error sekaligus)
        try:
            df = SilverOHLCVSchema.validate(df, lazy=True)
        except pa.errors.SchemaErrors as e:
            failure_count = len(e.failure_cases)
            logger.warning(f"[{ticker}] Pandera menemukan {failure_count} pelanggaran schema. Memfilter baris invalid...")
            # Quarantine: buang baris yang gagal validasi, lanjutkan yang valid
            failed_indices = e.failure_cases['index'].dropna().unique()
            df = df.drop(index=failed_indices, errors='ignore')

        # 2. Filter tambahan manual: hapus volume=0 (hari libur/suspend), NaN, harga tidak wajar
        df = df[df['volume'] > 0]
        df = df.dropna(subset=['adj_close'])
        df = df[(df['close'] > 0) & (df['high'] >= df['low'])]

        dropped = initial_len - len(df)
        if dropped > 0:
            logger.info(f"[{ticker}] Dihapus {dropped} baris data anomali/kosong.")

        if df.empty:
            logger.warning(f"[{ticker}] Setelah dibersihkan, DataFrame kosong.")
            return []

        # 3. Tambahkan kolom tanggal untuk partisi
        df = df.copy()
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month

        saved_paths = []

        # 4. Simpan ke Parquet terpartisi per BULAN
        # Ini menangani kasus data yang mencakup lebih dari satu bulan (misal: periode 5d melewati akhir bulan)
        for (year_val, month_val), group_df in df.groupby(['year', 'month']):

            partition_dir = os.path.join(
                self.output_dir,
                f"ticker={ticker}",
                f"year={year_val}",
                f"month={month_val:02d}"
            )
            os.makedirs(partition_dir, exist_ok=True)

            # Mengatasi "Small Files Problem": Nama file menggunakan format tahun dan bulan.
            # Ini akan menyebabkan file bulan ini tertimpa (overwrite) oleh versi data yang lebih lengkap.
            file_name = f"data_{year_val}_{month_val:02d}.parquet"
            file_path = os.path.join(partition_dir, file_name)

            # Drop kolom partisi karena foldernya sudah merepresentasikan itu
            df_to_save = group_df.drop(columns=['year', 'month'])

            df_to_save.to_parquet(file_path, engine='pyarrow', index=False)
            logger.info(f"[{ticker}] Berhasil menyimpan file Parquet di: {file_path}")
            saved_paths.append(file_path)

        return saved_paths

