import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)


class GoldAggregator:
    """
    Menghasilkan data agregat level Gold dari Silver layer.
    Menambahkan indikator teknikal untuk keperluan analisis tren dan momentum.

    Indikator v1:
    - SMA-5  (Simple Moving Average 5 hari — tren jangka pendek)
    - SMA-20 (Simple Moving Average 20 hari — tren jangka menengah)
    - Daily Return (%) — perubahan harga harian
    - RSI-14 (Relative Strength Index 14 hari — momentum overbought/oversold)
    """
    def __init__(self, output_dir: str = "temp_data"):
        self.output_dir = output_dir

    @staticmethod
    def _compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
        """
        Menghitung Relative Strength Index (RSI).
        RSI = 100 - (100 / (1 + RS))
        RS = Average Gain / Average Loss selama `period` hari.
        """
        delta = series.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = (-delta).where(delta < 0, 0.0)

        # Menggunakan Exponential Moving Average (EMA) untuk smoothing yang lebih akurat
        avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
        avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def aggregate_with_indicators(self, ticker: str, df: pd.DataFrame) -> list:
        """
        Menambahkan indikator teknikal dan menyimpan ke Gold layer.
        Mengembalikan list of path file Parquet yang dihasilkan.
        """
        if df is None or df.empty:
            logger.warning(f"[{ticker}] DataFrame kosong. Skip Gold aggregation.")
            return []

        df = df.copy()
        df = df.sort_values('date')

        # === Indikator Teknikal v1 ===
        # SMA-5: Simple Moving Average 5 hari (tren jangka pendek)
        df['sma_5'] = df['close'].rolling(window=5, min_periods=1).mean()

        # SMA-20: Simple Moving Average 20 hari (tren jangka menengah)
        df['sma_20'] = df['close'].rolling(window=20, min_periods=1).mean()

        # Daily Return: Persentase perubahan harga harian
        df['daily_return_pct'] = df['close'].pct_change() * 100

        # RSI-14: Relative Strength Index 14 hari (momentum)
        df['rsi_14'] = self._compute_rsi(df['close'], period=14)

        # Optimasi tipe data untuk kolom indikator baru
        indicator_cols = ['sma_5', 'sma_20', 'daily_return_pct', 'rsi_14']
        for col in indicator_cols:
            df[col] = df[col].astype('float32')

        # Partisi per bulan (konsisten dengan Silver)
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month

        saved_paths = []

        for (year_val, month_val), group_df in df.groupby(['year', 'month']):
            partition_dir = os.path.join(
                self.output_dir, "gold",
                f"ticker={ticker}",
                f"year={year_val}",
                f"month={month_val:02d}"
            )
            os.makedirs(partition_dir, exist_ok=True)

            file_name = f"enriched_{year_val}_{month_val:02d}.parquet"
            file_path = os.path.join(partition_dir, file_name)

            df_to_save = group_df.drop(columns=['year', 'month'])
            df_to_save.to_parquet(file_path, engine='pyarrow', index=False)
            logger.info(f"[{ticker}] Gold parquet tersimpan: {file_path}")
            saved_paths.append(file_path)

        return saved_paths
