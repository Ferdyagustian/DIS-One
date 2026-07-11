import yfinance as yf
import logging

# Catatan: logging.basicConfig() dihapus agar tidak bentrok dengan logging Airflow.
# Airflow sudah mengelola konfigurasi logging secara internal.
logger = logging.getLogger(__name__)

class YFinanceExtractor:
    """
    Kelas untuk menarik data OHLCV harian dari Yahoo Finance.
    Difokuskan untuk saham IHSG dengan suffix .JK
    """
    def __init__(self, tickers: list):
        # Tambahkan suffix .JK jika belum ada (asumsi target IHSG)
        self.tickers = [t if t.endswith('.JK') else f"{t}.JK" for t in tickers]

    def extract_historical_data(self, period="max", start_date=None, end_date=None) -> dict:
        """
        Menarik data historis untuk banyak ticker.
        Mengembalikan dictionary di mana key = ticker, value = DataFrame Pandas.
        """
        logger.info(f"Memulai ekstraksi data untuk {len(self.tickers)} ticker: {self.tickers}")
        extracted_data = {}
        
        for ticker in self.tickers:
            try:
                stock = yf.Ticker(ticker)
                
                if start_date and end_date:
                    df = stock.history(start=start_date, end=end_date)
                else:
                    df = stock.history(period=period)
                
                if df.empty:
                    logger.warning(f"Data kosong untuk ticker {ticker}. Mungkin libur atau salah kode.")
                    continue
                
                # Standarisasi nama kolom ke huruf kecil
                df.columns = [c.lower() for c in df.columns]
                
                # Filter kolom yang dibutuhkan saja
                required_cols = ['open', 'high', 'low', 'close', 'volume']
                
                # yfinance 'history' otomatis mereturn harga yang sudah disesuaikan (adjusted) pada kolom Close!
                # Kita akan menduplikasi 'close' menjadi 'adj_close' untuk kejelasan ML.
                # (Catatan: yfinance memberikan Dividend/Splits juga, tapi kita fokus harga dulu)
                df = df[required_cols].copy()
                df['adj_close'] = df['close'] 
                df['ticker'] = ticker
                
                # Optimalisasi Memori Pandas (Datatypes) untuk memangkas ukuran file hingga 50%
                float_cols = ['open', 'high', 'low', 'close', 'adj_close']
                df[float_cols] = df[float_cols].astype('float32')
                df['volume'] = df['volume'].astype('int64')
                
                # Pastikan index Date menjadi kolom biasa agar mudah dikonversi ke Parquet
                df = df.reset_index()
                
                # Standardisasi nama kolom date jika diperlukan
                if 'Date' in df.columns:
                    df.rename(columns={'Date': 'date'}, inplace=True)
                elif 'Datetime' in df.columns:
                    df.rename(columns={'Datetime': 'date'}, inplace=True)

                # Convert timezone aware ke UTC lalu buang info tz agar kompatibel Parquet
                if df['date'].dt.tz is not None:
                    df['date'] = df['date'].dt.tz_convert('UTC').dt.tz_localize(None)
                
                extracted_data[ticker] = df
                logger.info(f"Berhasil menarik {len(df)} baris data untuk {ticker}")
                
            except Exception as e:
                logger.error(f"Gagal menarik data untuk {ticker}: {str(e)}")
                raise
                
        return extracted_data

if __name__ == "__main__":
    # Test ringan
    extractor = YFinanceExtractor(['BBCA', 'GOTO'])
    data = extractor.extract_historical_data(period="5d")
    for k, v in data.items():
        print(f"\nSample data {k}:")
        print(v.head(2))
