import os
import sys
import shutil
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

# Menambahkan folder src ke sys.path agar Airflow bisa mendeteksi module kita
DAGS_FOLDER = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DAGS_FOLDER)
SRC_FOLDER = os.path.join(PROJECT_ROOT, "src")
if SRC_FOLDER not in sys.path:
    sys.path.insert(0, SRC_FOLDER)

from extractors.yfinance_extractor import YFinanceExtractor
from transformers.parquet_transformer import ParquetTransformer
from transformers.gold_aggregator import GoldAggregator
from loaders.azure_blob_loader import AzureBlobLoader

logger = logging.getLogger(__name__)

# Kredensial Azure akan diambil di level eksekusi Task (bukan global) untuk praktik terbaik

AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME", "stock-data")
LOCAL_TEMP_DIR = os.path.join(PROJECT_ROOT, "temp_data")

def send_telegram_alert(context):
    """
    Mengirimkan notifikasi ke Telegram jika task gagal.
    Menggunakan TelegramHook yang mengambil kredensial dari Airflow Connection 'telegram_default'.
    """
    # Import dilakukan di dalam fungsi agar tidak crash jika package belum terinstall (misal saat pytest lokal)
    from airflow.providers.telegram.hooks.telegram import TelegramHook
    
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    execution_date = context.get('execution_date')
    log_url = task_instance.log_url
    
    # Deteksi jika ini adalah mapped task (dynamic task mapping dari .expand())
    map_index = getattr(task_instance, 'map_index', -1)
    map_str = f" (Map Index: {map_index})" if map_index != -1 else ""

    message = (
        f"🔴 <b>Airflow Task Failed</b> 🔴\n\n"
        f"<b>DAG:</b> {dag_id}\n"
        f"<b>Task:</b> {task_id}{map_str}\n"
        f"<b>Time:</b> {execution_date}\n\n"
        f"🔗 <a href='{log_url}'>Lihat Log di Airflow UI</a>"
    )
    
    try:
        hook = TelegramHook(telegram_conn_id='telegram_default')
        hook.send_message({
            'text': message,
            'parse_mode': 'HTML',
            'disable_web_page_preview': True
        })
        logger.info(f"Berhasil mengirim Telegram alert untuk task {task_id}")
    except Exception as e:
        logger.error(f"Gagal mengirim Telegram alert: {str(e)}")

def send_telegram_success_alert(context):
    """
    Mengirimkan notifikasi ke Telegram HANYA saat seluruh DAG (Pipeline) selesai dengan sukses.
    Ini ditaruh di level DAG untuk mencegah spam dari setiap task individu.
    """
    from airflow.providers.telegram.hooks.telegram import TelegramHook
    
    dag = context.get('dag')
    dag_id = dag.dag_id if dag else "daily_ihsg_ingestion"
    execution_date = context.get('execution_date')

    message = (
        f"✅ <b>Pipeline Berhasil (Success)</b> ✅\n\n"
        f"<b>DAG:</b> {dag_id}\n"
        f"<b>Time:</b> {execution_date}\n\n"
        f"Semua data saham harian telah ditarik, diproses, dan di-upload ke Azure."
    )
    
    try:
        hook = TelegramHook(telegram_conn_id='telegram_default')
        hook.send_message({
            'text': message,
            'parse_mode': 'HTML',
            'disable_web_page_preview': True
        })
        logger.info(f"Berhasil mengirim Telegram SUCCESS alert untuk DAG {dag_id}")
    except Exception as e:
        logger.error(f"Gagal mengirim Telegram SUCCESS alert: {str(e)}")

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_telegram_alert,
}

@task
def get_tickers():
    """
    Mengambil daftar saham secara dinamis dari Airflow Variable saat eksekusi (menghindari beban DB saat parsing).
    Jika belum ada Variable 'ihsg_tickers' di Web UI, gunakan list default.
    """
    from airflow.models import Variable
    return Variable.get("ihsg_tickers", default_var=['BBCA', 'BBRI', 'BMRI', 'TLKM', 'ASII', 'GOTO'], deserialize_json=True)

@task
def process_single_ticker(ticker: str, **context):
    """
    Fungsi utama untuk memproses 1 saham secara independen (Dynamic Task Mapping) dengan Idempotensi Waktu.
    Menjalankan pipeline 3-layer: Bronze (Raw CSV) → Silver (Cleaned Parquet) → Gold (Enriched with Indicators).
    """
    logger.info(f"Memulai proses Ingestion Saham IHSG untuk ticker: {ticker}")
    
    # Idempotensi: Ambil rentang waktu berdasarkan execution date (data_interval_end)
    end_date_dt = context.get('data_interval_end')
    
    # Ambil data dari tanggal 1 di bulan yang sama (1 bulan penuh)
    start_date_dt = end_date_dt.replace(day=1)
    
    # Tambah 1 hari untuk end_date_fetch karena yfinance history bersifat exclusive di tanggal akhir.
    # Ini memastikan data hari eksekusi (data_interval_end) ikut terambil sempurna.
    end_date_fetch_dt = end_date_dt + timedelta(days=1)
    
    start_date_str = start_date_dt.strftime('%Y-%m-%d')
    end_date_str = end_date_fetch_dt.strftime('%Y-%m-%d')
    
    logger.info(f"Mengambil data historis dari {start_date_str} hingga {end_date_str}")

    # 1. Extract
    extractor = YFinanceExtractor(tickers=[ticker])
    raw_data_dict = extractor.extract_historical_data(start_date=start_date_str, end_date=end_date_str)

    # 2. Transform (Silver)
    transformer = ParquetTransformer(output_dir=LOCAL_TEMP_DIR)

    # 3. Transform (Gold)
    gold_aggregator = GoldAggregator(output_dir=LOCAL_TEMP_DIR)

    # 4. Load (Best Practice: Fetch credentials during task execution, not globally)
    azure_conn_str = None
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection("azure_blob_default")
        # Connection string dapat disimpan di kolom password, extra, atau uri
        azure_conn_str = conn.password
        if not azure_conn_str and conn.extra_dejson:
            azure_conn_str = conn.extra_dejson.get("connection_string")
        if not azure_conn_str:
            azure_conn_str = conn.get_uri()
    except Exception:
        # Fallback ke Environment Variable jika koneksi Airflow belum dikonfigurasi
        azure_conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    if not azure_conn_str:
        raise ValueError("Kredensial Azure tidak ditemukan di Airflow Connections maupun Environment Variable.")

    loader = AzureBlobLoader(connection_string=azure_conn_str, container_name=AZURE_CONTAINER_NAME)

    # Iterate over items (walaupun hanya ada 1 key karena sudah disuntikkan .JK di dalam extractor)
    for processed_ticker, df in raw_data_dict.items():
        logger.info(f"Memproses ticker: {processed_ticker}")

        # === 1. TAHAP BRONZE (RAW DATA) ===
        if df is not None and not df.empty:
            # Partisi konsisten dengan Silver: ticker/year/month
            year_str = str(end_date_dt.year)
            month_str = f"{end_date_dt.month:02d}"
            bronze_dir = os.path.join(
                LOCAL_TEMP_DIR, "bronze",
                f"ticker={processed_ticker}",
                f"year={year_str}",
                f"month={month_str}"
            )
            os.makedirs(bronze_dir, exist_ok=True)
            
            # Gunakan execution_date string sebagai identifier raw data
            bronze_filename = f"raw_{end_date_str}.csv"
            bronze_local_path = os.path.join(bronze_dir, bronze_filename)
            
            # Simpan DataFrame persis seperti saat ditarik dari yfinance
            df.to_csv(bronze_local_path, index=False)
            logger.info(f"[{processed_ticker}] Raw data CSV tersimpan di lokal: {bronze_local_path}")
            
            # Upload ke lapisan Bronze di Azure
            bronze_azure_path = loader.upload_file(local_file_path=bronze_local_path, blob_name_prefix="bronze/stocks")
            if bronze_azure_path:
                logger.info(f"[{processed_ticker}] [BRONZE] Selesai di-upload ke Azure: {bronze_azure_path}")
            else:
                logger.error(f"[{processed_ticker}] [BRONZE] Gagal upload ke Azure.")

        # === 2. TAHAP SILVER (CLEANSED PARQUET) ===
        # Transformasi ke Parquet lokal (termasuk Pandera Schema Validation + Data Quality Checks)
        local_parquet_paths = transformer.validate_and_transform(ticker=processed_ticker, df=df)

        # Upload setiap file partisi ke lapisan Silver di Azure
        for local_parquet_path in local_parquet_paths:
            azure_path = loader.upload_file(local_file_path=local_parquet_path, blob_name_prefix="silver/stocks")
            if azure_path:
                logger.info(f"[{processed_ticker}] [SILVER] Selesai di-upload ke Azure: {azure_path}")
            else:
                logger.error(f"[{processed_ticker}] [SILVER] Gagal upload ke Azure untuk path: {local_parquet_path}")

        # === 3. TAHAP GOLD (ENRICHED WITH TECHNICAL INDICATORS) ===
        # Gunakan data yang sudah bersih (filter ulang dari raw untuk konsistensi)
        if df is not None and not df.empty:
            gold_df = df[df['volume'] > 0].dropna(subset=['adj_close'])
            gold_df = gold_df[(gold_df['close'] > 0) & (gold_df['high'] >= gold_df['low'])]

            gold_paths = gold_aggregator.aggregate_with_indicators(
                ticker=processed_ticker, df=gold_df
            )
            for gold_path in gold_paths:
                azure_path = loader.upload_file(local_file_path=gold_path, blob_name_prefix="gold/stocks")
                if azure_path:
                    logger.info(f"[{processed_ticker}] [GOLD] Selesai di-upload ke Azure: {azure_path}")
                else:
                    logger.error(f"[{processed_ticker}] [GOLD] Gagal upload ke Azure untuk path: {gold_path}")

@task(trigger_rule="all_done")
def cleanup_temp_data():
    """
    Teardown task: Bersihkan seluruh file temporary di temp_data/ setelah semua proses selesai.
    trigger_rule='all_done' memastikan cleanup tetap jalan meskipun ada task yang gagal,
    sehingga temp_data/ tidak menumpuk kalau ada error di tengah pipeline.
    """
    logger.info("Memulai cleanup temp_data/ ...")

    # Hapus seluruh isi temp_data/ kecuali folder itu sendiri
    if os.path.exists(LOCAL_TEMP_DIR):
        for item in os.listdir(LOCAL_TEMP_DIR):
            item_path = os.path.join(LOCAL_TEMP_DIR, item)
            try:
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                    logger.info(f"Cleanup: Menghapus direktori {item_path}")
                else:
                    os.remove(item_path)
                    logger.info(f"Cleanup: Menghapus file {item_path}")
            except OSError as e:
                logger.warning(f"Cleanup gagal untuk {item_path}: {e}")

    logger.info("Cleanup temp_data/ selesai.")

with DAG(
    'daily_ihsg_ingestion',
    default_args=default_args,
    description='Pipeline 3-layer (Bronze/Silver/Gold) data saham IHSG ke Azure Blob Storage',
    schedule='0 18 * * 1-5',  # Jam 18:00 tiap Senin-Jumat (WIB = UTC+7, sesuaikan ke UTC jika perlu)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['finance', 'ihsg', 'ml-ingestion'],
    on_success_callback=send_telegram_success_alert,
) as dag:

    # Mengambil konfigurasi saham secara dinamis
    tickers = get_tickers()
    
    # Expand tugas untuk masing-masing ticker (Dynamic Task Mapping)
    processing = process_single_ticker.expand(ticker=tickers)

    # Cleanup temp files setelah semua task selesai (trigger_rule="all_done")
    cleanup = cleanup_temp_data()

    # Dependency: cleanup dijalankan setelah semua processing selesai
    processing >> cleanup
