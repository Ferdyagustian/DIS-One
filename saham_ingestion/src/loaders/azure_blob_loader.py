import os
import logging
from pathlib import Path
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)

class AzureBlobLoader:
    """
    Kelas untuk mengunggah file lokal (misal Parquet) ke Azure Blob Storage.
    """
    def __init__(self, connection_string: str, container_name: str):
        self.connection_string = connection_string
        self.container_name = container_name
        try:
            self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        except Exception as e:
            logger.error(f"Gagal menginisiasi Azure Blob Client: {e}")
            raise

    def upload_file(self, local_file_path: str, blob_name_prefix: str = "") -> str:
        """
        Mengunggah satu file ke blob storage.
        `blob_name_prefix` bisa digunakan untuk menaruh di dalam sub-folder (contoh: "stocks/")
        """
        if not os.path.exists(local_file_path):
            logger.error(f"File tidak ditemukan: {local_file_path}")
            return None

        # Konstruksi struktur blob_name (path di Azure)
        # Misal local_file_path = "/tmp/stocks_data/ticker=BBCA.JK/year=2023/month=10/data.parquet"
        # Kita ekstrak path relatifnya dari "/tmp/stocks_data/"
        
        # Gunakan pathlib.Path agar aman di Windows (backslash) maupun Unix (slash)
        path_obj = Path(local_file_path)
        path_parts = path_obj.parts
        # Ambil bagian 'ticker=.../year=.../month=.../file.parquet'
        # Asumsi struktur foldernya selalu ada 'ticker='
        try:
            ticker_idx = [i for i, part in enumerate(path_parts) if part.startswith('ticker=')][0]
            relative_path = "/".join(path_parts[ticker_idx:])
        except IndexError:
            # Fallback jika tidak ada struktur ticker=
            relative_path = path_obj.name
            
        blob_name = f"{blob_name_prefix}/{relative_path}" if blob_name_prefix else relative_path
        blob_name = blob_name.replace("//", "/") # Bersihkan double slash jika ada
        
        try:
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)
            
            with open(local_file_path, "rb") as data:
                logger.info(f"Mengunggah ke Azure: {blob_name}")
                blob_client.upload_blob(data, overwrite=True)
                
            logger.info("Upload berhasil!")
            return blob_name
            
        except Exception as e:
            logger.error(f"Gagal mengunggah file {local_file_path} ke Azure: {e}")
            return None
