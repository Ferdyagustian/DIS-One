import pytest
from unittest.mock import patch, MagicMock
from loaders.azure_blob_loader import AzureBlobLoader


class TestAzureBlobLoader:
    """Test suite untuk AzureBlobLoader dengan mock Azure SDK."""

    @patch('loaders.azure_blob_loader.BlobServiceClient')
    def test_upload_success(self, mock_bsc, tmp_path):
        """Upload file yang ada harus berhasil dan mengembalikan blob name."""
        mock_bsc.from_connection_string.return_value = MagicMock()

        loader = AzureBlobLoader(connection_string="fake-conn-string", container_name="stock-data")

        # Buat file test dengan struktur partisi
        test_file = tmp_path / "ticker=BBCA.JK" / "year=2024" / "month=07" / "data_2024_07.parquet"
        test_file.parent.mkdir(parents=True)
        test_file.write_bytes(b"fake parquet content")

        result = loader.upload_file(str(test_file), blob_name_prefix="silver/stocks")
        assert result is not None
        assert "ticker=BBCA.JK" in result
        assert "silver/stocks" in result

    @patch('loaders.azure_blob_loader.BlobServiceClient')
    def test_upload_file_not_found(self, mock_bsc):
        """Upload file yang tidak ada harus mengembalikan None."""
        mock_bsc.from_connection_string.return_value = MagicMock()
        loader = AzureBlobLoader(connection_string="fake-conn-string", container_name="stock-data")
        result = loader.upload_file("/nonexistent/file.parquet", blob_name_prefix="test")
        assert result is None

    @patch('loaders.azure_blob_loader.BlobServiceClient')
    def test_upload_without_prefix(self, mock_bsc, tmp_path):
        """Upload tanpa prefix harus tetap berfungsi."""
        mock_bsc.from_connection_string.return_value = MagicMock()
        loader = AzureBlobLoader(connection_string="fake-conn-string", container_name="stock-data")

        test_file = tmp_path / "ticker=GOTO.JK" / "data.parquet"
        test_file.parent.mkdir(parents=True)
        test_file.write_bytes(b"fake parquet content")

        result = loader.upload_file(str(test_file), blob_name_prefix="")
        assert result is not None
        assert "ticker=GOTO.JK" in result

    @patch('loaders.azure_blob_loader.BlobServiceClient')
    def test_upload_fallback_no_ticker_pattern(self, mock_bsc, tmp_path):
        """File tanpa pola 'ticker=' harus menggunakan fallback (nama file saja)."""
        mock_bsc.from_connection_string.return_value = MagicMock()
        loader = AzureBlobLoader(connection_string="fake-conn-string", container_name="stock-data")

        test_file = tmp_path / "random_file.csv"
        test_file.write_bytes(b"some csv content")

        result = loader.upload_file(str(test_file), blob_name_prefix="misc")
        assert result is not None
        assert "random_file.csv" in result

    @patch('loaders.azure_blob_loader.BlobServiceClient')
    def test_upload_exception_returns_none(self, mock_bsc, tmp_path):
        """Jika upload ke Azure gagal, harus mengembalikan None (bukan crash)."""
        mock_client = MagicMock()
        mock_blob_client = MagicMock()
        mock_blob_client.upload_blob.side_effect = Exception("Azure connection refused")
        mock_client.get_blob_client.return_value = mock_blob_client
        mock_bsc.from_connection_string.return_value = mock_client

        loader = AzureBlobLoader(connection_string="fake-conn-string", container_name="stock-data")

        test_file = tmp_path / "ticker=BBCA.JK" / "data.parquet"
        test_file.parent.mkdir(parents=True)
        test_file.write_bytes(b"fake data")

        result = loader.upload_file(str(test_file), blob_name_prefix="silver/stocks")
        assert result is None
