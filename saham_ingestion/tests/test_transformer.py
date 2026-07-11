import pytest
import os
import pandas as pd
from transformers.parquet_transformer import ParquetTransformer


class TestParquetTransformer:
    """Test suite untuk ParquetTransformer (termasuk integrasi Pandera)."""

    def test_validate_cleans_zero_volume(self, sample_dirty_dataframe, tmp_path):
        """Baris dengan volume=0 harus dihapus."""
        transformer = ParquetTransformer(output_dir=str(tmp_path))
        paths = transformer.validate_and_transform('BBCA.JK', sample_dirty_dataframe)
        assert len(paths) > 0

        # Baca kembali parquet dan periksa bahwa volume=0 sudah dihapus
        result_df = pd.read_parquet(paths[0])
        assert (result_df['volume'] > 0).all()

    def test_validate_cleans_high_less_than_low(self, sample_dirty_dataframe, tmp_path):
        """Baris dengan high < low harus dihapus."""
        transformer = ParquetTransformer(output_dir=str(tmp_path))
        paths = transformer.validate_and_transform('BBCA.JK', sample_dirty_dataframe)
        assert len(paths) > 0

        result_df = pd.read_parquet(paths[0])
        assert (result_df['high'] >= result_df['low']).all()

    def test_validate_empty_df(self, tmp_path):
        """DataFrame None harus mengembalikan list kosong."""
        transformer = ParquetTransformer(output_dir=str(tmp_path))
        result = transformer.validate_and_transform('BBCA.JK', None)
        assert result == []

    def test_validate_empty_dataframe(self, tmp_path):
        """DataFrame kosong harus mengembalikan list kosong."""
        transformer = ParquetTransformer(output_dir=str(tmp_path))
        result = transformer.validate_and_transform('BBCA.JK', pd.DataFrame())
        assert result == []

    def test_output_is_parquet(self, sample_raw_dataframe, tmp_path):
        """Output harus berupa file .parquet yang valid."""
        transformer = ParquetTransformer(output_dir=str(tmp_path))
        paths = transformer.validate_and_transform('BBCA.JK', sample_raw_dataframe)
        for p in paths:
            assert p.endswith('.parquet')
            assert os.path.exists(p)
            # Pastikan bisa dibaca ulang
            df = pd.read_parquet(p)
            assert not df.empty

    def test_partition_by_month(self, sample_raw_dataframe, tmp_path):
        """Semua data di Juli 2024 harus menghasilkan tepat 1 file partisi."""
        transformer = ParquetTransformer(output_dir=str(tmp_path))
        paths = transformer.validate_and_transform('BBCA.JK', sample_raw_dataframe)
        assert len(paths) == 1
        assert 'year=2024' in paths[0]
        assert 'month=07' in paths[0]

    def test_partition_columns_not_in_output(self, sample_raw_dataframe, tmp_path):
        """Kolom 'year' dan 'month' tidak boleh ada di file Parquet (sudah di-drop)."""
        transformer = ParquetTransformer(output_dir=str(tmp_path))
        paths = transformer.validate_and_transform('BBCA.JK', sample_raw_dataframe)
        result_df = pd.read_parquet(paths[0])
        assert 'year' not in result_df.columns
        assert 'month' not in result_df.columns

    def test_dropped_count_logged(self, sample_dirty_dataframe, tmp_path):
        """Harus menghapus baris anomali dari dirty DataFrame."""
        transformer = ParquetTransformer(output_dir=str(tmp_path))
        paths = transformer.validate_and_transform('BBCA.JK', sample_dirty_dataframe)
        assert len(paths) > 0

        result_df = pd.read_parquet(paths[0])
        # Dari 5 baris awal, minimal 2 harus dihapus (volume=0 dan high<low)
        assert len(result_df) <= 3
