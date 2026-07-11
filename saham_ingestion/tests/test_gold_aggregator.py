import pytest
import os
import pandas as pd
from transformers.gold_aggregator import GoldAggregator


class TestGoldAggregator:
    """Test suite untuk GoldAggregator (indikator teknikal)."""

    def test_aggregate_adds_sma_columns(self, sample_raw_dataframe, tmp_path):
        """Output harus memiliki kolom SMA-5 dan SMA-20."""
        agg = GoldAggregator(output_dir=str(tmp_path))
        paths = agg.aggregate_with_indicators('BBCA.JK', sample_raw_dataframe)
        assert len(paths) > 0

        result_df = pd.read_parquet(paths[0])
        assert 'sma_5' in result_df.columns
        assert 'sma_20' in result_df.columns

    def test_aggregate_adds_rsi_column(self, sample_raw_dataframe, tmp_path):
        """Output harus memiliki kolom RSI-14."""
        agg = GoldAggregator(output_dir=str(tmp_path))
        paths = agg.aggregate_with_indicators('BBCA.JK', sample_raw_dataframe)
        result_df = pd.read_parquet(paths[0])
        assert 'rsi_14' in result_df.columns

    def test_aggregate_adds_daily_return(self, sample_raw_dataframe, tmp_path):
        """Output harus memiliki kolom daily_return_pct."""
        agg = GoldAggregator(output_dir=str(tmp_path))
        paths = agg.aggregate_with_indicators('BBCA.JK', sample_raw_dataframe)
        result_df = pd.read_parquet(paths[0])
        assert 'daily_return_pct' in result_df.columns

    def test_aggregate_empty_df(self, tmp_path):
        """DataFrame kosong harus mengembalikan list kosong."""
        agg = GoldAggregator(output_dir=str(tmp_path))
        result = agg.aggregate_with_indicators('BBCA.JK', None)
        assert result == []

    def test_aggregate_output_is_parquet(self, sample_raw_dataframe, tmp_path):
        """Output harus berupa file .parquet yang valid."""
        agg = GoldAggregator(output_dir=str(tmp_path))
        paths = agg.aggregate_with_indicators('BBCA.JK', sample_raw_dataframe)
        for p in paths:
            assert p.endswith('.parquet')
            assert os.path.exists(p)

    def test_aggregate_gold_folder_structure(self, sample_raw_dataframe, tmp_path):
        """Output path harus mengikuti struktur gold/ticker/year/month."""
        agg = GoldAggregator(output_dir=str(tmp_path))
        paths = agg.aggregate_with_indicators('BBCA.JK', sample_raw_dataframe)
        assert len(paths) == 1
        assert 'gold' in paths[0]
        assert 'ticker=BBCA.JK' in paths[0]
        assert 'year=2024' in paths[0]
        assert 'month=07' in paths[0]

    def test_rsi_computation_bounds(self, tmp_path):
        """RSI harus berada dalam range 0-100 (untuk data dengan cukup observasi)."""
        # Buat DataFrame dengan 20 hari data untuk RSI-14
        dates = pd.date_range('2024-07-01', periods=20, freq='D')
        df = pd.DataFrame({
            'date': dates,
            'open': [100 + i for i in range(20)],
            'high': [105 + i for i in range(20)],
            'low': [95 + i for i in range(20)],
            'close': [102 + i for i in range(20)],
            'adj_close': [102 + i for i in range(20)],
            'volume': [1000000] * 20,
            'ticker': ['TEST.JK'] * 20,
        })
        df[['open', 'high', 'low', 'close', 'adj_close']] = df[['open', 'high', 'low', 'close', 'adj_close']].astype('float32')

        agg = GoldAggregator(output_dir=str(tmp_path))
        paths = agg.aggregate_with_indicators('TEST.JK', df)
        result_df = pd.read_parquet(paths[0])

        # RSI yang bukan NaN harus dalam range 0-100
        valid_rsi = result_df['rsi_14'].dropna()
        if len(valid_rsi) > 0:
            assert (valid_rsi >= 0).all()
            assert (valid_rsi <= 100).all()

    def test_indicator_dtypes_float32(self, sample_raw_dataframe, tmp_path):
        """Kolom indikator harus bertipe float32 (memory optimized)."""
        agg = GoldAggregator(output_dir=str(tmp_path))
        paths = agg.aggregate_with_indicators('BBCA.JK', sample_raw_dataframe)
        result_df = pd.read_parquet(paths[0])

        for col in ['sma_5', 'sma_20', 'daily_return_pct', 'rsi_14']:
            assert result_df[col].dtype == 'float32', f"{col} bukan float32"
