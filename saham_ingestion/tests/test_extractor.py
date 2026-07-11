import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from extractors.yfinance_extractor import YFinanceExtractor


class TestYFinanceExtractor:
    """Test suite untuk YFinanceExtractor."""

    def test_ticker_suffix_auto_append(self):
        """Ticker tanpa suffix .JK harus otomatis ditambahkan."""
        ext = YFinanceExtractor(['BBCA', 'GOTO'])
        assert ext.tickers == ['BBCA.JK', 'GOTO.JK']

    def test_ticker_suffix_no_duplicate(self):
        """Ticker yang sudah memiliki suffix .JK tidak boleh diduplikasi."""
        ext = YFinanceExtractor(['BBCA.JK'])
        assert ext.tickers == ['BBCA.JK']

    def test_ticker_mixed_suffix(self):
        """Campuran ticker dengan dan tanpa suffix harus dihandle dengan benar."""
        ext = YFinanceExtractor(['BBCA', 'GOTO.JK', 'BMRI'])
        assert ext.tickers == ['BBCA.JK', 'GOTO.JK', 'BMRI.JK']

    @patch('yfinance.Ticker')
    def test_extract_success(self, mock_ticker_cls):
        """Ekstraksi data harus mengembalikan DataFrame dengan kolom yang benar."""
        mock_instance = MagicMock()
        mock_instance.history.return_value = pd.DataFrame({
            'Open': [9500.0],
            'High': [9650.0],
            'Low': [9400.0],
            'Close': [9600.0],
            'Volume': [1000000],
        }, index=pd.DatetimeIndex(['2024-07-01'], name='Date'))
        mock_ticker_cls.return_value = mock_instance

        ext = YFinanceExtractor(['BBCA'])
        result = ext.extract_historical_data(start_date='2024-07-01', end_date='2024-07-02')

        assert 'BBCA.JK' in result
        df = result['BBCA.JK']
        assert 'adj_close' in df.columns
        assert 'ticker' in df.columns
        assert 'date' in df.columns
        assert df['open'].dtype == 'float32'
        assert df['ticker'].iloc[0] == 'BBCA.JK'

    @patch('yfinance.Ticker')
    def test_extract_empty_data(self, mock_ticker_cls):
        """Ticker yang mengembalikan data kosong harus di-skip tanpa error."""
        mock_instance = MagicMock()
        mock_instance.history.return_value = pd.DataFrame()
        mock_ticker_cls.return_value = mock_instance

        ext = YFinanceExtractor(['INVALID'])
        result = ext.extract_historical_data(period='5d')
        assert len(result) == 0

    @patch('yfinance.Ticker')
    def test_extract_api_failure_raises(self, mock_ticker_cls):
        """Kegagalan API harus melempar exception (bukan swallow)."""
        mock_instance = MagicMock()
        mock_instance.history.side_effect = Exception("Connection timeout")
        mock_ticker_cls.return_value = mock_instance

        ext = YFinanceExtractor(['BBCA'])
        with pytest.raises(Exception, match="Connection timeout"):
            ext.extract_historical_data(period='5d')

    @patch('yfinance.Ticker')
    def test_extract_with_period_param(self, mock_ticker_cls):
        """Menggunakan parameter period (tanpa start/end) harus berfungsi."""
        mock_instance = MagicMock()
        mock_instance.history.return_value = pd.DataFrame({
            'Open': [100.0], 'High': [110.0], 'Low': [95.0],
            'Close': [105.0], 'Volume': [50000],
        }, index=pd.DatetimeIndex(['2024-07-01'], name='Date'))
        mock_ticker_cls.return_value = mock_instance

        ext = YFinanceExtractor(['TLKM'])
        result = ext.extract_historical_data(period='5d')

        assert 'TLKM.JK' in result
        mock_instance.history.assert_called_once_with(period='5d')
