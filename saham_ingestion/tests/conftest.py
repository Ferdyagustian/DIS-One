import pytest
import pandas as pd
from datetime import datetime


@pytest.fixture
def sample_raw_dataframe():
    """DataFrame contoh dari yfinance (sudah di-reset index, float32 optimized)."""
    return pd.DataFrame({
        'date': pd.to_datetime(['2024-07-01', '2024-07-02', '2024-07-03']),
        'open': pd.array([9500.0, 9600.0, 9700.0], dtype='float32'),
        'high': pd.array([9650.0, 9750.0, 9850.0], dtype='float32'),
        'low':  pd.array([9400.0, 9500.0, 9600.0], dtype='float32'),
        'close': pd.array([9600.0, 9700.0, 9800.0], dtype='float32'),
        'adj_close': pd.array([9600.0, 9700.0, 9800.0], dtype='float32'),
        'volume': pd.array([1000000, 1200000, 1100000], dtype='int64'),
        'ticker': ['BBCA.JK', 'BBCA.JK', 'BBCA.JK'],
    })


@pytest.fixture
def sample_dirty_dataframe():
    """DataFrame dengan baris anomali untuk menguji data quality checks."""
    data = pd.DataFrame({
        'date': pd.to_datetime(['2024-07-01', '2024-07-02', '2024-07-03', '2024-07-04', '2024-07-05']),
        'open': pd.array([9500.0, 9600.0, 9700.0, 9800.0, 9900.0], dtype='float32'),
        'high': pd.array([9650.0, 9750.0, 9850.0, 9900.0, 9000.0], dtype='float32'),   # idx 4: high < low
        'low':  pd.array([9400.0, 9500.0, 9600.0, 9700.0, 9950.0], dtype='float32'),    # idx 4: low > high
        'close': pd.array([9600.0, 9700.0, 9800.0, 9850.0, 9100.0], dtype='float32'),
        'adj_close': pd.array([9600.0, 9700.0, 9800.0, 9850.0, 9100.0], dtype='float32'),
        'volume': pd.array([1000000, 1200000, 1100000, 0, 500000], dtype='int64'),       # idx 3: volume = 0
        'ticker': ['BBCA.JK', 'BBCA.JK', 'BBCA.JK', 'BBCA.JK', 'BBCA.JK'],
    })
    return data
