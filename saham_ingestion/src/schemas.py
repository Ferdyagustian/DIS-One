import pandera as pa
import pandas as pd
from pandera import Field
from pandera.typing import Series


class RawOHLCVSchema(pa.DataFrameModel):
    """
    Schema validasi untuk data mentah dari Yahoo Finance (boundary Bronze).
    Digunakan untuk memverifikasi integritas data sebelum diproses lebih lanjut.
    """
    date:      Series[pd.Timestamp]
    open:      Series[float] = Field(gt=0, nullable=False, description="Harga pembukaan")
    high:      Series[float] = Field(gt=0, nullable=False, description="Harga tertinggi")
    low:       Series[float] = Field(gt=0, nullable=False, description="Harga terendah")
    close:     Series[float] = Field(gt=0, nullable=False, description="Harga penutupan")
    adj_close: Series[float] = Field(gt=0, nullable=False, description="Harga penutupan adjusted")
    volume:    Series[int]   = Field(ge=0, nullable=False, description="Volume perdagangan")
    ticker:    Series[str]   = Field(str_matches=r"^[A-Z]{3,5}\.JK$", description="Kode saham IHSG")

    class Config:
        coerce = True
        strict = "filter"


class SilverOHLCVSchema(pa.DataFrameModel):
    """
    Schema validasi untuk data bersih di Silver layer.
    Lebih ketat dari RawOHLCVSchema — volume harus > 0, high >= low.
    """
    date:      Series[pd.Timestamp]
    open:      Series[float] = Field(gt=0, nullable=False)
    high:      Series[float] = Field(gt=0, nullable=False)
    low:       Series[float] = Field(gt=0, nullable=False)
    close:     Series[float] = Field(gt=0, nullable=False)
    adj_close: Series[float] = Field(gt=0, nullable=False)
    volume:    Series[int]   = Field(gt=0, nullable=False)  # Harus > 0 (bukan hari libur)
    ticker:    Series[str]   = Field(str_matches=r"^[A-Z]{3,5}\.JK$")

    class Config:
        coerce = True
