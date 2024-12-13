from enum import Enum ,auto


START_DATE = "2010-01-01"
END_DATE = "2023-12-31"

DATA_FOLDERS = [
    "../data/processed/dow_jones",
    "../data/processed/ftse_100",
    "../data/processed/nikkei_225",
    "../data/processed/s&p_500",
    "../data/processed/EGX_30"
]

class ColumnNames(Enum):
    Date    = auto()
    Open    = auto()
    Close   = auto()
    Low     = auto()
    High    = auto()
    Volume  = auto()


class Indices(Enum):
    sp_500      = auto()
    nikkei_225  = auto()
    ftse_100    = auto()
    dow_Jones   = auto()
    EGX_30      = auto()