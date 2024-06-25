import pandas as pd
import datetime as dt
import warnings

from abc import ABC, abstractmethod
from request import connect_DWH_CoSo, connect_DWH_AppData, connect_DWH_ThiTruong

from datawarehouse import BDATE, SYNC, EXEC

# import module code sẵn
from warning import warning_RMD_EOD

warnings.filterwarnings('ignore')


class Query(ABC):

    @property
    @abstractmethod
    def result(self):
        pass

    @property
    @abstractmethod
    def runDate(self):
        pass


class Liquidity3M(Query):

    def __init__(self):
        self.__result = None
        self.__runDate = None

    @property
    def result(self) -> pd.DataFrame:
        # Ngày chạy
        self.__runDate = dt.datetime.now()
        dateString = self.__runDate.strftime('%Y-%m-%d')
        # lùi 1 ngày làm việc
        previousWorkDateString = BDATE(dateString, -1)
        previousWorkDate = dt.datetime.strptime(previousWorkDateString, '%Y-%m-%d')
        # chạy module warning_RMD_EOD
        if self.__runDate.hour >= 17:
            run_time = self.__runDate
        else:
            run_time = previousWorkDate
        self.__result = warning_RMD_EOD.run(run_time=run_time)
        return self.__result

    @property
    def runDate(self):
        return self.__runDate


class DataAllStocksPortfolio(Query):

    def __init__(self, runDate: dt.datetime):
        self.__result = None
        self.__runDate = runDate

    @property
    def result(self) -> pd.DataFrame:

        # Ngày chạy
        dateString = self.__runDate.strftime('%Y-%m-%d')

        statement = f"EXEC [spRMD_MonitorStock_AllStockPortfolio_Report] @t0Date='{dateString}'"

        self.__result = pd.read_sql(
            sql=statement,
            con=connect_DWH_AppData
        )
        return self.__result

    @property
    def runDate(self):
        return self.__runDate


class DataAllStockPriceBoard(Query):

    def __init__(self, runDate: dt.datetime):
        self.__result = None
        self.__runDate = runDate

    @property
    def result(self) -> pd.DataFrame:

        # Ngày chạy
        dateString = self.__runDate.strftime('%Y-%m-%d')

        statement = f"EXEC [spRMD_MonitorStock_AllStockPriceBoard_Report] @t0Date='{dateString}'"

        self.__result = pd.read_sql(
            sql=statement,
            con=connect_DWH_AppData
        )
        return self.__result

    @property
    def runDate(self):
        return self.__runDate

