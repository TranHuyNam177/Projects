import pandas as pd

# import module code sẵn
import datetime as dt
from request import connect_DWH_AppData
from datawarehouse import BDATE
from warning import warning_RMD_EOD

class Query(object):

    def __init__(self):
        self.__runDate = None
        self.__fromDateROD0040 = None
        self.__toDateROD0040 = None
        self.__roomCode = None
        self.__result = None

    @property
    def runDate(self):
        return self.__runDate

    @runDate.setter
    def runDate(self, value: dt.datetime):
        self.__runDate = value.strftime("%Y-%m-%d")

    @property
    def fromDateROD0040(self):
        return self.__fromDateROD0040

    @fromDateROD0040.setter
    def fromDateROD0040(self, value: dt.datetime):
        self.__fromDateROD0040 = value.strftime("%Y-%m-%d")

    @property
    def toDateROD0040(self):
        return self.__toDateROD0040

    @toDateROD0040.setter
    def toDateROD0040(self, value: dt.datetime):
        self.__toDateROD0040 = value.strftime("%Y-%m-%d")


class SheetSummaryInfo(Query):

    def __init__(self):
        super().__init__()

    @property
    def result(self):

        statement = f"""
        EXEC [spRMD_MonthlyDeal_Report_SheetSummary] '{self.runDate}', '{self.fromDateROD0040}', '{self.toDateROD0040}'
        """

        self.__result = pd.read_sql(
            sql=statement,
            con=connect_DWH_AppData
        )
        return self.__result


class SheetSummaryInfoUpdate(Query):

    def __init__(self):
        super().__init__()

    @property
    def result(self):

        statement = f"""
        EXEC [spRMD_MonthlyDeal_Report_SheetSummary_Update] '{self.runDate}'
        """

        self.__result = pd.read_sql(
            sql=statement,
            con=connect_DWH_AppData
        )
        return self.__result


class SheetMonthlyDeal(Query):

    def __init__(self):
        super().__init__()

    @property
    def result(self):

        statement = f"""
        EXEC [spRMD_MonthlyDeal_Report_SheetMonthlyDeal] '{self.runDate}', '{self.fromDateROD0040}', '{self.toDateROD0040}'
        """

        self.__result = pd.read_sql(
            sql=statement,
            con=connect_DWH_AppData
        )
        return self.__result

class SheetMonthlyDealUpdate(Query):

    def __init__(self):
        super().__init__()

    @property
    def result(self):

        statement = f"""
        EXEC [spRMD_MonthlyDeal_Report_SheetMonthlyDeal_Update] '{self.runDate}'
        """

        self.__result = pd.read_sql(
            sql=statement,
            con=connect_DWH_AppData
        )
        return self.__result


class Liquidity3M(Query):

    def __init__(self):
        super().__init__()

    @property
    def result(self) -> pd.DataFrame:
        # lùi 1 ngày làm việc
        previousWorkDateString = BDATE(self.runDate, -1)
        previousWorkDate = dt.datetime.strptime(previousWorkDateString, '%Y-%m-%d')
        # chạy module warning_RMD_EOD
        converRunDateToDateTime = dt.datetime.strptime(self.runDate, '%Y-%m-%d')
        if converRunDateToDateTime.hour >= 16:
            run_time = converRunDateToDateTime
        else:
            run_time = previousWorkDate
        self.__result = warning_RMD_EOD.run(run_time=run_time)[['Stock', '3M Avg. Volume']].reset_index(drop=True)
        return self.__result


