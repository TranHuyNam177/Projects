import numpy as np
import pandas as pd

from warning.MonitorStock.data import DataForReport, Liquidity3M, WarningMonitorStock
from warning.MonitorStock.utils import ReadDataFromExcel, Check


class Report:

    def __init__(self):
        self.__dataFromWarningRMDEOD = Liquidity3M().result
        self.__dfFixedMP = ReadDataFromExcel().fixedMP

    @property
    def runMailWarningReport(self) -> pd.DataFrame:

        dataFromDB = WarningMonitorStock().result
        finalTable = pd.merge(
            left=dataFromDB,
            right=self.__dataFromWarningRMDEOD[['Stock', '3M Avg. Volume']],
            how='left',
            on='Stock'
        )
        fixList = self.__dfFixedMP['Fix list'].to_list()
        finalTable['Note'] = finalTable['Stock'].apply(lambda x: 'Fixed' if x in fixList else '')
        finalTable.loc[:, finalTable.columns != 'Stock'] = finalTable.fillna(0)

        return finalTable

    @property
    def runReviewMPReport(self) -> pd.DataFrame:

        dataFromDB = DataForReport().result

        table = dataFromDB.merge(
            self.__dataFromWarningRMDEOD[['Stock', '3M Avg. Volume', 'Last day Volume', '1M Illiquidity Days']],
            how='left',
            on='Stock'
        ).merge(
            ReadDataFromExcel().Intranet,
            how='left',
            on='Stock'
        ).merge(
            ReadDataFromExcel().HighRisk,
            how='left',
            on='Stock'
        )

        # fillna
        table['3M Avg. Volume'].fillna(0, inplace=True)
        table['Last day Volume'].fillna(0, inplace=True)
        table['1M Illiquidity Days'].fillna(0, inplace=True)
        table['IntranetGeneralRoom'].fillna(0, inplace=True)
        table['HighRiskApprovedQuantity'].fillna(0, inplace=True)

        # dùng div vì có trường hợp chia với 0 ra inf
        table['Approved/liquidity'] = table['IntranetGeneralRoom'].div(table['Last day Volume']).replace(np.inf, 0)
        table['TotalPotentialApproved'] = (table['IntranetGeneralRoom'] + table['HighRiskApprovedQuantity']) * table['MaxPrice'] * table['Max_Rmr_Rdp']
        table['TotalPotentialUsedRoom'] = (table['GeneralRoom_UsedRoom'] + table['SpecialRoom_UsedRoom']) * table['MaxPrice'] * table['Max_Rmr_Rdp']

        fixList = self.__dfFixedMP['Fix list'].to_list()
        table['FixedMP'] = table['Stock'].apply(lambda x: 'Fixed' if x in fixList else '')

        dfBlackList = ReadDataFromExcel().blackList
        blackList = dfBlackList['Stock'].to_list()
        table['Blacklist'] = table['Stock'].apply(lambda x: 'Y' if x in blackList else '')

        table['Note 1'] = table[['MarketCap', 'Last day Volume']].apply(
            lambda x: Check().note1(x[0], x[1]),
            axis=1
        )

        table['Note 2'] = table[['CurrentBP', 'BP_0.05', 'BP_130%RP']].apply(
            lambda x: Check().note2(x[0], x[1], x[2]),
            axis=1
        )

        # chia 2 cột cho 1,000,000
        table['MarketCap'] = table['MarketCap'] / 1e6
        table['NetProfit'] = table['NetProfit'] / 1e6

        return table
