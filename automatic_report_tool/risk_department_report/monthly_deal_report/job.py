import pandas as pd
import datetime as dt
import os
import numpy as np

from os.path import join
from dateutil.relativedelta import relativedelta

from datawarehouse import BATCHINSERT, DELETE
from request import connect_DWH_CoSo


class ImportFile:

    def __init__(self):
        self.__path = r"\\192.168.10.101\phs-storge-2018\RiskManagementDept\RMD_Data\Luu tru van ban\Monthly Report\1. Monthly deal report"
        self.__runDate = None

    @property
    def runDate(self):
        return self.__runDate

    @runDate.setter
    def runDate(self, value):
        self.__runDate = value

    @staticmethod
    def findDate(months: int, inputDate: dt.datetime):

        if months == 1:
            fromDate = dt.datetime(inputDate.year, inputDate.month, 1)
        else:
            backDate = inputDate - relativedelta(months=months - 1)
            fromDate = dt.datetime(backDate.year, backDate.month, 1)
        return fromDate

    def readFile(self):

        DELETE(
            conn=connect_DWH_CoSo,
            table='CommitmentMonthlyDeal',
            where=''
        )

        filePath = join(self.__path, 'Summary commitment monthly deal.xlsx')
        fileData = pd.read_excel(
            filePath,
            usecols='A:B,D:E',
            sheet_name='Final',
            names=['RoomCode', 'Stock', 'MATV', 'Months'],
            dtype={'RoomCode': str}
        )
        fileData = fileData.loc[~fileData['RoomCode'].isna()].reset_index(drop=True)
        fileData['FromDate'] = fileData['Months'].apply(lambda x: self.findDate(x, self.__runDate))

        BATCHINSERT(
            df=fileData,
            conn=connect_DWH_CoSo,
            table='CommitmentMonthlyDeal'
        )


class ExcelWriter:

    def __init__(self, inputDate: dt.datetime):

        self.__runDate = inputDate

        self.__dataSheet1 = None
        self.__dataSheet2 = None
        self.__roomCodeSheet1 = None
        self.__roomCodeSheet2 = None
        self.__filePath = None

        self.__root = fr"C:\Users\{os.getlogin()}"
        if not os.path.isdir(join(self.__root, 'Shared Folder')):
            os.mkdir((join(self.__root, 'Shared Folder')))
        if not os.path.isdir(join(self.__root, 'Shared Folder', 'Monthly Deal')):
            os.mkdir((join(self.__root, 'Shared Folder', 'Monthly Deal')))

        self.__destinationDir = join(self.__root, 'Shared Folder', 'Monthly Deal')

        self.__yearString = str(self.__runDate.year)
        if self.__runDate.month > 9:
            mStr = f'{self.__runDate.month}'
        else:
            mStr = f'0{self.__runDate.month}'
        self.__monthString = f'T{mStr}'
        self.__dateString = inputDate.strftime('%d.%m.%Y')

        # create 'year' folder
        if not os.path.isdir(join(self.__destinationDir, self.__yearString)):
            os.mkdir((join(self.__destinationDir, self.__yearString)))
        # create 'month' folder
        if not os.path.isdir(join(self.__destinationDir, self.__yearString, self.__monthString)):
            os.mkdir((join(self.__destinationDir, self.__yearString, self.__monthString)))
        # create 'date' folder in date folder
        if not os.path.isdir(join(self.__destinationDir, self.__yearString, self.__monthString, self.__dateString)):
            os.mkdir((join(self.__destinationDir, self.__yearString, self.__monthString, self.__dateString)))

    @property
    def roomCodeSheet1(self):
        return self.__roomCodeSheet1

    @roomCodeSheet1.setter
    def roomCodeSheet1(self, value: str = 'all'):
        self.__roomCodeSheet1 = value.lower()

    @property
    def roomCodeSheet2(self):
        return self.__roomCodeSheet2

    @roomCodeSheet2.setter
    def roomCodeSheet2(self, value: str = 'all'):
        self.__roomCodeSheet2 = value.lower()

    @property
    def dataSheet1(self):
        return self.__dataSheet1

    @dataSheet1.setter
    def dataSheet1(self, value: pd.DataFrame):
        self.__dataSheet1 = value

    @property
    def dataSheet2(self):
        return self.__dataSheet2

    @dataSheet2.setter
    def dataSheet2(self, value: pd.DataFrame):
        self.__dataSheet2 = value

    @property
    def pathToAttachFile(self):
        return self.__filePath

    def writeExcel(self):
        dateString = self.__runDate.strftime("%d.%m.%Y")
        subTitleString = 'Date: ' + self.__runDate.strftime("%d/%m/%Y")
        timeString = f"{self.__runDate.hour}.{self.__runDate.minute}.{self.__runDate.second}"
        fileName = f"Monthly deal report {dateString} - {timeString}.xlsx"
        self.__filePath = join(
            self.__destinationDir,
            self.__yearString,
            self.__monthString,
            self.__dateString,
            fileName
        )

        writer = pd.ExcelWriter(
            self.__filePath,
            engine='xlsxwriter',
            engine_kwargs={
                'options': {'nan_inf_to_errors': True}
            }
        )

        # ================================ SHEET Summary Information ================================
        workbook = writer.book
        worksheet = workbook.add_worksheet('Summary Information')
        worksheet.freeze_panes('B2')
        worksheet.hide_gridlines(option=2)
        worksheet.autofilter('A1:AA1')

        # FORMAT
        titleFormat = workbook.add_format({
            'bold': True,
            'font_name': 'Times New Roman',
            'font_size': 28,
            'align': 'center',
            'valign': 'vcenter'
        })

        subTitleFormat = workbook.add_format({
            'bold': True,
            'italic': True,
            'font_name': 'Times New Roman',
            'font_size': 11,
            'align': 'center',
            'valign': 'vcenter',
            'num_format': 'dd/mm/yyyy',
            # 'bg_color': '#FFFF00'

        })

        headerFormat1 = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'font_name': 'Times New Roman',
            'font_size': 11,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#FFC000'
        })

        headerFormat2 = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'font_name': 'Times New Roman',
            'font_size': 11,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#FFC000'
        })

        headerFormat3 = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'font_name': 'Times New Roman',
            'font_size': 11,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#92D050'
        })

        headerFormat4 = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'font_name': 'Times New Roman',
            'font_size': 10,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#FFFF00'
        })

        headerFormat5 = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'font_name': 'Times New Roman',
            'font_size': 10,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#FFC000'
        })

        textFormat = workbook.add_format({
            'font_name': 'Times New Roman',
            'border': 1,
            'font_size': 10,
            'align': 'center',
            'valign': 'vcenter'
        })

        textLeftFormat = workbook.add_format({
            'font_name': 'Times New Roman',
            'border': 1,
            'font_size': 10,
            'align': 'left',
            'valign': 'vcenter'
        })

        mergeFormat = workbook.add_format({
            'text_wrap': True,
            'font_name': 'Times New Roman',
            'font_size': 10,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter'
        })

        numberFormat = workbook.add_format({
            'font_name': 'Times New Roman',
            'font_size': 10,
            'align': 'right',
            'valign': 'vcenter',
            'num_format': '_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)',
            'border': 1,
        })

        decimalFormat = workbook.add_format({
            'font_name': 'Times New Roman',
            'font_size': 10,
            'align': 'right',
            'valign': 'vcenter',
            'num_format': '_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_)',
            'border': 1
        })

        percentFormat = workbook.add_format({
            'font_name': 'Times New Roman',
            'font_size': 10,
            'align': 'right',
            'valign': 'vcenter',
            # 'num_format': '0%',
            'border': 1,
        })

        headers1 = [
            'Room code', 'Account', 'Stock', 'Sub Account', 'Location', 'Quantity', 'Closed price', 'Total Cash'
        ]
        headers2 = [
            'Market value of this stock', 'Total market value of all stocks',
            'Principal Outstanding', 'Interest Outstanding', 'Total Outstanding'
        ]
        headers3 = ['Trading value of account', 'Trading value of stock', 'Trading fee of account']

        # Check if room code input != all
        if self.roomCodeSheet1 != 'all':
            self.dataSheet1 = self.dataSheet1.loc[
                self.dataSheet1['RoomCode'] == self.roomCodeSheet1
                ].reset_index(drop=True)

        # Write to Excel
        worksheet.write_row('A1', headers1, headerFormat1)
        worksheet.write_row('I1', headers2, headerFormat2)
        worksheet.write_row('N1', headers3, headerFormat3)
        worksheet.set_column('A:A', 7)
        worksheet.set_column('B:B', 11)
        worksheet.set_column('C:C', 7)
        worksheet.set_column('D:F', 11)
        worksheet.set_column('G:G', 8)
        worksheet.set_column('H:P', 14)
        worksheet.set_row(0, 40)
        worksheet.write_column('A2', self.dataSheet1['RoomCode'], textFormat)
        worksheet.write_column('B2', self.dataSheet1['Account'], textFormat)
        worksheet.write_column('C2', self.dataSheet1['Stock'], textFormat)
        worksheet.write_column('D2', self.dataSheet1['SubAccount'], textFormat)
        worksheet.write_column('E2', self.dataSheet1['Location'], textFormat)
        worksheet.write_column('F2', self.dataSheet1['Quantity'], numberFormat)
        worksheet.write_column('G2', self.dataSheet1['ClosedPrice'], numberFormat)
        worksheet.write_column('H2', self.dataSheet1['TotalCash'], numberFormat)
        worksheet.write_column('I2', self.dataSheet1['MarketValueStock'], numberFormat)
        worksheet.write_column('J2', self.dataSheet1['TotalMarketValueAllStock'], numberFormat)
        worksheet.write_column('K2', self.dataSheet1['PrincipalOutstanding'], numberFormat)
        worksheet.write_column('L2', self.dataSheet1['InterestOutstanding'], numberFormat)
        worksheet.write_column('M2', self.dataSheet1['TotalOutstanding'], numberFormat)
        worksheet.write_column('N2', self.dataSheet1['TradingValueBySubAccount'], numberFormat)
        worksheet.write_column('O2', self.dataSheet1['TradingValueByStock'], numberFormat)
        worksheet.write_column('P2', self.dataSheet1['TradingFee'], numberFormat)
        worksheet.autofilter('A1:P1')

        # ================================ SHEET Monthly deal report ================================
        worksheet = workbook.add_worksheet('MONTHLY DEAL REPORT')
        worksheet.hide_gridlines(option=2)
        worksheet.freeze_panes('O6')

        headers = [
            'No', 'Room Code', 'Setup date', 'Approved Date (MM)', 'Stock', 'Sector', 'Location', 'Account',
            'Approved Quantity', 'Set up', 'MR\nRatio (%)', 'DP\nRatio (%)', 'Max Price', 'P.Outs Set Up',
            'Average Volume 03 months', 'Closed price', 'Breakeven Price Min', 'Breakeven Price Max',
            'Used quantity (Shares)', 'P.Outs net Cash (Unit: Mil VND)', 'Trading Value (Unit: Mil VND)',
            'Trading Value of Stock (Unit: Mil VND)', 'Quantity (shares)',
            'MATV (Monthly average trading value at least)'
        ]
        headers2 = ['Commitment Fix Type Others special', "RMD's Suggestion", "DGD's opinions"]
        # Check if room code input != all
        if self.roomCodeSheet2 != 'all':
            self.dataSheet2 = self.dataSheet2.loc[
                self.dataSheet2['RoomCode'] == self.roomCodeSheet2
            ].reset_index(drop=True)

        worksheet.set_column('A:A', 3)
        worksheet.set_column('B:B', 6)
        worksheet.set_column('C:D', 11)
        worksheet.set_column('E:E', 6)
        worksheet.set_column('F:G', 11)
        worksheet.set_column('H:H', 20)
        worksheet.set_column('I:J', 11)
        worksheet.set_column('K:L', 6)
        worksheet.set_column('M:M', 7)
        worksheet.set_column('N:N', 14)
        worksheet.set_column('O:O', 11)
        worksheet.set_column('P:R', 7)
        worksheet.set_column('S:X', 11)
        worksheet.set_column('Y:AA', 20)

        worksheet.set_default_row(24)
        worksheet.set_row(0, 20)
        worksheet.set_row(1, 20)
        worksheet.set_row(2, 18)
        worksheet.set_row(3, 11)
        worksheet.set_row(4, 52)

        worksheet.merge_range('A1:S2', 'MONTHLY DEAL REPORT', titleFormat)
        worksheet.merge_range('A3:S3', subTitleString, subTitleFormat)
        worksheet.write_row('A5', headers, headerFormat5)
        worksheet.write_row('Y5', headers2, headerFormat4)
        worksheet.write_column('A6', np.arange(self.dataSheet2.shape[0]) + 1, textFormat)
        worksheet.write_column('B6', self.dataSheet2['RoomCode'], textFormat)
        worksheet.write_column('C6', [''] * len(self.dataSheet2), textFormat)
        worksheet.write_column('D6', [''] * len(self.dataSheet2), textFormat)
        worksheet.write_column('E6', self.dataSheet2['Stock'], textFormat)
        worksheet.write_column('F6', self.dataSheet2['Sector'], textLeftFormat)
        worksheet.write_column('G6', self.dataSheet2['Location'], textLeftFormat)
        worksheet.write_column('H6', self.dataSheet2['AccountGroup'], mergeFormat)
        worksheet.write_column('T6', self.dataSheet2['P.OutsNetCash'], numberFormat)
        worksheet.write_column('U6', self.dataSheet2['TradingValueByAccount'], numberFormat)
        worksheet.write_column('X6', self.dataSheet2['MATV'], decimalFormat)
        emptyList = []
        for i in range(0, len(self.dataSheet2)):
            if i != 0 and self.dataSheet2['RoomCode'].iloc[i] == self.dataSheet2['RoomCode'].iloc[i - 1]:
                emptyList.append(int(i))
            elif i == 0:
                emptyList.append(int(i))
            else:
                worksheet.merge_range(
                    f'H{min(emptyList) + 6}:H{max(emptyList) + 6}',
                    self.dataSheet2['AccountGroup'].iloc[min(emptyList)],
                    mergeFormat
                )
                worksheet.merge_range(
                    f'T{min(emptyList) + 6}:T{max(emptyList) + 6}',
                    self.dataSheet2['P.OutsNetCash'].iloc[min(emptyList)],
                    numberFormat
                )
                worksheet.merge_range(
                    f'U{min(emptyList) + 6}:U{max(emptyList) + 6}',
                    self.dataSheet2['TradingValueByAccount'].iloc[min(emptyList)],
                    numberFormat
                )
                worksheet.merge_range(
                    f'X{min(emptyList) + 6}:X{max(emptyList) + 6}',
                    self.dataSheet2['MATV'].iloc[min(emptyList)],
                    decimalFormat
                )
                emptyList = [i]
        worksheet.merge_range(
            f'H{min(emptyList) + 6}:H{max(emptyList) + 6}',
            self.dataSheet2['AccountGroup'].iloc[min(emptyList)],
            mergeFormat
        )
        worksheet.merge_range(
            f'T{min(emptyList) + 6}:T{max(emptyList) + 6}',
            self.dataSheet2['P.OutsNetCash'].iloc[min(emptyList)],
            numberFormat
        )
        worksheet.merge_range(
            f'U{min(emptyList) + 6}:U{max(emptyList) + 6}',
            self.dataSheet2['TradingValueByAccount'].iloc[min(emptyList)],
            numberFormat
        )
        worksheet.merge_range(
            f'X{min(emptyList) + 6}:X{max(emptyList) + 6}',
            self.dataSheet2['MATV'].iloc[min(emptyList)],
            decimalFormat
        )
        worksheet.write_column('I6', [''] * len(self.dataSheet2), textFormat)
        worksheet.write_column('J6', self.dataSheet2['Setup'], numberFormat)
        worksheet.write_column('K6', self.dataSheet2['MR_Ratio'], percentFormat)
        worksheet.write_column('L6', self.dataSheet2['DP_Ratio'], percentFormat)
        worksheet.write_column('M6', self.dataSheet2['MaxPrice'], numberFormat)
        worksheet.write_column('N6', self.dataSheet2['P.OutsSetUp'], numberFormat)
        worksheet.write_column('O6', self.dataSheet2['3M Avg. Volume'], numberFormat)
        worksheet.write_column('P6', self.dataSheet2['ClosedPrice'], numberFormat)
        worksheet.write_column('Q6', self.dataSheet2['BreakevenPrice.Min'], numberFormat)
        worksheet.write_column('R6', self.dataSheet2['BreakevenPrice.Max'], numberFormat)
        worksheet.write_column('S6', self.dataSheet2['UsedQuantity'], numberFormat)
        worksheet.write_column('V6', self.dataSheet2['TradingValueByStock'], numberFormat)
        worksheet.write_column('W6', self.dataSheet2['Quantity'], numberFormat)
        worksheet.write_column('Y6', [''] * len(self.dataSheet2), textFormat)
        worksheet.write_column('Z6', [''] * len(self.dataSheet2), textFormat)
        worksheet.write_column('AA6', [''] * len(self.dataSheet2), textFormat)

        writer.close()

        return self.__filePath


class ExcelWriterUpdate:

    def __init__(self, inputDate: dt.datetime):

        self.__runDate = inputDate

        self.__dataSheet1 = None
        self.__dataSheet2 = None
        self.__filePath = None

        # self.__destinationDir = r"C:\Shared Folder\Risk Management\Report\BaoCaoThang"  # Server
        self.__destinationDir = r"C:\Users\namtran\Shared Folder\Risk Management\Report\BaoCaoThang"

        self.__yearString = str(self.__runDate.year)
        if self.__runDate.month > 9:
            mStr = f'{self.__runDate.month}'
        else:
            mStr = f'0{self.__runDate.month}'
        self.__monthString = f'T{mStr}'
        self.__dateString = inputDate.strftime('%d.%m.%Y')

        # create 'year' folder
        if not os.path.isdir(join(self.__destinationDir, self.__yearString)):
            os.mkdir((join(self.__destinationDir, self.__yearString)))
        # create 'month' folder
        if not os.path.isdir(join(self.__destinationDir, self.__yearString, self.__monthString)):
            os.mkdir((join(self.__destinationDir, self.__yearString, self.__monthString)))

    @property
    def dataSheet1(self):
        return self.__dataSheet1

    @dataSheet1.setter
    def dataSheet1(self, value: pd.DataFrame):
        self.__dataSheet1 = value

    @property
    def dataSheet2(self):
        return self.__dataSheet2

    @dataSheet2.setter
    def dataSheet2(self, value: pd.DataFrame):
        self.__dataSheet2 = value

    @property
    def pathToAttachFile(self):
        return self.__filePath

    def writeExcel(self):
        dateString = self.__runDate.strftime("%d.%m.%Y")
        subTitleString = 'Date: ' + self.__runDate.strftime("%d/%m/%Y")
        fileName = f"Monthly deal report {dateString}.xlsx"
        self.__filePath = join(
            self.__destinationDir,
            self.__yearString,
            self.__monthString,
            fileName
        )

        writer = pd.ExcelWriter(
            self.__filePath,
            engine='xlsxwriter',
            engine_kwargs={
                'options': {'nan_inf_to_errors': True}
            }
        )

        # ================================ SHEET Summary Information ================================
        workbook = writer.book
        worksheet = workbook.add_worksheet('Summary Information')
        worksheet.freeze_panes('B2')
        worksheet.hide_gridlines(option=2)
        worksheet.autofilter('A1:AA1')

        # FORMAT
        titleFormat = workbook.add_format({
            'bold': True,
            'font_name': 'Times New Roman',
            'font_size': 28,
            'align': 'center',
            'valign': 'vcenter'
        })

        subTitleFormat = workbook.add_format({
            'bold': True,
            'italic': True,
            'font_name': 'Times New Roman',
            'font_size': 11,
            'align': 'center',
            'valign': 'vcenter',
            'num_format': 'dd/mm/yyyy',
            # 'bg_color': '#FFFF00'

        })

        headerFormat1 = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'font_name': 'Times New Roman',
            'font_size': 11,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#FFC000'
        })

        headerFormat3 = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'font_name': 'Times New Roman',
            'font_size': 11,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#92D050'
        })

        headerFormat4 = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'font_name': 'Times New Roman',
            'font_size': 10,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#FFFF00'
        })

        headerFormat5 = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'font_name': 'Times New Roman',
            'font_size': 10,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#FFC000'
        })

        textFormat = workbook.add_format({
            'font_name': 'Times New Roman',
            'border': 1,
            'font_size': 10,
            'align': 'center',
            'valign': 'vcenter'
        })

        textLeftFormat = workbook.add_format({
            'font_name': 'Times New Roman',
            'border': 1,
            'font_size': 10,
            'align': 'left',
            'valign': 'vcenter'
        })

        mergeFormat = workbook.add_format({
            'text_wrap': True,
            'font_name': 'Times New Roman',
            'font_size': 10,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter'
        })

        numberFormat = workbook.add_format({
            'font_name': 'Times New Roman',
            'font_size': 10,
            'align': 'right',
            'valign': 'vcenter',
            'num_format': '_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)',
            'border': 1,
        })

        decimalFormat = workbook.add_format({
            'font_name': 'Times New Roman',
            'font_size': 10,
            'align': 'right',
            'valign': 'vcenter',
            'num_format': '_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_)',
            'border': 1
        })

        percentFormat = workbook.add_format({
            'font_name': 'Times New Roman',
            'font_size': 10,
            'align': 'right',
            'valign': 'vcenter',
            # 'num_format': '0%',
            'border': 1,
        })

        headers1 = [
            'Room code', 'Account', 'Stock', 'Sub Account', 'Location', 'Quantity', 'Closed price', 'Total Cash'
        ]
        headers2 = [
            'Market value of this stock', 'Total market value of all stocks',
            'Principal Outstanding', 'Interest Outstanding', 'Total Outstanding'
        ]
        headers3 = ['Trading value of account', 'Trading value of stock', 'Trading fee of account']

        # Write to Excel
        worksheet.write_row('A1', headers1, headerFormat1)
        worksheet.write_row('I1', headers2, headerFormat1)
        worksheet.write_row('N1', headers3, headerFormat3)
        worksheet.set_column('A:A', 7)
        worksheet.set_column('B:B', 11)
        worksheet.set_column('C:C', 7)
        worksheet.set_column('D:F', 11)
        worksheet.set_column('G:G', 8)
        worksheet.set_column('H:P', 14)
        worksheet.set_row(0, 40)
        worksheet.write_column('A2', self.dataSheet1['RoomCode'], textFormat)
        worksheet.write_column('B2', self.dataSheet1['Account'], textFormat)
        worksheet.write_column('C2', self.dataSheet1['Stock'], textFormat)
        worksheet.write_column('D2', self.dataSheet1['SubAccount'], textFormat)
        worksheet.write_column('E2', self.dataSheet1['Location'], textFormat)
        worksheet.write_column('F2', self.dataSheet1['Quantity'], numberFormat)
        worksheet.write_column('G2', self.dataSheet1['ClosedPrice'], numberFormat)
        worksheet.write_column('H2', self.dataSheet1['TotalCash'], numberFormat)
        worksheet.write_column('I2', self.dataSheet1['MarketValueStock'], numberFormat)
        worksheet.write_column('J2', self.dataSheet1['TotalMarketValueAllStock'], numberFormat)
        worksheet.write_column('K2', self.dataSheet1['PrincipalOutstanding'], numberFormat)
        worksheet.write_column('L2', self.dataSheet1['InterestOutstanding'], numberFormat)
        worksheet.write_column('M2', self.dataSheet1['TotalOutstanding'], numberFormat)
        worksheet.write_column('N2', self.dataSheet1['TradingValueBySubAccount'], numberFormat)
        worksheet.write_column('O2', self.dataSheet1['TradingValueByStock'], numberFormat)
        worksheet.write_column('P2', self.dataSheet1['TradingFee'], numberFormat)
        worksheet.autofilter('A1:P1')

        # ================================ SHEET Monthly deal report ================================
        worksheet = workbook.add_worksheet('MONTHLY DEAL REPORT')
        worksheet.hide_gridlines(option=2)
        worksheet.freeze_panes('O6')

        headers = [
            'No', 'Room Code', 'Setup date', 'Approved Date (MM)', 'Stock', 'Sector', 'Location', 'Account',
            'Approved Quantity', 'Set up', 'MR\nRatio (%)', 'DP\nRatio (%)', 'Max Price', 'P.Outs Set Up',
            'Average Volume 03 months', 'Closed price', 'Breakeven Price Min', 'Breakeven Price Max',
            'Used quantity (Shares)', 'P.Outs net Cash (Unit: Mil VND)', 'Trading Value (Unit: Mil VND)',
            'Trading Value of Stock (Unit: Mil VND)', 'Quantity (shares)',
            'MATV (Monthly average trading value at least)', 'Note'
        ]
        headers2 = ['Commitment Fix Type Others special', "RMD's Suggestion", "DGD's opinions"]

        worksheet.set_column('A:A', 3)
        worksheet.set_column('B:B', 6)
        worksheet.set_column('C:D', 11)
        worksheet.set_column('E:E', 6)
        worksheet.set_column('F:G', 11)
        worksheet.set_column('H:H', 20)
        worksheet.set_column('I:J', 11)
        worksheet.set_column('K:L', 6)
        worksheet.set_column('M:M', 7)
        worksheet.set_column('N:N', 14)
        worksheet.set_column('O:O', 11)
        worksheet.set_column('P:R', 7)
        worksheet.set_column('S:X', 11)
        worksheet.set_column('Y:Y', 8)
        worksheet.set_column('Z:AB', 20)

        worksheet.set_default_row(24)
        worksheet.set_row(0, 20)
        worksheet.set_row(1, 20)
        worksheet.set_row(2, 18)
        worksheet.set_row(3, 11)
        worksheet.set_row(4, 52)

        worksheet.merge_range('A1:S2', 'MONTHLY DEAL REPORT', titleFormat)
        worksheet.merge_range('A3:S3', subTitleString, subTitleFormat)
        worksheet.write_row('A5', headers, headerFormat5)
        worksheet.write_row('Z5', headers2, headerFormat4)
        worksheet.write('AC5', 'Check', headerFormat1)
        worksheet.write_column('A6', np.arange(self.dataSheet2.shape[0]) + 1, textFormat)
        worksheet.write_column('B6', self.dataSheet2['RoomCode'], textFormat)
        worksheet.write_column('C6', [''] * len(self.dataSheet2), textFormat)
        worksheet.write_column('D6', [''] * len(self.dataSheet2), textFormat)
        worksheet.write_column('E6', self.dataSheet2['Stock'], textFormat)
        worksheet.write_column('F6', self.dataSheet2['Sector'], textLeftFormat)
        worksheet.write_column('G6', self.dataSheet2['Location'], textLeftFormat)
        worksheet.write_column('H6', self.dataSheet2['AccountGroup'], mergeFormat)
        worksheet.write_column('T6', self.dataSheet2['P.OutsNetCash'], numberFormat)
        worksheet.write_column('U6', self.dataSheet2['TradingValueByAccount'], numberFormat)
        worksheet.write_column('X6', self.dataSheet2['MATV'], decimalFormat)
        worksheet.write_column('Y6', self.dataSheet2['Note'], decimalFormat)
        emptyList = []
        for i in range(0, len(self.dataSheet2)):
            if i != 0 and self.dataSheet2['RoomCode'].iloc[i] == self.dataSheet2['RoomCode'].iloc[i - 1]:
                emptyList.append(int(i))
            elif i == 0:
                emptyList.append(int(i))
            else:
                worksheet.merge_range(
                    f'H{min(emptyList) + 6}:H{max(emptyList) + 6}',
                    self.dataSheet2['AccountGroup'].iloc[min(emptyList)],
                    mergeFormat
                )
                if (
                    not self.dataSheet2['Months'].iloc[i] == self.dataSheet2['Months'].iloc[i - 1] and
                    len(emptyList) > 3
                ):
                    position = max(emptyList) + 5
                else:
                    position = max(emptyList) + 6

                worksheet.merge_range(
                    f'T{min(emptyList) + 6}:T{position}',
                    self.dataSheet2['P.OutsNetCash'].iloc[min(emptyList)],
                    numberFormat
                )
                worksheet.merge_range(
                    f'U{min(emptyList) + 6}:U{position}',
                    self.dataSheet2['TradingValueByAccount'].iloc[min(emptyList)],
                    numberFormat
                )
                worksheet.merge_range(
                    f'X{min(emptyList) + 6}:X{position}',
                    self.dataSheet2['MATV'].iloc[min(emptyList)],
                    decimalFormat
                )
                # worksheet.merge_range(
                #     f'Y{min(emptyList) + 6}:Y{max(emptyList) + 6}',
                #     self.dataSheet2['Note'].iloc[min(emptyList)],
                #     decimalFormat
                # )
                emptyList = [i]
        worksheet.merge_range(
            f'H{min(emptyList) + 6}:H{max(emptyList) + 6}',
            self.dataSheet2['AccountGroup'].iloc[min(emptyList)],
            mergeFormat
        )
        worksheet.merge_range(
            f'T{min(emptyList) + 6}:T{max(emptyList) + 6}',
            self.dataSheet2['P.OutsNetCash'].iloc[min(emptyList)],
            numberFormat
        )
        worksheet.merge_range(
            f'U{min(emptyList) + 6}:U{max(emptyList) + 6}',
            self.dataSheet2['TradingValueByAccount'].iloc[min(emptyList)],
            numberFormat
        )
        worksheet.merge_range(
            f'X{min(emptyList) + 6}:X{max(emptyList) + 6}',
            self.dataSheet2['MATV'].iloc[min(emptyList)],
            decimalFormat
        )
        # worksheet.merge_range(
        #     f'Y{min(emptyList) + 6}:Y{max(emptyList) + 6}',
        #     self.dataSheet2['Note'].iloc[min(emptyList)],
        #     decimalFormat
        # )
        worksheet.write_column('I6', [''] * len(self.dataSheet2), textFormat)
        worksheet.write_column('J6', self.dataSheet2['Setup'], numberFormat)
        worksheet.write_column('K6', self.dataSheet2['MR_Ratio'], percentFormat)
        worksheet.write_column('L6', self.dataSheet2['DP_Ratio'], percentFormat)
        worksheet.write_column('M6', self.dataSheet2['MaxPrice'], numberFormat)
        worksheet.write_column('N6', self.dataSheet2['P.OutsSetUp'], numberFormat)
        worksheet.write_column('O6', self.dataSheet2['3M Avg. Volume'], numberFormat)
        worksheet.write_column('P6', self.dataSheet2['ClosedPrice'], numberFormat)
        worksheet.write_column('Q6', self.dataSheet2['BreakevenPrice.Min'], numberFormat)
        worksheet.write_column('R6', self.dataSheet2['BreakevenPrice.Max'], numberFormat)
        worksheet.write_column('S6', self.dataSheet2['UsedQuantity'], numberFormat)
        worksheet.write_column('V6', self.dataSheet2['TradingValueByStock'], numberFormat)
        worksheet.write_column('W6', self.dataSheet2['Quantity'], numberFormat)
        worksheet.write_column('Z6', [''] * len(self.dataSheet2), textFormat)
        worksheet.write_column('AA6', [''] * len(self.dataSheet2), textFormat)
        worksheet.write_column('AB6', [''] * len(self.dataSheet2), textFormat)
        worksheet.write_column('AC6', self.dataSheet2['Check'], textFormat)
        writer.close()

        return self.__filePath
