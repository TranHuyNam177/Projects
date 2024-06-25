import os
import numpy as np
from os.path import join

from automation.risk_management.MonitorStockReport.data import *
from automation.risk_management.MonitorStockReport.utils import ReadDataFromExcel, Check

class Report:

    def __init__(self, runDate: dt.datetime):
        self.__dataFromWarningRMDEOD = Liquidity3M().result
        self.__dfFixedMP = ReadDataFromExcel().fixedMP
        self.__runDate = runDate

    @property
    def runAllStockPortfolioReport(self) -> pd.DataFrame:

        dataFromDB = DataAllStocksPortfolio(self.__runDate).result

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

        table = table.loc[table['Blacklist'] != 'Y'].reset_index(drop=True)

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

    @property
    def runAllStockPriceBoard(self) -> pd.DataFrame:
        dataFromDB = DataAllStockPriceBoard(self.__runDate).result

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
        table['TotalPotentialApproved'] = (table['IntranetGeneralRoom'] + table['HighRiskApprovedQuantity']) * table[
            'MaxPrice'] * table['Max_Rmr_Rdp']
        table['TotalPotentialUsedRoom'] = (table['GeneralRoom_UsedRoom'] + table['SpecialRoom_UsedRoom']) * table[
            'MaxPrice'] * table['Max_Rmr_Rdp']

        fixList = self.__dfFixedMP['Fix list'].to_list()
        table['FixedMP'] = table['Stock'].apply(lambda x: 'Fixed' if x in fixList else '')

        dfBlackList = ReadDataFromExcel().blackList
        blackList = dfBlackList['Stock'].to_list()
        table['Blacklist'] = table['Stock'].apply(lambda x: 'Y' if x in blackList else '')

        table = table.loc[table['Blacklist'] != 'Y'].reset_index(drop=True)

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


class ExcelWriterMonitorReport:

    def __init__(self, runDate: dt.datetime):
        self.__runDate = runDate
        self.__data = None
        self.__destinationDir = fr"C:\Users\namtran\Shared Folder\Risk Management\Report\BaoCaoThang"
        # self.__destinationDir = fr"C:\Shared Folder\Risk Management\Report\BaoCaoThang"  # server

        self.__yearString = str(self.__runDate.year)
        if self.__runDate.month > 9:
            mStr = f'{self.__runDate.month}'
        else:
            mStr = f'0{self.__runDate.month}'
        self.__monthString = f'T{mStr}'

        # create 'year' folder
        if not os.path.isdir(join(self.__destinationDir, self.__yearString)):
            os.mkdir((join(self.__destinationDir, self.__yearString)))
        # create 'month' folder
        if not os.path.isdir(join(self.__destinationDir, self.__yearString, self.__monthString)):
            os.mkdir((join(self.__destinationDir, self.__yearString, self.__monthString)))

        self.__filePath = None
        report = Report(runDate=runDate)
        self.__dataSheetAllStocksPortfolio = report.runAllStockPortfolioReport
        self.__dataSheetAllStockPriceBoard = report.runAllStockPriceBoard

    def __writeToExcel(
            self,
            excelWriter: pd.ExcelWriter
    ):
        workbook = excelWriter.book

        dateString = self.__runDate.strftime("%d/%m/%Y")

        # FORMAT
        abbreviationFormat = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'border': 1,
            'font_name': 'Times New Roman',
            'font_size': 8,
            'align': 'center',
            'valign': 'vcenter'
        })

        abbreviationRedFormat = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'border': 1,
            'font_name': 'Times New Roman',
            'font_size': 8,
            'align': 'center',
            'valign': 'vcenter',
            'font_color': '#FF0000'
        })

        headerFormat = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'border': 1,
            'font_name': 'Times New Roman',
            'font_size': 10,
            'align': 'center',
            'valign': 'vcenter'
        })

        reportFormat = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'border': 1,
            'font_name': 'Times New Roman',
            'font_size': 12,
            'align': 'center',
            'valign': 'vcenter'
        })

        headerNoBoldFormat = workbook.add_format({
            'text_wrap': True,
            'border': 1,
            'italic':True,
            'font_name': 'Times New Roman',
            'font_size': 10,
            'align': 'center',
            'valign': 'vcenter'
        })

        headerRedFormat = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'border': 1,
            'font_name': 'Times New Roman',
            'font_size': 10,
            'align': 'center',
            'valign': 'vcenter',
            'font_color': '#FF0000'
        })

        headerOrangeBgFormat = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'border': 1,
            'font_name': 'Times New Roman',
            'font_size': 10,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#FFC000'
        })

        headerBlueBgFormat = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'border': 1,
            'font_name': 'Times New Roman',
            'font_size': 10,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#5B9BD5'
        })

        textCenterFormat = workbook.add_format({
            'font_name': 'Times New Roman',
            'font_size': 10,
            'align': 'center',
            'valign': 'vcenter',
            'border': 1,
        })

        textLeftFormat = workbook.add_format({
            'font_name': 'Times New Roman',
            'font_size': 10,
            'align': 'left',
            'valign': 'vcenter',
            'border': 1,
        })

        priceFormat = workbook.add_format({
            'font_name': 'Times New Roman',
            'font_size': 10,
            'align': 'right',
            'valign': 'vcenter',
            'num_format': '_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)',
            'border': 1,
        })

        priceDecimalFormat = workbook.add_format({
            'font_name': 'Times New Roman',
            'font_size': 10,
            'align': 'right',
            'valign': 'vcenter',
            'num_format': '_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_)',
            'border': 1,
        })

        ratioFormat = workbook.add_format({
            'font_name': 'Times New Roman',
            'font_size': 10,
            'border': 1,
            'align': 'right',
            'valign': 'vcenter',
            'num_format': '0%'

        })

        mappingDataSheet = {
            'All Stocks Portfolio': self.__dataSheetAllStocksPortfolio,
            'All Stocks PriceBoard': self.__dataSheetAllStockPriceBoard
        }

        for key, value in mappingDataSheet.items():
            worksheet = workbook.add_worksheet(key)
            worksheet.hide_gridlines(option=2)
            worksheet.freeze_panes('F5')

            # SET SIZE OF COLUMN
            worksheet.set_default_row(26)
            worksheet.set_row(1, 33)
            worksheet.set_row(2, 33)
            worksheet.set_column('A:A', 5)
            worksheet.set_column('B:C', 11)
            worksheet.set_column('D:D', 9)
            worksheet.set_column('E:E', 11)
            worksheet.set_column('F:I', 7)
            worksheet.set_column('J:L', 9)
            worksheet.set_column('M:M', 10)
            worksheet.set_column('N:Q', 9)
            worksheet.set_column('R:U', 12)
            worksheet.set_column('V:V', 11)
            worksheet.set_column('W:W', 8)
            worksheet.set_column('X:X', 12)
            worksheet.set_column('Y:Y', 13)
            worksheet.set_column('Z:AA', 9)
            worksheet.set_column('AB:AE', 6)
            worksheet.set_column('AF:AK', 10)
            worksheet.set_column('AL:AM', 8)
            worksheet.set_column('AN:AP', 14)
            worksheet.set_column('AQ:AR', 8)
            worksheet.set_column('AS:AS', 12)
            worksheet.set_column('AT:AT', 24)

            # WRITE HEADER
            worksheet.merge_range(
                'A2:E3',
                "",
                abbreviationFormat
            )
            worksheet.write_rich_string(
                'A2',
                abbreviationRedFormat,
                "Abbreviation:",
                abbreviationFormat,
                "\nSR: special room; GR: general room; MP: max price; RP:Reference price; BP:Breakeven price; MC:Market Cap; AV:Average Volume",
                abbreviationFormat
            )
            worksheet.merge_range(
                'F2:AP2',
                '',
                headerFormat
            )
            worksheet.write_rich_string(
                'F2',
                reportFormat,
                'Data for Risk Management Committee Meeting',
                headerNoBoldFormat,
                f'\nDate: {dateString}',
                headerNoBoldFormat
            )
            worksheet.merge_range('F3:J3', 'PRICE', headerFormat)
            worksheet.merge_range('K3:M3', 'BREAKEVEN PRICE', headerFormat)
            worksheet.merge_range('N3:O3', 'Based on P_value: 0.05', headerOrangeBgFormat)
            worksheet.merge_range('P3:Q3', 'Based on P_value: 0.02', headerOrangeBgFormat)
            worksheet.merge_range('R3:W3', 'MARKET INFORMATION', headerFormat)
            worksheet.merge_range('X3:AA3', 'LIQUIDITY', headerFormat)
            worksheet.merge_range('AB3:AC3', 'RATIO', headerFormat)
            worksheet.merge_range('AD3:AE3', "RMD's suggestion", headerRedFormat)
            worksheet.merge_range('AF3:AH3', 'GENERAL ROOM', headerFormat)
            worksheet.merge_range('AI3:AK3', 'SPECIAL ROOM', headerFormat)
            worksheet.merge_range('AL3:AM3', "RMD's suggestion", headerRedFormat)
            worksheet.merge_range('AN3:AP3', 'POTENTIAL OUTSTANDING', headerFormat)

            # WRITE SUB HEADER
            worksheet.write('A4', 'No', headerOrangeBgFormat)
            worksheet.write('B4', 'Group', headerOrangeBgFormat)
            worksheet.write('C4', 'Sector', headerOrangeBgFormat)
            worksheet.write('D4', 'Stock', headerOrangeBgFormat)
            worksheet.write_row(
                'E4',
                ['Name', 'Market price', 'RP', '130% RP', 'Max price'],
                headerOrangeBgFormat
            )
            worksheet.write('J4', "RMD's suggestion", headerRedFormat)
            worksheet.write_row('K4', ['Current BP', 'BP of 130% RP'], headerOrangeBgFormat)
            worksheet.write('M4', "RMD's suggestion", headerRedFormat)
            worksheet.write_row('N4', ['BP', 'Lowest BP'] * 2, headerOrangeBgFormat)
            worksheet.write_row(
                'R4',
                [
                    'Vol-listed',
                    'Floating shares',
                    '5% of listed-shares',
                    'Market Cap (million dong)',
                    'Net profit(million dong)',
                    'Ranking',
                    'Average Volume',
                    'Average Volume 3Ms',
                    'Illiquidity',
                    'Approved/liquidity (time)'
                ],
                headerBlueBgFormat
            )
            worksheet.write_row('AB4', ['MR ratio', 'DP ratio'], headerOrangeBgFormat)
            worksheet.write_row('AD4', ['MR ratio', 'DP ratio'], headerRedFormat)
            worksheet.write_row(
                'AF4',
                [
                    'Approved',
                    'Set up',
                    'Used room'

                ] * 2,
                headerOrangeBgFormat
            )
            worksheet.write_row('AL4', ['General room', 'Special room'], headerRedFormat)
            worksheet.write_row('AN4', ['Approved', 'Used room'], headerOrangeBgFormat)
            worksheet.write('AP4', "RMD's suggestion", headerRedFormat)
            worksheet.write_row('AQ4', ['Fixed MP', 'Note 1 (MC, AV)', 'Note 2 (CBP,Var)'], headerBlueBgFormat)

            # WRITE DATA
            table = value
            worksheet.write_column('A5', np.arange(table.shape[0])+1, textCenterFormat)
            worksheet.write_column('B5', [''] * len(table), textCenterFormat)
            worksheet.write_column('C5', table['Sector'], textLeftFormat)
            worksheet.write_column('D5', table['Stock'], textCenterFormat)
            worksheet.write_column('E5', table['Name'], textLeftFormat)
            worksheet.write_column('F5', table['MarketPrice'], priceFormat)
            worksheet.write_column('G5', table['ReferencePrice'], priceFormat)
            worksheet.write_column('H5', table['130%RP'], priceFormat)
            worksheet.write_column('I5', table['MaxPrice'], priceFormat)
            worksheet.write_column('J5', [''] * len(table), textCenterFormat)
            worksheet.write_column('K5', table['CurrentBP'], priceFormat)
            worksheet.write_column('L5', table['BP_130%RP'], priceFormat)
            worksheet.write_column('M5', [''] * len(table), textCenterFormat)
            worksheet.write_column('N5', table['BP_0.05'], priceFormat)
            worksheet.write_column('O5', table['LowestBP_0.05'], priceFormat)
            worksheet.write_column('P5', table['BP_0.02'], priceFormat)
            worksheet.write_column('Q5', table['LowestBP_0.02'], priceFormat)
            worksheet.write_column('R5', table['Vol-listed'], priceFormat)
            worksheet.write_column('S5', table['FloatingShares'], priceFormat)
            worksheet.write_column('T5', table['5%ListedShares'], priceFormat)
            worksheet.write_column('U5', table['MarketCap'], priceFormat)
            worksheet.write_column('V5', table['NetProfit'], priceFormat)
            worksheet.write_column('W5', table['Ranking'], textCenterFormat)
            worksheet.write_column('X5', table['Last day Volume'], priceFormat)
            worksheet.write_column('Y5', table['3M Avg. Volume'], priceFormat)
            worksheet.write_column('Z5', table['1M Illiquidity Days'], priceFormat)
            worksheet.write_column('AA5', table['Approved/liquidity'], priceDecimalFormat)
            worksheet.write_column('AB5', table['MR_LoanRatio'], ratioFormat)
            worksheet.write_column('AC5', table['DP_LoanRatio'], ratioFormat)
            worksheet.write_column('AD5', [''] * len(table), textCenterFormat)
            worksheet.write_column('AE5', [''] * len(table), textCenterFormat)
            worksheet.write_column('AF5', table['IntranetGeneralRoom'], priceFormat)
            worksheet.write_column('AG5', table['GeneralRoom_Setup'], priceFormat)
            worksheet.write_column('AH5', table['GeneralRoom_UsedRoom'], priceFormat)
            worksheet.write_column('AI5', table['HighRiskApprovedQuantity'], priceFormat)
            worksheet.write_column('AJ5', table['SpecialRoom_Setup'], priceFormat)
            worksheet.write_column('AK5', table['SpecialRoom_UsedRoom'], priceFormat)
            worksheet.write_column('AL5', [''] * len(table), textCenterFormat)
            worksheet.write_column('AM5', [''] * len(table), textCenterFormat)
            worksheet.write_column('AN5', table['TotalPotentialApproved'], priceFormat)
            worksheet.write_column('AO5', table['TotalPotentialUsedRoom'], priceFormat)
            worksheet.write_column('AP5', [''] * len(table), textCenterFormat)
            worksheet.write_column('AQ5', table['FixedMP'], textCenterFormat)
            worksheet.write_column('AR5', table['Note 1'], textCenterFormat)
            worksheet.write_column('AS5', table['Note 2'], textCenterFormat)

    def run(self):
        dateString = self.__runDate.strftime("%d.%m.%Y")
        fileName = f"Data_Review all stocks {dateString}.xlsx"
        self.__filePath = join(
            self.__destinationDir, self.__yearString, self.__monthString, fileName
        )
        excelWriter = pd.ExcelWriter(
            self.__filePath,
            engine='xlsxwriter',
            engine_kwargs={'options': {'nan_inf_to_errors': True}}
        )

        self.__writeToExcel(excelWriter=excelWriter)
        # CLOSING FILE
        excelWriter.close()
