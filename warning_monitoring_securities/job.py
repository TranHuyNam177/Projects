import numpy as np
import os

from os.path import join

from warning.MonitorStock.data import *
from warning.MonitorStock.utils import PATH


class ExcelWriterMailWarning:

    def __init__(self):
        self.__today = dt.datetime.now()
        self.__data = None
        # self.__destinationDir = fr"C:\Users\namtran\Shared Folder\Risk Management\Monitor Stock"
        self.__destinationDir = fr"C:\Shared Folder\Risk Management\Monitor Stock"  # server

        self.__yearString = str(self.__today.year)
        if self.__today.month > 9:
            mStr = f'{self.__today.month}'
        else:
            mStr = f'0{self.__today.month}'
        self.__monthString = f'T{mStr}'
        self.__dateString = dt.datetime.now().strftime('%d.%m.%Y')

        # create 'year' folder
        if not os.path.isdir(join(self.__destinationDir, self.__yearString)):
            os.mkdir((join(self.__destinationDir, self.__yearString)))
        # create 'month' folder
        if not os.path.isdir(join(self.__destinationDir, self.__yearString, self.__monthString)):
            os.mkdir((join(self.__destinationDir, self.__yearString, self.__monthString)))
        # create 'date' folder in date folder
        if not os.path.isdir(join(self.__destinationDir, self.__yearString, self.__monthString, self.__dateString)):
            os.mkdir((join(self.__destinationDir, self.__yearString, self.__monthString, self.__dateString)))

        self.__filePath = None

    @property
    def inputData(self):
        if self.__data is None:
            raise ValueError('No data to write!')
        return self.__data

    @inputData.setter
    def inputData(self, value: pd.DataFrame):
        self.__data = value

    @property
    def pathToAttachFile(self):
        return self.__filePath

    @property
    def pathToDataFile(self):
        return join(self.__destinationDir, self.__yearString, self.__monthString, self.__dateString)

    def run(self):

        dateString = self.__today.strftime("%d.%m.%Y")
        timeString = self.__today.strftime("%H.%M.%S")
        fileName = f"Monitor Stock {timeString} - {dateString}.xlsx"
        self.__filePath = join(self.__destinationDir, self.__yearString, self.__monthString, self.__dateString, fileName)
        result = self.inputData

        writer = pd.ExcelWriter(
            self.__filePath,
            engine='xlsxwriter',
            engine_kwargs={'options': {'nan_inf_to_errors': True}}
        )

        workbook = writer.book
        worksheet = workbook.add_worksheet('Monitor Stock')
        worksheet.hide_gridlines(option=2)

        # format
        headerFormat = workbook.add_format({
            'bold': True,
            'font_name': 'Times New Roman',
            'font_size': 12,
            'border': 1,
            'text_wrap': True,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#548235',
            'font_color': '#FFFFFF',
        })

        textFormat = workbook.add_format({
            'font_name': 'Times New Roman',
            'font_size': 12,
            'align': 'center',
            'valign': 'vcenter',
            'border': 1,
        })

        priceFormat = workbook.add_format({
            'font_name': 'Times New Roman',
            'font_size': 12,
            'align': 'right',
            'valign': 'vcenter',
            'num_format': '_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)',
            'border': 1,
        })

        liquidityFormat = workbook.add_format({
            'font_name': 'Times New Roman',
            'font_size': 12,
            'align': 'right',
            'valign': 'vcenter',
            'num_format': '_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_)',
            'border': 1,
        })

        priceSpecialFormatBgBold = workbook.add_format({
            'font_name': 'Times New Roman',
            'font_size': 12,
            'align': 'right',
            'valign': 'vcenter',
            'num_format': '_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)',
            'border': 1,
            'bold': True,
            'bg_color': '#FFFF00',
            'font_color': '#FF0000',
        })

        priceSpecialFormatNoBgBold = workbook.add_format({
            'font_name': 'Times New Roman',
            'font_size': 12,
            'align': 'right',
            'valign': 'vcenter',
            'num_format': '_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)',
            'border': 1,
            'bold': True,
            'font_color': '#FF0000',
        })

        priceSpecialFormatNoBgNoBold = workbook.add_format({
            'font_name': 'Times New Roman',
            'font_size': 12,
            'align': 'right',
            'valign': 'vcenter',
            'num_format': '_(* #,##0_);_(* (#,##0);_(* "-"??_);_(@_)',
            'border': 1,
            'font_color': '#FF0000',
        })

        textSpecialFormatNoBg = workbook.add_format({
            'font_name': 'Times New Roman',
            'font_size': 12,
            'align': 'center',
            'valign': 'vcenter',
            'border': 1,
            'font_color': '#FF0000',
        })

        percentFormat = workbook.add_format({
            'font_name': 'Times New Roman',
            'font_size': 12,
            'num_format': '0.00%',
            'border': 1,
            'align': 'right',
            'valign': 'vcenter'
        })

        percentSpecialFormat = workbook.add_format({
            'font_name': 'Times New Roman',
            'bold': True,
            'font_size': 12,
            'num_format': '0.00%',
            'border': 1,
            'align': 'right',
            'valign': 'vcenter',
            'bg_color': '#FFFF00',
            'font_color': '#FF0000'
        })

        percentRatioFormat = workbook.add_format({
            'font_name': 'Times New Roman',
            'font_size': 12,
            'num_format': '0%',
            'border': 1,
            'align': 'right',
            'valign': 'vcenter',
        })

        worksheet.set_column('A:A', 10)
        worksheet.set_column('B:E', 15)
        worksheet.set_column('F:I', 15)
        worksheet.set_column('J:K', 20)
        worksheet.set_column('L:L', 15)
        worksheet.set_column('M:M', 20)
        worksheet.set_column('N:N', 15)

        headers = [
            'Stock',
            'Market price',
            'Reference price',
            'Max price',
            'Ratio',
            'General room',
            'Used system room',
            'Special room',
            'Used special room',
            '% MP / Market price',
            '% Used GR / Approved GR',
            'Liquidity within 03 months',
            'Remaining P.Outs',
            'Note'
        ]
        # write
        worksheet.write_row('A1', headers, headerFormat)
        worksheet.write_column('A2', result['Stock'], textFormat)
        worksheet.write_column('B2', result['MarketPrice'], priceFormat)
        worksheet.write_column('C2', result['ReferencePrice'], priceFormat)
        worksheet.write_column('D2', result['MaxPrice'], priceFormat)
        worksheet.write_column('E2', result['Ratio'], percentRatioFormat)
        worksheet.write_column('F2', result['GeneralRoom'], priceFormat)
        worksheet.write_column('G2', result['UsedSystemRoom'], priceFormat)
        worksheet.write_column('H2', result['SpecialRoom'], priceFormat)
        worksheet.write_column('I2', result['UsedSpecialRoom'], priceFormat)
        worksheet.write_column('J2', result['% MP/MarketPrice'], percentFormat)
        worksheet.write_column('K2', result['% Used GR/ Approved GR'], percentFormat)
        worksheet.write_column('L2', result['3M Avg. Volume'], liquidityFormat)
        worksheet.write_column('M2', result['Remaining P.Outs'], priceFormat)
        worksheet.write_column('N2', result['Note'], textFormat)

        alphabet = 'ABCDEFGHIJKLMN'

        for idx in result.index:
            stock = result.loc[idx, 'Stock']
            marketPrice = result.loc[idx, 'MarketPrice']
            maxPrice = result.loc[idx, 'MaxPrice']
            pctMPDivMarketPrice = result.loc[idx, '% MP/MarketPrice']
            pctUsedGRDivApprovedGR = result.loc[idx, '% Used GR/ Approved GR']
            remainingPOuts = result.loc[idx, 'Remaining P.Outs']
            checkFloor = result.loc[idx, 'KiemTraGiamSan']

            # trường hợp các mã chạm sàn
            if checkFloor == 1:
                worksheet.write(idx + 1, alphabet.index('A'), stock, textSpecialFormatNoBg)
                worksheet.write(idx + 1, alphabet.index('B'), marketPrice, priceSpecialFormatNoBgNoBold)
            # trường hợp % MP/MarketPrice <= 5%
            if pctMPDivMarketPrice <= 5 / 100:
                worksheet.write(idx + 1, alphabet.index('J'), pctMPDivMarketPrice, percentSpecialFormat)
                worksheet.write(idx + 1, alphabet.index('D'), maxPrice, priceSpecialFormatBgBold)
            # trường hợp % Used GR/ Approved GR >= 85%
            if pctUsedGRDivApprovedGR >= 85/100:
                worksheet.write(idx + 1, alphabet.index('K'), pctUsedGRDivApprovedGR, percentSpecialFormat)
            # trường hợp Remaining P.Outs < 1.5 tỷ
            if 0 < remainingPOuts < 1.5e9:
                worksheet.write(idx + 1, alphabet.index('M'), remainingPOuts, priceSpecialFormatNoBgBold)

        writer.close()


class ExcelWriterReviewMPReport:

    def __init__(self):
        self.__today = dt.datetime.now()
        self.__data = None
        # self.__destinationDir = fr"C:\Users\namtran\Shared Folder\Risk Management\Monitor Stock"
        self.__destinationDir = fr"C:\Shared Folder\Risk Management\Monitor Stock"  # server

        self.__yearString = str(self.__today.year)
        if self.__today.month > 9:
            mStr = f'{self.__today.month}'
        else:
            mStr = f'0{self.__today.month}'
        self.__monthString = f'T{mStr}'
        self.__dateString = dt.datetime.now().strftime('%d.%m.%Y')

        # create 'year' folder
        if not os.path.isdir(join(self.__destinationDir, self.__yearString)):
            os.mkdir((join(self.__destinationDir, self.__yearString)))
        # create 'month' folder
        if not os.path.isdir(join(self.__destinationDir, self.__yearString, self.__monthString)):
            os.mkdir((join(self.__destinationDir, self.__yearString, self.__monthString)))
        # create 'date' folder in date folder
        if not os.path.isdir(join(self.__destinationDir, self.__yearString, self.__monthString, self.__dateString)):
            os.mkdir((join(self.__destinationDir, self.__yearString, self.__monthString, self.__dateString)))

        self.__filePath = None

    @property
    def inputData(self):
        if self.__data is None:
            raise ValueError('No data to write!')
        return self.__data

    @inputData.setter
    def inputData(self, value: pd.DataFrame):
        self.__data = value

    @property
    def pathToAttachFile(self):
        return self.__filePath

    def __writeToExcel(
            self,
            excelWriter: pd.ExcelWriter,
            sheetName: str,
            table: pd.DataFrame
    ):

        workbook = excelWriter.book
        worksheet = workbook.add_worksheet(sheetName)
        worksheet.hide_gridlines(option=2)
        worksheet.freeze_panes('F5')

        dateString = self.__today.strftime("%d/%m/%Y")

        # tô màu sheet
        if sheetName in ('MP ≤ Market price', 'MP > Market price ≤ 5%'):
            worksheet.set_tab_color("#FFC000")
        elif sheetName == 'Room':
            worksheet.set_tab_color("#B7FFD8")
        else:
            worksheet.set_tab_color("#FFFF00")

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
        worksheet.set_column('AS:AT', 12)

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
                'Last Volume',
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
        worksheet.write_row('AQ4', ['Fixed MP', 'Blacklist', 'Note 1 (MC, AV)', 'Note 2 (CBP,Var)'], headerBlueBgFormat)

        # WRITE DATA
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
        worksheet.write_column('U5', table['MarketCap'], priceDecimalFormat)
        worksheet.write_column('V5', table['NetProfit'], priceDecimalFormat)
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
        worksheet.write_column('AR5', table['Blacklist'], textCenterFormat)
        worksheet.write_column('AS5', table['Note 1'], textCenterFormat)
        worksheet.write_column('AT5', table['Note 2'], textCenterFormat)

    def run(self, option: str):

        """
        :param option: if half send 2 sheet (1 & 2), else send 4 sheet
        :return: None
        """

        dateString = self.__today.strftime("%d.%m.%Y")
        timeString = self.__today.strftime("%H.%M.%S")
        fileName = f"Data_Review MP & Room_Final {timeString} - {dateString}.xlsx"
        self.__filePath = join(
            self.__destinationDir, self.__yearString, self.__monthString, self.__dateString, fileName
        )
        excelWriter = pd.ExcelWriter(
            self.__filePath,
            engine='xlsxwriter',
            engine_kwargs={'options': {'nan_inf_to_errors': True}}
        )

        fullTable = self.inputData

        mappingSheet = {
            1: 'MP ≤ Market price',
            2: 'MP > Market price ≤ 5%',
            3: 'MP > Market price < 10%',
            4: 'MP > 130%',
            5: 'Room'
        }

        if option == 'half':
            chooseSheet = (1, 2, 5)
        else:
            chooseSheet = range(1, 6)

        for sheet in chooseSheet:
            if sheet == 5:
                table = fullTable.loc[fullTable['RoomSheet'] == 1].reset_index(drop=True)
                table.sort_values(by='Stock', inplace=True)
            else:
                table = fullTable.loc[fullTable['Sheet'] == sheet].reset_index(drop=True)
            self.__writeToExcel(
                excelWriter=excelWriter,
                sheetName=mappingSheet.get(sheet),
                table=table
            )
        # CLOSING FILE
        excelWriter.close()
