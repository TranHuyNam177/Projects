import datetime as dt
import pandas as pd
import time
import os

from os.path import join
from datawarehouse import BDATE, SYNC, CHECKBATCH
from request import connect_DWH_AppData, connect_DWH_CoSo
from automation.risk_management import get_info, dept_folder


class Report:

    def __init__(
            self,
            runDate: dt.datetime
    ) -> None:
        self.__runDate = runDate

        self.__year = runDate.year
        self.__month = runDate.month
        # tDate = ngày chạy báo cáo
        self.t0Date = runDate.strftime('%Y-%m-%d')
        # t1Date = T+1
        self.t1Date = BDATE(self.t0Date, 1)
        # t2Date = T+2
        self.t2Date = BDATE(self.t0Date, 2)
        # t_1Date = T-1
        self.t_1Date = BDATE(self.t0Date, -1)
        # t_2Date = T-2
        self.t_2Date = BDATE(self.t0Date, -2)
        # t_2Month = Lùi lại 2 tháng (60 ngày làm việc)
        self.t_2Month = BDATE(self.t0Date, -60)
        # ngày chạy RLN0006
        if runDate.weekday() == 4:  # nếu chạy trúng ngày T6 thì RLN0006 lấy của ngày CN
            self.__runDateRLN0006 = runDate + dt.timedelta(days=2)
        else:
            self.__runDateRLN0006 = runDate
        self.tDateRLN0006 = self.__runDateRLN0006.strftime('%Y-%m-%d')

    @property
    def data(self):

        dataDate = self.__runDate.strftime("%Y-%m-%d")
        dataRLN0006Date = self.__runDateRLN0006.strftime("%Y-%m-%d")

        today = dt.datetime.now()

        if self.__runDate.date() == today.date():  # ngày chạy bằng ngày hiện tại
            # khi bắt đầu chạy file, nếu chưa batch xong thì chờ 60s
            while not (CHECKBATCH(conn=connect_DWH_CoSo, batchType=2)):
                time.sleep(60)
            # đồng bộ data
            SYNC(connect_DWH_CoSo, 'spVLN0001', FrDate=dataDate, ToDate=dataDate)
            SYNC(connect_DWH_CoSo, 'spvmr0003', FrDate=dataDate, ToDate=dataDate)
            SYNC(connect_DWH_CoSo, 'spvmr9004', FrDate=dataDate, ToDate=dataDate)
            SYNC(connect_DWH_CoSo, 'spvpr0109', FrDate=dataDate, ToDate=dataDate)
            SYNC(connect_DWH_CoSo, 'spRSE0008', FrDate=dataDate, ToDate=dataDate)
            SYNC(connect_DWH_CoSo, 'spROD0001', FrDate=dataDate, ToDate=dataDate)
            SYNC(connect_DWH_CoSo, 'spRCA0020', FrDate=dataDate, ToDate=dataDate)
            SYNC(connect_DWH_CoSo, 'spRCA0003', FrDate=dataDate, ToDate=dataDate)
            SYNC(connect_DWH_CoSo, 'spRSE1000', FrDate=dataDate, ToDate=dataDate)
            SYNC(connect_DWH_CoSo, 'spmargin_outstanding', FrDate=dataDate, ToDate=dataRLN0006Date)
            SYNC(connect_DWH_CoSo, 'sprelationship', FrDate=dataDate, ToDate=dataDate)
            SYNC(connect_DWH_CoSo, 'spManHinhHome', FrDate=dataDate, ToDate=dataDate)
            SYNC(connect_DWH_CoSo, 'sptransaction_in_system', FrDate=dataDate, ToDate=dataDate)
            SYNC(connect_DWH_CoSo, 'sp220001', FrDate=dataDate, ToDate=dataDate)

        statement = f"""
        EXEC
            [spRMD_ForceSell_SuKienQuyen]
            @t_2Month='{self.t_2Month}',
            @t_2Date='{self.t_2Date}', 
            @t_1Date='{self.t_1Date}', 
            @t0Date='{self.t0Date}', 
            @t1Date='{self.t1Date}', 
            @t2Date='{self.t2Date}',
            @toDateRLN0006='{self.tDateRLN0006}'
        """

        return pd.read_sql(
            sql=statement,
            con=connect_DWH_AppData
        )

    def writeExcel(self) -> None:

        info = get_info('daily', self.__runDate)
        period = info['period']
        folder_name = info['folder_name']
        # create folder
        if not os.path.isdir(join(dept_folder, folder_name, period)):
            os.mkdir((join(dept_folder, folder_name, period)))

        # ghi file excel
        fileName = f"ForceSell - Sự kiện quyền_{self.__runDate.strftime('%d.%m.%Y')}.xlsx"
        filePath = join(dept_folder, folder_name, period, fileName)

        excelWriter = pd.ExcelWriter(
            path=filePath,
            engine='xlsxwriter',
            engine_kwargs={'options': {'nan_inf_to_errors': True}}
        )

        workbook = excelWriter.book

        ###################################################
        ###################################################
        ###################################################

        # EXCEL FORMATS
        headerNormalFormat = workbook.add_format({
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'font_size': 10,
            'font_name': 'Calibri',
            'text_wrap': True
        })
        headerRedFormat = workbook.add_format({
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'font_size': 10,
            'font_name': 'Calibri',
            'text_wrap': True,
            'font_color': '#FF0000'
        })
        accountNormalFormat = workbook.add_format({
            'align': 'center',
            'valign': 'vcenter',
            'font_size': 10,
            'font_name': 'Calibri'
        })
        accountBadDebtFormat = workbook.add_format({
            'align': 'center',
            'valign': 'vcenter',
            'font_size': 10,
            'font_name': 'Calibri',
            'bg_color': '#ED7D31'
        })
        accountHasWFTFormat = workbook.add_format({
            'align': 'center',
            'valign': 'vcenter',
            'font_size': 10,
            'font_name': 'Calibri',
            'font_color': '#FF0000',
            'bg_color': '#FFFF00'
        })
        textLeftFormat = workbook.add_format({
            'align': 'left',
            'valign': 'top',
            'font_size': 10,
            'font_name': 'Calibri'
        })
        moneyFormat = workbook.add_format({
            'align': 'right',
            'valign': 'top',
            'font_size': 10,
            'font_name': 'Calibri',
            'num_format': '#,##0'
        })
        moneyRedFormat = workbook.add_format(
            {
                'align': 'right',
                'valign': 'top',
                'font_size': 10,
                'font_name': 'Calibri',
                'num_format': '#,##0',
                'font_color': '#FF0000',
            }
        )
        numberFormat = workbook.add_format({
            'align': 'right',
            'valign': 'top',
            'font_size': 10,
            'font_name': 'Calibri'
        })
        numberRedFormat = workbook.add_format({
            'align': 'right',
            'valign': 'top',
            'font_size': 10,
            'font_name': 'Calibri',
            'font_color': '#FF0000'
        })
        dateFormat = workbook.add_format({
            'align': 'left',
            'valign': 'top',
            'font_size': 10,
            'font_name': 'Calibri',
            'num_format': 'dd/mm/yyyy'
        })

        ###################################################
        ###################################################
        ###################################################

        # Danh sách tên cột
        headers = [
            'Tên chi nhánh',  # A
            'Số TK lưu ký',  # B
            'Tình trạng force sell',  # C
            'Xử lý force sell',  # D
            'Note',  # E
            'Tên khách hàng',  # F
            'TL thực tế MR Trước',  # G
            'TL thực tế MR',  # H
            'TL thực tế TC Trước',  # I
            'TL thực tế TC',  # J
            'Ngày bắt đầu Call',  # K
            'Tiền mặt nộp về 100% Trước',  # L
            'Tiền mặt nộp về 100%',  # M
            'Tiền mặt nộp về 85% Trước',  # N
            'Tiền mặt nộp về 85%',  # O
            'Tiền mặt nộp về Rtt_DP Trước',  # P
            'Tiền mặt nộp về Rtt_DP',  # Q
            'Nợ hạn mức',  # R
            'Nợ MR+TC quá hạn',  # S
            'Tên MG',  # T
            'DP',  # U
            'Khối lượng cổ phiếu chở về'  # V
        ]

        ###################################################
        ###################################################
        ###################################################

        result = self.data
        result = result.drop(['SoTieuKhoan'], axis=1)
        # lấy ra danh sách tài khoản nợ xấu
        badDebtAccounts = result.loc[
            result['BadDebt'] == 'BadDebt',
            'SoTaiKhoan'
        ].to_list()

        worksheet = workbook.add_worksheet(f'XuLyBan')

        # set size cho cột
        worksheet.set_column('A:B', 10)
        worksheet.set_column('C:E', 8)
        worksheet.set_column('F:F', 25)
        worksheet.set_column('G:J', 9)
        worksheet.set_column('K:S', 13)
        worksheet.set_column('T:T', 25)
        worksheet.set_column('U:V', 13)
        for col in 'GILNP':
            worksheet.set_column(f'{col}:{col}', None, None, {'hidden': True})

        # viết vào file excel
        for num, header in enumerate(headers):
            worksheet.write(0, num, header, headerNormalFormat)

        worksheet.conditional_format('C1:E1', {'type': 'no_errors', 'format': headerRedFormat})
        for colNum, colName in enumerate(result.columns[:-1]):
            if colName.lower().startswith('tl'):
                fmt = numberFormat
            elif pd.api.types.is_numeric_dtype(result[colName]):
                fmt = moneyFormat
            elif pd.api.types.is_datetime64_dtype(result[colName]):
                result[colName] = result[colName].replace(pd.NaT, dt.datetime(9999, 12, 31))
                fmt = dateFormat
            else:
                fmt = textLeftFormat
            worksheet.write_column(1, colNum, result[colName], fmt)

        # Format lần 2 các TH cần lưu ý
        for row in range(len(result)):

            Account = result.loc[result.index[row], 'SoTaiKhoan']
            NoHanMuc = result.loc[result.index[row], 'NoHanMuc']
            TLThucTeMR = result.loc[result.index[row], 'TLThucTeMR_Sau']
            NoMRTCQuaHan = result.loc[result.index[row], 'NoMRTCQuaHan']
            NoDP = result.loc[result.index[row], 'DP']
            TLThucTeDP = result.loc[result.index[row], 'TLThucTeTC_Sau']
            KhoiLuongCPChoVe = result.loc[result.index[row], 'KhoiLuongCoPhieuChoVe']

            writtenRow = row + 1

            for col, colName in enumerate(result.columns):
                if colName == 'SoTaiKhoan':
                    if Account in badDebtAccounts:
                        fmt = accountBadDebtFormat
                    elif KhoiLuongCPChoVe > 0:
                        fmt = accountHasWFTFormat
                    else:
                        fmt = accountNormalFormat
                    worksheet.write(writtenRow, col, Account, fmt)
                if NoDP > 0 and TLThucTeDP < 100:
                    if colName in ('TienMatVeRTTDP_Sau', 'TLThucTeTC_Sau'):
                        value = result.loc[result.index[row], colName]
                        worksheet.write(writtenRow, col, value, numberRedFormat)
                else:
                    if NoHanMuc != 0:
                        condition1 = TLThucTeMR >= 85 and colName in ('NoHanMuc',)
                        condition2 = TLThucTeMR < 85 and colName in (
                            'NoHanMuc', 'TLThucTeMR_Sau', 'TienMatNopVe100_Sau', 'TienMatNopVe85_Sau'
                        )
                        if condition1 or condition2:
                            value = result.loc[result.index[row], colName]
                            worksheet.write(writtenRow, col, value, moneyRedFormat)
                    else:
                        condition1 = NoMRTCQuaHan != 0 and colName in ('NoMRTCQuaHan',)
                        condition2 = NoMRTCQuaHan == 0 and colName in ('TienMatNopVe100_Sau', 'TienMatNopVe85_Sau')
                        condition3 = NoMRTCQuaHan == 0 and colName in ('TLThucTeMR_Sau',)
                        if condition1 or condition2:
                            value = result.loc[result.index[row], colName]
                            worksheet.write(writtenRow, col, value, moneyRedFormat)
                        elif condition3:
                            value = result.loc[result.index[row], colName]
                            worksheet.write(writtenRow, col, value, numberRedFormat)
        excelWriter.close()


def run():
    report = Report(dt.datetime.now())
    report.writeExcel()
