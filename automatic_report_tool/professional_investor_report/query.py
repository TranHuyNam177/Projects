import os
import datetime as dt
import pandas as pd
from request import connect_DWH_CoSo
from datawarehouse import BDATE
from info import CompanyName, CompanyAddress, CompanyPhoneNumber

import warnings

warnings.filterwarnings('ignore')


class Query:

    def __init__(
            self,
            fromDate: dt.datetime,
            toDate: dt.datetime,
            accountCode: str,
    ):
        self.fromDate = fromDate
        self.toDate = toDate
        fromDateRawString = fromDate.strftime('%Y-%m-%d')
        toDateRawString = toDate.strftime('%Y-%m-%d')
        self.fromDateString = self.__correctDate(fromDateRawString)
        self.toDateString = self.__correctDate(toDateRawString)
        self.accountCode = accountCode
        self.result = None
        # File Name
        fileDirectory = './output'
        if not os.path.isdir(fileDirectory):
            os.mkdir(fileDirectory)
        fileName = f"{accountCode}_{fromDateRawString}_{toDateRawString}.xlsx".replace('-', '')
        self.filePath = os.path.join(fileDirectory, fileName)

    @staticmethod
    def __correctDate(dateString: str):
        return BDATE(BDATE(dateString, -1), 1)

    def execute(
            self,
    ):
        self.result = pd.read_sql(
            f"""
            -- @block 
            -- @conn DWH-PHS
            -- @label AvgNAV
            DECLARE @SoTaiKhoan CHAR(10);
            SET @SoTaiKhoan = '{self.accountCode}';

            DECLARE @FromDate DATETIME;
            SET @FromDate = '{self.fromDateString}';

            DECLARE @ToDate DATETIME;
            SET @ToDate = '{self.toDateString}';

            IF @FromDate < '2022-04-19' OR @ToDate < '2022-04-19'
                THROW 50001, 'Cannot query before 19/04/2022', 1

            DECLARE @TenKhachHang NVARCHAR(MAX);
            SET @TenKhachHang = (SELECT TOP 1 [customer_name] FROM [account] WHERE [account_code] = @SoTaiKhoan);

            WITH
            [_Diff] AS ( --ok
                SELECT *, [Row] - MAX([Row]*[Work]) OVER(ORDER By [Date] ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) [Diff]
                FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY [Date].[Date]) [Row] FROM [Date]) [a]
            )
            ,[BangNgay] AS ( --ok
                SELECT
                    [Date].[Date] [T0],
                    [Date].[Date] - [_Diff].[Diff] [T1],
                    [Date].[Work] [NgayLamViec]
                FROM [Date]
                LEFT JOIN [_Diff] ON [Date].[Date] = [_Diff].[Date]
            )
            ,[_BangGia] AS ( --ok
                SELECT
                    [Date] [Ngay],
                    [Ticker] [MaChungKhoan],
                    [Close] * 1000 [GiaDongCua]
                FROM [DWH-ThiTruong].[dbo].[DuLieuGiaoDichNgay]
            )
            ,[_GiaTriChungKhoanTheoNgayVaMa] AS ( --ok
                SELECT
                    [RSE0008].[Ngay],
                    [RSE0008].[GiaoDich] 
                        + [RSE0008].[HCCN] 
                        + [RSE0008].[PhongToa]
                    [SoDuChungKhoan],
                    [RSE0008].[MaCK] [MaChungKhoan],
                    CASE
                        WHEN [BangNgay].[NgayLamViec] = 1 AND [_BangGia].[GiaDongCua] IS NULL THEN 0
                        ELSE [_BangGia].[GiaDongCua]
                    END [GiaDongCua],
                    CASE
                        WHEN [BangNgay].[NgayLamViec] = 1 AND [_BangGia].[GiaDongCua] IS NULL THEN 0
                        ELSE [_BangGia].[GiaDongCua] * (
                            [RSE0008].[GiaoDich] 
                            + [RSE0008].[HCCN] 
                            + [RSE0008].[PhongToa]
                        ) 
                    END [GiaTriChungKhoan]
                FROM [RSE0008]
                LEFT JOIN [sub_account] ON [sub_account].[sub_account] = [RSE0008].[SoTieuKhoan]
                LEFT JOIN [BangNgay] ON [BangNgay].[T0] = [RSE0008].[Ngay]
                LEFT JOIN [_BangGia] ON [_BangGia].[Ngay] = [BangNgay].[T1] AND [_BangGia].[MaChungKhoan] = [RSE0008].[MaCK]
                WHERE [RSE0008].[Ngay] BETWEEN @FromDate AND @ToDate
                    AND [RSE0008].[SoTieuKhoan] IN (SELECT [sub_account] FROM [sub_account] WHERE [account_code] =  @SoTaiKhoan)
            )
            ,[GiaTriChungKhoanTheoNgay] AS ( -- ok
                SELECT
                    [_GiaTriChungKhoanTheoNgayVaMa].[Ngay],
                    SUM([_GiaTriChungKhoanTheoNgayVaMa].[GiaTriChungKhoan]) [GiaTriChungKhoan]
                FROM [_GiaTriChungKhoanTheoNgayVaMa]
                GROUP BY [_GiaTriChungKhoanTheoNgayVaMa].[Ngay]
            ),
            [DuNo] AS ( -- ok
                SELECT 
                    [margin_outstanding].[date] [Ngay],
                    [margin_outstanding].[principal_outstanding] [DuNoGoc],
                    [margin_outstanding].[interest_outstanding] [DuNoLai]
                FROM [margin_outstanding]
                WHERE [margin_outstanding].[account_code] =  @SoTaiKhoan
                    AND [margin_outstanding].[date] BETWEEN @FromDate AND @ToDate
            )
            ,[_SoLuongChungKhoanDuocTinh] AS ( -- ok 
                SELECT
                    COALESCE([vmr0104].[date],[VMR9004].[date]) [Ngay],
                    COALESCE([vmr0104].[ticker],[VMR9004].[ticker]) [MaChungKhoan],
                    SUM(CASE
                        WHEN ISNULL([vmr0104].[special_room],0)+ISNULL([vmr0104].[used_system_room],0)
                            > ISNULL([VMR9004].[receiving_volume],0)
                            THEN ISNULL([VMR9004].[receiving_volume],0)
                        ELSE ISNULL([vmr0104].[special_room],0)+ISNULL([vmr0104].[used_system_room],0)
                    END) [SoLuongChungKhoanDuocTinh]
                FROM [vmr0104]
                FULL OUTER JOIN [VMR9004] 
                    ON [vmr0104].[sub_account] = [VMR9004].[sub_account]
                    AND [vmr0104].[date] = [VMR9004].[date]
                    AND [vmr0104].[ticker] = [VMR9004].[ticker]
                WHERE COALESCE([vmr0104].[sub_account],[VMR9004].[sub_account]) IN (SELECT [sub_account] FROM [sub_account] WHERE [account_code] =  @SoTaiKhoan)
                    AND COALESCE([vmr0104].[date],[VMR9004].[date]) BETWEEN @FromDate AND @ToDate
                GROUP BY 
                    COALESCE([vmr0104].[date],[VMR9004].[date]),
                    COALESCE([vmr0104].[ticker],[VMR9004].[ticker])
            )
            ,[_DP] AS ( -- ok 
                SELECT 
                    [margin_outstanding].[date] [Ngay],
                    SUM(CASE WHEN [type] = N'Trả chậm' THEN 1 ELSE 0 END) [Count]
                FROM [margin_outstanding]
                WHERE [margin_outstanding].[account_code] =  @SoTaiKhoan
                    AND [margin_outstanding].[date] BETWEEN @FromDate AND @ToDate
                GROUP BY [margin_outstanding].[date]
            )
            ,[_GiaVayTyLe] AS ( -- ok
                SELECT 
                    [VMR9004].[date] [Ngay],
                    [VMR9004].[ticker] [MaChungKhoan],
                    [VMR9004].[mr_price] [GiaVay],
                    CASE WHEN [_DP].[Count] = 0 THEN [VMR9004].[mr_ratio] ELSE [VMR9004].[dp_ratio] END [TyLe]
                FROM [VMR9004]
                LEFT JOIN [_DP] ON [_DP].[Ngay] = [VMR9004].[date]
                WHERE [VMR9004].[date] BETWEEN @FromDate AND @ToDate
                    AND [VMR9004].[sub_account] IN (SELECT [sub_account] FROM [sub_account] WHERE [account_code] =  @SoTaiKhoan)
            )
            ,[DuNoChungKhoanMuaChoVe] AS ( -- ok, không đủ ngày)
                SELECT
                    COALESCE([_SoLuongChungKhoanDuocTinh].[Ngay],[_GiaVayTyLe].[Ngay]) [Ngay],
                    SUM(
                        ISNULL([_SoLuongChungKhoanDuocTinh].[SoLuongChungKhoanDuocTinh],0) 
                        * ISNULL([_GiaVayTyLe].[GiaVay],0)
                        * ISNULL([_GiaVayTyLe].[TyLe],0) / 100
                    ) [DuNoChungKhoanMuaChoVe]
                FROM [_SoLuongChungKhoanDuocTinh]
                FULL OUTER JOIN [_GiaVayTyLe] ON [_SoLuongChungKhoanDuocTinh].[Ngay] = [_GiaVayTyLe].[Ngay]
                    AND [_SoLuongChungKhoanDuocTinh].[MaChungKhoan] = [_GiaVayTyLe].[MaChungKhoan]
                GROUP BY 
                    COALESCE([_SoLuongChungKhoanDuocTinh].[Ngay],[_GiaVayTyLe].[Ngay])
            )
            ,[TienTrenTaiKhoan] AS ( -- ok, không đủ ngày)
                SELECT
                    [rmr0062].[date] [Ngay],
                    SUM([rmr0062].[cash]) [TienTrenTaiKhoan]
                FROM [rmr0062]
                WHERE [rmr0062].[date] BETWEEN @FromDate AND @ToDate
                    AND [rmr0062].[account_code] =  @SoTaiKhoan
                GROUP BY [rmr0062].[date]
            )
            SELECT
                @TenKhachHang [TenKhachHang],
                [BangNgay].[T0] [Ngay],
                ISNULL([GiaTriChungKhoanTheoNgay].[GiaTriChungKhoan],0) [GiaTriChungKhoan],
                ISNULL([DuNo].[DuNoGoc],0) [DuNoGoc],
                ISNULL([DuNo].[DuNoLai],0) [DuNoLai],
                [DuNoChungKhoanMuaChoVe].[DuNoChungKhoanMuaChoVe],
                [TienTrenTaiKhoan].[TienTrenTaiKhoan]
            FROM [BangNgay]
            LEFT JOIN [GiaTriChungKhoanTheoNgay] ON [GiaTriChungKhoanTheoNgay].[Ngay] = [BangNgay].[T0]
            LEFT JOIN [DuNo] ON [DuNo].[Ngay] = [BangNgay].[T0]
            LEFT JOIN [DuNoChungKhoanMuaChoVe] ON [DuNoChungKhoanMuaChoVe].[Ngay] = [BangNgay].[T0]
            LEFT JOIN [TienTrenTaiKhoan] ON [TienTrenTaiKhoan].[Ngay] = [BangNgay].[T0]
            WHERE [BangNgay].[T0] BETWEEN @FromDate AND @ToDate
            ORDER BY [BangNgay].[T0]
            -- PHẢI FORWARD FILL 2 CỘT CUỐI BẰNG PANDAS
            """,
            connect_DWH_CoSo
        )
        self.result['DuNoChungKhoanMuaChoVe'] = self.result['DuNoChungKhoanMuaChoVe'].fillna(method='ffill').fillna(0)
        self.result['TienTrenTaiKhoan'] = self.result['TienTrenTaiKhoan'].fillna(method='ffill').fillna(0)
        self.result['TongNo'] = self.result['DuNoGoc'] + self.result['DuNoLai'] - self.result[
            'DuNoChungKhoanMuaChoVe'] - self.result['TienTrenTaiKhoan']
        self.result['TongNo'] = self.result['TongNo'].apply(max, args=(0,))
        self.result['TaiSanRong'] = self.result['GiaTriChungKhoan'] - self.result['TongNo']

    def toExcel(
            self,
    ):
        if self.result is None:
            self.execute()

        writer = pd.ExcelWriter(
            self.filePath,
            engine='xlsxwriter',
            engine_kwargs={'options': {'nan_inf_to_errors': True}}
        )
        workbook = writer.book
        worksheet = workbook.add_worksheet('Sheet1')
        worksheet.hide_gridlines(option=2)
        # set column width
        worksheet.set_column('A:A', 16)
        worksheet.set_column('B:G', 25)
        # insert image
        worksheet.insert_image('A1', './img/phs_logo.png', {'x_scale': 0.55, 'y_scale': 0.55})
        company_format = workbook.add_format({
            'font_size': 12,
            'font_name': 'Times New Roman',
            'bold': True
        })
        general_format = workbook.add_format({
            'font_size': 12,
            'font_name': 'Times New Roman',
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'text_wrap': True,
            'border': 1
        })
        normal_format = workbook.add_format({
            'font_size': 12,
            'font_name': 'Times New Roman'
        })
        date_range_format = workbook.add_format({
            'font_size': 12,
            'font_name': 'Times New Roman',
            'italic': True,
            'align': 'center'
        })
        center_format = workbook.add_format({
            'font_size': 12,
            'font_name': 'Times New Roman',
            'align': 'center',
            'bold': True,
        })
        number_format = workbook.add_format({
            'font_size': 12,
            'font_name': 'Times New Roman',
            'num_format': '#,##0',
            'border': 1
        })
        date_format = workbook.add_format({
            'font_size': 12,
            'font_name': 'Times New Roman',
            'num_format': 'dd/mm/yyyy',
            'border': 1
        })
        worksheet.merge_range('B1:G1', CompanyName, company_format)
        worksheet.merge_range('B2:G2', CompanyAddress, normal_format)
        worksheet.merge_range('B3:G3', CompanyPhoneNumber, normal_format)
        worksheet.merge_range('A5:G5', f'Họ và tên: {self.result.loc[self.result.index[0], "TenKhachHang"]}',
                              center_format)
        worksheet.merge_range('A6:G6', f'Số tài khoản lưu ký: {self.accountCode}', center_format)
        worksheet.merge_range('A7:G7',
                              f"Từ ngày: {self.fromDate.strftime('%d.%m.%Y')} đến ngày: {self.toDate.strftime('%d.%m.%Y')}",
                              date_range_format)

        columnNames = ['Ngày', 'Giá Trị Chứng Khoán', 'Nợ Gốc', 'Nợ Lãi', 'Dư Nợ CK Mua Chờ Về', 'Tiền Trên Tài Khoản',
                       'Tài Sản Ròng']
        worksheet.write_row('A9', columnNames, general_format)
        worksheet.write_column('A10', self.result['Ngay'], date_format)
        worksheet.write_column('B10', self.result['GiaTriChungKhoan'], number_format)
        worksheet.write_column('C10', self.result['DuNoGoc'], number_format)
        worksheet.write_column('D10', self.result['DuNoLai'], number_format)
        worksheet.write_column('E10', self.result['DuNoChungKhoanMuaChoVe'], number_format)
        worksheet.write_column('F10', self.result['TienTrenTaiKhoan'], number_format)
        worksheet.write_column('G10', self.result['TaiSanRong'], number_format)
        lastRow = self.result.shape[0] + 10
        worksheet.merge_range(f'A{lastRow}:F{lastRow}', 'Trung bình', general_format)
        worksheet.write(f'G{lastRow}', f'=AVERAGE(G10:G{lastRow - 1})', number_format)
        workbook.close()

# if __name__ == '__main__':
#     os.chdir(r"C:\Users\hiepdang\PycharmProjects\DataAnalytics\automation\trading_service\giaodichluuky\BaoCaoTrungBinhNAV")
#     query = Query(dt.datetime(2022,4,19),dt.datetime(2022,10,19),'022C696969')
#     query.execute()
#     query.toExcel()
