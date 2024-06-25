from automation.risk_management.MonitorStockReport.worker import *
from datawarehouse import BDATE


def run():
    today = dt.datetime.now()
    # Tìm Ngày chạy
    todayString = today.strftime('%Y-%m-%d')
    if dt.datetime.strptime(BDATE(BDATE(todayString, -1), 1), '%Y-%m-%d').date() != dt.datetime.now().date():
        runDate = dt.datetime.strptime(BDATE(todayString, -1), '%Y-%m-%d')
        runDateString = runDate.strftime('%Y-%m-%d')
    else:
        runDate = today
        runDateString = runDate.strftime('%Y-%m-%d')

    if runDate == today:
        # đồng bộ dữ liệu bảng điện, vpr0109 và 230007
        print("Waiting to SYNC data table vpr0109 ...")
        SYNC(connect_DWH_CoSo, 'spvpr0109', FrDate=runDateString, ToDate=runDateString)
        print("Waiting to SYNC data table 230007 ...")
        SYNC(connect_DWH_CoSo, 'sp230007', FrDate=runDateString, ToDate=runDateString)
        print("Waiting to SYNC data BangDienRealTime ...")
        EXEC(connect_DWH_ThiTruong, 'spBangDienRealTime')

    print("Waiting to SYNC data table DataHop ...")
    EXEC(connect_DWH_CoSo, 'spDataHop', Date=runDateString)

    # File excel report Review MP
    writeExcelReport = ExcelWriterMonitorReport(runDate=runDate)
    writeExcelReport.run()
