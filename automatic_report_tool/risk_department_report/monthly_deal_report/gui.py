import os
import datetime as dt
import re

from PyQt5 import QtWidgets
from PyQt5.QtGui import QIcon, QFont
from PyQt5.QtWidgets import QMessageBox, QPushButton, QGridLayout, QWidget, QApplication

from automation.risk_management.MonthlyDealReport.job import ExcelWriter
from automation.risk_management.MonthlyDealReport.data import SheetMonthlyDeal, SheetSummaryInfo, Liquidity3M
from datawarehouse import CHECKBATCH, BDATE, SYNC, EXEC
from request import connect_DWH_CoSo

class InputWindow(QWidget):

    def __init__(self):
        super().__init__()

        self.setFixedSize(500, 300)
        self.setWindowTitle("Monthly Deal Report")
        self.setWindowIcon(QIcon('phs_icon.ico'))

        # Label: Chọn Room code cho sheet Summary Info
        roomCodeSheetSummaryLabel = QtWidgets.QLabel('Room code Summary Info: ')
        roomCodeSheetSummaryLabel.setFont(QFont('Times New Roman', 15))
        # Text box: Room code cho sheet Summary Info
        self.roomCodeSheetSummaryBox = QtWidgets.QLineEdit()
        self.roomCodeSheetSummaryBox.setFont(QFont('Times New Roman', 15))
        self.roomCodeSheetSummaryBox.setText('ALL')

        # Label: Chọn Room code cho sheet Monthly Deal
        roomCodeSheetMonthlyDealLabel = QtWidgets.QLabel('Room code Monthly Deal: ')
        roomCodeSheetMonthlyDealLabel.setFont(QFont('Times New Roman', 15))
        # Text box: Room code cho sheet MonthlyDeal
        self.roomCodeSheetMonthlyDealBox = QtWidgets.QLineEdit()
        self.roomCodeSheetMonthlyDealBox.setFont(QFont('Times New Roman', 15))
        self.roomCodeSheetMonthlyDealBox.setText('ALL')

        # Label: Chọn ngày chạy báo cáo
        runDateLabel = QtWidgets.QLabel('Export date: ')
        runDateLabel.setFont(QFont('Times New Roman', 15))
        # Text box: Chọn From Date to ROD0040
        self.runDateBox = QtWidgets.QLineEdit()
        self.runDateBox.setFont(QFont('Times New Roman', 15))
        self.runDateBox.setText(dt.datetime.now().strftime("%d/%m/%Y"))

        # Label: Chọn From Date to ROD0040
        fromDateROD0040Label = QtWidgets.QLabel('Start date: ')
        fromDateROD0040Label.setFont(QFont('Times New Roman', 15))
        # Text box: Chọn From Date to ROD0040
        self.fromDateROD0040Box = QtWidgets.QLineEdit()
        self.fromDateROD0040Box.setFont(QFont('Times New Roman', 15))
        self.fromDateROD0040Box.setText('dd/mm/yyyy')

        # Label: Chọn To Date to ROD0040
        toDateROD0040Label = QtWidgets.QLabel('End date: ')
        toDateROD0040Label.setFont(QFont('Times New Roman', 15))
        # Text box: Chọn To Date to ROD0040
        self.toDateROD0040Box = QtWidgets.QLineEdit()
        self.toDateROD0040Box.setFont(QFont('Times New Roman', 15))
        self.toDateROD0040Box.setText('dd/mm/yyyy')

        # Button: Xuất báo cáo
        exportButton = QPushButton('Export report', self)
        exportButton.setFont(QFont('Times New Roman', 12))
        exportButton.clicked.connect(self.run)

        # Create layout
        layout = QGridLayout()
        layout.setContentsMargins(10, 10, 10, 10)
        # Add widgets to the layout
        layout.addWidget(roomCodeSheetSummaryLabel, 0, 0)
        layout.addWidget(self.roomCodeSheetSummaryBox, 0, 1)
        layout.addWidget(roomCodeSheetMonthlyDealLabel, 1, 0)
        layout.addWidget(self.roomCodeSheetMonthlyDealBox, 1, 1)
        layout.addWidget(runDateLabel, 2, 0)
        layout.addWidget(self.runDateBox, 2, 1)
        layout.addWidget(fromDateROD0040Label, 3, 0)
        layout.addWidget(self.fromDateROD0040Box, 3, 1)
        layout.addWidget(toDateROD0040Label, 4, 0)
        layout.addWidget(self.toDateROD0040Box, 4, 1)
        layout.addWidget(exportButton, 5, 1)
        # Set layout in the main window
        self.setLayout(layout)
        self.__filePath = None

    def check(self):
        roomCodeSheetSummaryText = self.roomCodeSheetSummaryBox.text()
        roomCodeSheetMonthlyDealText = self.roomCodeSheetMonthlyDealBox.text()
        runDateText = self.runDateBox.text()
        fromDateROD0040Text = self.fromDateROD0040Box.text()
        toDateROD0040Text = self.toDateROD0040Box.text()

        conditionRoomCode1 = re.search(r'\d{4}', roomCodeSheetSummaryText) is not None
        conditionRoomCode2 = roomCodeSheetSummaryText.lower() == 'all'
        conditionRoomCode3 = re.search(r'\d{4}', roomCodeSheetMonthlyDealText) is not None
        conditionRoomCode4 = roomCodeSheetMonthlyDealText.lower() == 'all'

        checkRoomCode = (conditionRoomCode1 or conditionRoomCode2) and (conditionRoomCode3 or conditionRoomCode4)
        checkDate1 = re.search(r'\d{2}/\d{2}/\d{4}', fromDateROD0040Text) is not None
        checkDate2 = re.search(r'\d{2}/\d{2}/\d{4}', toDateROD0040Text) is not None
        checkDate3 = re.search(r'\d{2}/\d{2}/\d{4}', runDateText) is not None

        if all((checkRoomCode, checkDate1, checkDate2, checkDate3)):
            return True
        return False

    def run(self):
        roomCodeSheetSummaryText = self.roomCodeSheetSummaryBox.text()
        roomCodeSheetMonthlyDealText = self.roomCodeSheetMonthlyDealBox.text()
        runDateText = self.runDateBox.text()
        fromDateROD0040Text = self.fromDateROD0040Box.text()
        toDateROD0040Text = self.toDateROD0040Box.text()

        if not self.check():
            type(self).popUp(QMessageBox.Warning, 'Dữ liệu đầu vào chưa đúng!')
        else:
            if dt.datetime.strptime(runDateText, '%d/%m/%Y').date() == dt.datetime.now().date(): # Nếu chạy ngày hôm nay
                if CHECKBATCH(connect_DWH_CoSo, 2):  # batch cuối ngày rồi
                    runDate = dt.datetime.strptime(runDateText, '%d/%m/%Y')
                    runDateString = runDate.strftime("%Y-%m-%d")
                    # Đồng bộ dữ liệu
                    SYNC(connect_DWH_CoSo, 'spvmr9004', runDateString, runDateString)
                    SYNC(connect_DWH_CoSo, 'spRMR0062', runDateString, runDateString)
                    SYNC(connect_DWH_CoSo, 'sprmr0015', runDateString, runDateString)
                    SYNC(connect_DWH_CoSo, 'sp230006_DanhSachTieuKhoan', runDateString, runDateString)
                    SYNC(connect_DWH_CoSo, 'sp230006_DanhSachChungKhoan', runDateString, runDateString)
                    SYNC(connect_DWH_CoSo, 'sprelationship', runDateString, runDateString)
                    SYNC(connect_DWH_CoSo, 'spmargin_outstanding', runDateString, runDateString)
                    SYNC(connect_DWH_CoSo, 'sptrading_record', runDateString, runDateString)
                    SYNC(connect_DWH_CoSo, 'spvpr0109', runDateString, runDateString)
                    SYNC(connect_DWH_CoSo, 'spvpr0108', runDateString, runDateString)
                    SYNC(connect_DWH_CoSo, 'spvcf0051', runDateString, runDateString)
                    EXEC(connect_DWH_CoSo, 'spDataHop', Date=runDateString)
                else:
                    # chưa batch mặc định lấy lùi 1 ngày làm việc
                    runDateText = BDATE(dt.datetime.strptime(runDateText, '%d/%m/%Y').strftime('%Y-%m-%d'), -1)
                    runDate = dt.datetime.strptime(runDateText, '%Y-%m-%d')
            else:  # Nếu chạy ngày trong quá khứ
                runDate = dt.datetime.strptime(runDateText, '%d/%m/%Y')

            fromDateROD0040 = dt.datetime.strptime(fromDateROD0040Text, '%d/%m/%Y')
            toDateROD0040 = dt.datetime.strptime(toDateROD0040Text, '%d/%m/%Y')
            # Sheet Summary Info
            dataSheetSummaryInfo = SheetSummaryInfo()
            dataSheetSummaryInfo.runDate = runDate
            dataSheetSummaryInfo.fromDateROD0040 = fromDateROD0040
            dataSheetSummaryInfo.toDateROD0040 = toDateROD0040
            tableSheetSummaryInfo = dataSheetSummaryInfo.result
            # Sheet Monthly Deal
            dataSheetMontlyDeal = SheetMonthlyDeal()
            dataSheetMontlyDeal.runDate = runDate
            dataSheetMontlyDeal.fromDateROD0040 = fromDateROD0040
            dataSheetMontlyDeal.toDateROD0040 = toDateROD0040
            tableSheetMontlyDeal = dataSheetMontlyDeal.result
            # warning list EOD
            warningListEOD = Liquidity3M()
            warningListEOD.runDate = runDate
            dataFromWarningRMDEOD = warningListEOD.result
            tableSheetMontlyDeal = tableSheetMontlyDeal.merge(
                dataFromWarningRMDEOD,
                how='left',
                on='Stock'
            )
            tableSheetMontlyDeal['3M Avg. Volume'].fillna(0, inplace=True)
            tableSheetMontlyDeal['P.OutsSetUp'] = tableSheetMontlyDeal['Setup'] * tableSheetMontlyDeal['MaxRatio'] * tableSheetMontlyDeal['MaxPrice']
            tableSheetMontlyDeal['MR_Ratio'] = tableSheetMontlyDeal['MR_Ratio'] * 100
            tableSheetMontlyDeal['DP_Ratio'] = tableSheetMontlyDeal['DP_Ratio'] * 100
            # write to Excel
            excelWriter = ExcelWriter(runDate)
            excelWriter.roomCodeSheet1 = roomCodeSheetSummaryText
            excelWriter.roomCodeSheet2 = roomCodeSheetMonthlyDealText
            excelWriter.dataSheet1 = tableSheetSummaryInfo
            excelWriter.dataSheet2 = tableSheetMontlyDeal
            self.__filePath = excelWriter.writeExcel()
            os.startfile(self.__filePath)

    @staticmethod
    def popUp(
            icon: QtWidgets.QMessageBox.Icon,
            message: str
    ):
        messageBox = QMessageBox()
        messageBox.setIcon(icon)
        messageBox.setWindowTitle('Thông báo')
        messageBox.setText(message)
        messageBox.exec()


if __name__ == "__main__":
    app = QApplication([])
    window = InputWindow()
    window.show()
    app.exec_()


r"""
BUILD
D:
cd D:\virtualenvDepartment\RM
venv_MonthlyDealReport\Scripts\activate
cd D:\DataAnalytics\automation\risk_management\MonthlyDealReport
pyinstaller gui.py -F --icon=phs_icon.ico -p D:\DataAnalytics -n AppMonthlyDeal --add-data phs_icon.ico;.

"""