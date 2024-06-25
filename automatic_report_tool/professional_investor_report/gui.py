import re
import sys
import datetime as dt
import os

from PyQt5 import QtWidgets
from PyQt5.QtWidgets import QApplication, QMessageBox, QPushButton, QWidget, QGridLayout
from PyQt5.QtGui import QFont, QIcon

from automation.trading_service.giaodichluuky.BaoCaoTrungBinhNAV.query import Query


class GUI(QWidget):

    def __init__(
        self,
    ):

        super().__init__()
        
        self.setFixedSize(260,120)
        self.setWindowTitle("BC NĐT Chuyên Nghiệp")

        # tạo layout
        layout = QGridLayout()
        layout.setContentsMargins(5,5,5,5)

        # Label: Đến ngày
        fromDateLabel = QtWidgets.QLabel('Từ Ngày: ')
        fromDateLabel.setFont(QFont('Times New Roman',12))
        layout.addWidget(fromDateLabel,0,0)
        # Box: Đến ngày
        fromDateBox = QtWidgets.QLineEdit()
        fromDateBox.setFont(QFont('Times New Roman',12))
        fromDateBox.setPlaceholderText('dd/mm/yyyy')
        fromDateBox.errorMessage = 'Ngày bắt đầu không hợp lệ.'
        layout.addWidget(fromDateBox,0,1)

        # Label: Đến ngày
        toDateLabel = QtWidgets.QLabel('Đến Ngày: ')
        toDateLabel.setFont(QFont('Times New Roman',12))
        layout.addWidget(toDateLabel,1,0)
        # Box: Đến ngày
        toDateBox = QtWidgets.QLineEdit()
        toDateBox.setFont(QFont('Times New Roman',12))
        toDateBox.setPlaceholderText('dd/mm/yyyy')
        toDateBox.errorMessage = 'Ngày kết thúc không hợp lệ.'
        layout.addWidget(toDateBox,1,1)

        # Label: Số TKLK
        accountCodeLabel = QtWidgets.QLabel('Số tài khoản lưu ký: ')
        accountCodeLabel.setFont(QFont('Times New Roman',12))
        layout.addWidget(accountCodeLabel,2,0)
        # Box: Số TKLK
        accountCodeBox = QtWidgets.QLineEdit()
        accountCodeBox.setFont(QFont('Times New Roman',12))
        accountCodeBox.setPlaceholderText('022...')
        accountCodeBox.errorMessage = 'Số tài khoản không hợp lệ.'
        layout.addWidget(accountCodeBox,2,1)

        # Button: OK
        okButton = QPushButton('OK')
        okButton.setFont(QFont('Times New Roman',12))
        okButton.clicked.connect(lambda: self.getData(fromDateBox,toDateBox,accountCodeBox))
        okButton.keyPressEvent
        layout.addWidget(okButton,3,0)

        # Button: Cancel
        cancelButton = QPushButton('Cancel')
        cancelButton.setFont(QFont('Times New Roman',12))
        cancelButton.clicked.connect(QtWidgets.qApp.quit)
        layout.addWidget(cancelButton,3,1)

        # Set Layout
        self.setLayout(layout)

    @classmethod
    def getData(
        cls,
        fromDateBox: QtWidgets.QLineEdit,
        toDateBox: QtWidgets.QLineEdit,
        accountCodeBox: QtWidgets.QLineEdit,
    ):  
        fromDate = cls.getDate(fromDateBox)
        toDate = cls.getDate(toDateBox)
        accountCode = cls.getAccountCode(accountCodeBox)
        # Kiểm tra điều kiện
        if fromDate > toDate:
            cls.popUpInvalidInput(QMessageBox.Warning,'Ngày bắt đầu lớn hơn ngày kết thúc.')
        if fromDate < dt.datetime(2022,4,19):
            cls.popUpInvalidInput(QMessageBox.Warning,'Không lưu dữ liệu trước 19/04/2022.')

        query = Query(fromDate,toDate,accountCode)
        query.execute()
        try:
            query.toExcel()
            cls.popUpInvalidInput(QMessageBox.Information,'Hoàn thành.')
        except (PermissionError,):
            cls.popUpInvalidInput(QMessageBox.Warning,'Báo cáo đang được mở.')
        else:
            os.startfile(query.filePath)

    @classmethod
    def getDate(
        cls,
        box: QtWidgets.QLineEdit
    ):
        rawDateString = box.text()
        dateString = re.sub(r'[^\d]','-',rawDateString)
        matchRegex = re.search(r'\d{2}-\d{2}-\d{4}',dateString)
        try:
            return dt.datetime.strptime(matchRegex.group(),'%d-%m-%Y')
        except (AttributeError,ValueError):
            cls.popUpInvalidInput(QMessageBox.Warning,box.errorMessage)

    @classmethod
    def getAccountCode(
        cls,
        box: QtWidgets.QLineEdit
    ):
        rawAccountCode = box.text()
        accountCode = re.sub(r'[^\w]','',rawAccountCode).upper() # cho phép 022c123456 thành 022C123456
        if accountCode.startswith('022') and len(accountCode) == 10:
            return accountCode
        cls.popUpInvalidInput(QMessageBox.Warning,box.errorMessage)
            
    @staticmethod
    def popUpInvalidInput(
        icon: QtWidgets.QMessageBox.Icon,
        message: str,
    ):
        messageBox = QMessageBox()
        messageBox.setIcon(icon)
        messageBox.setWindowIcon(QIcon('./icon/phs_icon2.ico'))
        messageBox.setWindowTitle('Thông báo')
        messageBox.setText(message)
        messageBox.exec()


if __name__ == '__main__':
    app = QApplication(sys.argv)
    gui = GUI()
    gui.show()
    sys.exit(app.exec())


r"""

cd D:\virtualenvDepartment\SS
NDTCN_venv\Scripts\activate

cd D:\DataAnalytics\automation\trading_service\giaodichluuky\BaoCaoTrungBinhNAV

pyinstaller gui.py -F --icon=./icon/phs_icon.ico -p D:\DataAnalytics -n NhaDauTuChuyenNghiep --add-data=./img/phs_logo.png;.
"""