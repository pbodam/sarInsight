import sys
from PyQt5.QtWidgets import QApplication
from gui import SarInsightGUI

app = QApplication(sys.argv)

window = SarInsightGUI()
window.show()

sys.exit(app.exec_())