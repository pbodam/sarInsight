import pyqtgraph as pg
import os 

from PyQt5.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QPushButton,
    QFileDialog,
    QLabel,
    QComboBox
)

from cpu_module import get_cpu_data
from memory_module import get_memory_data
from disk_module import get_disk_data


class SarInsightGUI(QWidget):

    def __init__(self):

        super().__init__()

        self.setWindowTitle("sarInsight")
        self.resize(1000, 900)

        layout = QVBoxLayout()

        # Timezone selection
        self.tz_label = QLabel("Display Timezone")

        self.tz_select = QComboBox()
        self.tz_select.addItems([
            "UTC",
            "Asia/Kolkata",
            "Europe/London",
            "America/New_York"
        ])

        # Load SAR file button
        self.load_button = QPushButton("Load SAR File")
        self.load_button.clicked.connect(self.load_sar)

        # Graph widgets
        self.cpu_plot = pg.PlotWidget(title="CPU Usage")
        self.mem_plot = pg.PlotWidget(title="Memory Usage")
        self.disk_plot = pg.PlotWidget(title="Disk Latency (await)")

        # Add widgets
        layout.addWidget(self.tz_label)
        layout.addWidget(self.tz_select)
        layout.addWidget(self.load_button)

        layout.addWidget(self.cpu_plot)
        layout.addWidget(self.mem_plot)
        layout.addWidget(self.disk_plot)

        self.setLayout(layout)

    def load_sar(self):

        base_dir = os.path.dirname(os.path.abspath(__file__))
        sa_dir = os.path.join(base_dir, "sa")
        file,_ = QFileDialog.getOpenFileName(self, "Select SAR file", sa_dir )

        if not file:
            return

        tz = self.tz_select.currentText()

        # Load data
        cpu = get_cpu_data(file, "UTC", tz)
        mem = get_memory_data(file, "UTC", tz)
        disk = get_disk_data(file, "UTC", tz)

        # Clear plots
        self.cpu_plot.clear()
        self.mem_plot.clear()
        self.disk_plot.clear()

        # CPU
        self.cpu_plot.plot(cpu["user"].values, pen="r", name="user")
        self.cpu_plot.plot(cpu["system"].values, pen="b", name="system")
        self.cpu_plot.plot(cpu["iowait"].values, pen="y", name="iowait")

        # Memory (used and free, in %)
        self.mem_plot.plot(mem["kbmemused"].values, pen="g", name="Used (%)")
        self.mem_plot.plot(mem["kbmemfree"].values, pen="c", name="Free (%)")

        # Disk latency
        self.disk_plot.plot(disk["await"].values, pen="m")