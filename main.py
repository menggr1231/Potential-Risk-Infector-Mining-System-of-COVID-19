from PyQt5 import QtWidgets
from UI import Ui_Form
from location_gaode import convert_address
from PyQt5.QtWidgets import QFileDialog
import os
from trajectory_matching_COVID_space_time import trajectory_match

class mywindow(QtWidgets.QWidget, Ui_Form):
    def __init__(self):
        super(mywindow, self).__init__()
        self.setupUi(self)
        self.notice_txt.setText("Please select the confirmed patient database path")
        self.load_dataset_file.clicked.connect(self.read_database_file)  # Select DB path
        # Convert Chinese address to latitude and longitude and express time as hours starting at 00:00 on January 23
        self.load_dataset_start.clicked.connect(self.convert)
        # Read test samples and perform matching calculations
        self.load_test_button.clicked.connect(self.read_test_traj)
        # Clear all information
        self.clear_button.clicked.connect(self.clear)

    def read_database_file(self):  # Read database txt
        self.notice_txt.setText("Have read the selected database file")
        self.filename, _ = QFileDialog.getOpenFileName(self, "Select", "./", "All Files(*)")
        self.load_dataset_filepath.setText(self.filename)

    def convert(self):
        self.notice_txt.setText("Have loaded the confirmed patient database")
        # Convert Chinese address to latitude and longitude and express time as hours starting at 00:00 on January 23
        convert_address(self.filename, "data_jwd.txt")

    def read_test_traj(self):
        # Read the input test trajectory
        traj = self.test_traj_input.toPlainText()
        # Read jdk path
        jdk = self.jdk_path.text()

        # Write test samples to test.txt
        with open('test.txt', 'a', encoding='UTF-8') as file_handle:
            for i in range(len(traj.split("\n"))):
                file_handle.write(traj.split("\n")[i].split(" ")[0] + " 0000 " +
                                  traj.split("\n")[i].split(" ")[1] + " " +
                                  traj.split("\n")[i].split(" ")[2] + " " +
                                  traj.split("\n")[i].split(" ")[3] + " " +
                                  traj.split("\n")[i].split(" ")[4] + "\n")
        convert_address("test.txt", "test_jwd.txt")

        # Calculating the similarity of spatiotemporal trajectories
        value, result = trajectory_match("./data_jwd.txt", "./test_jwd.txt", "./data.txt", "./test.txt", jdk).match()

        # Get results
        self.notice_txt.setText("Please see the prediction results")
        prob = str(round(float(value), 2))
        self.risk_prob_show.setText(prob)

        if value >= 0.80:  # High risk, red indicator
            self.label.setStyleSheet("image: url(:/icon/hong.png);")
        elif value == 0:  # No risk, green indicator
            self.label.setStyleSheet("image: url(:/icon/lv.png);")
        else:  # Low or medium risk, yellow indicator
            self.label.setStyleSheet("image: url(:/icon/huang.png);")

        # Show risk trajectories
        if len(result) == 0:
            self.a1.setText("0")
            self.a2.setText("0")
            self.a3.setText("0")
            self.a4.setText("0")
            self.a5.setText("0")
        elif len(result) == 1:
            self.a1.setText(str(round(float(result[0][1]), 2)))
            self.b1.setText(str(result[0][0][1]))
            self.c1.setText(str(result[0][3][1]) + "-" + str(result[0][3][3]))
            self.d1.setText(str(result[0][3][0]) + " To " + str(result[0][3][2]))
            self.e1.setText(str(result[0][2][1]) + "-" + str(result[0][2][3]))
            self.f1.setText(str(result[0][2][0]) + " To " + str(result[0][2][2]))
            self.a2.setText("0")
            self.a3.setText("0")
            self.a4.setText("0")
            self.a5.setText("0")
        elif len(result) == 2:
            self.a1.setText(str(round(float(result[0][1]), 2)))
            self.b1.setText(str(result[0][0][1]))
            self.c1.setText(str(result[0][3][1]) + "-" + str(result[0][3][3]))
            self.d1.setText(str(result[0][3][0]) + " To " + str(result[0][3][2]))
            self.e1.setText(str(result[0][2][1]) + "-" + str(result[0][2][3]))
            self.f1.setText(str(result[0][2][0]) + " To " + str(result[0][2][2]))
            self.a2.setText(str(round(float(result[1][1]), 2)))
            self.b2.setText(str(result[1][0][1]))
            self.c2.setText(str(result[1][3][1]) + "-" + str(result[1][3][3]))
            self.d2.setText(str(result[1][3][0]) + " To " + str(result[1][3][2]))
            self.e2.setText(str(result[1][2][1]) + "-" + str(result[1][2][3]))
            self.f2.setText(str(result[1][2][0]) + " To " + str(result[1][2][2]))
            self.a3.setText("0")
            self.a4.setText("0")
            self.a5.setText("0")
        elif len(result) == 3:
            self.a1.setText(str(round(float(result[0][1]), 2)))
            self.b1.setText(str(result[0][0][1]))
            self.c1.setText(str(result[0][3][1]) + "-" + str(result[0][3][3]))
            self.d1.setText(str(result[0][3][0]) + " To " + str(result[0][3][2]))
            self.e1.setText(str(result[0][2][1]) + "-" + str(result[0][2][3]))
            self.f1.setText(str(result[0][2][0]) + " To " + str(result[0][2][2]))
            self.a2.setText(str(round(float(result[1][1]), 2)))
            self.b2.setText(str(result[1][0][1]))
            self.c2.setText(str(result[1][3][1]) + "-" + str(result[1][3][3]))
            self.d2.setText(str(result[1][3][0]) + " To " + str(result[1][3][2]))
            self.e2.setText(str(result[1][2][1]) + "-" + str(result[1][2][3]))
            self.f2.setText(str(result[1][2][0]) + " To " + str(result[1][2][2]))
            self.a3.setText(str(round(float(result[2][1]), 2)))
            self.b3.setText(str(result[2][0][1]))
            self.c3.setText(str(result[2][3][1]) + "-" + str(result[2][3][3]))
            self.d3.setText(str(result[2][3][0]) + " To " + str(result[2][3][2]))
            self.e3.setText(str(result[2][2][1]) + "-" + str(result[2][2][3]))
            self.f3.setText(str(result[2][2][0]) + " To " + str(result[2][2][2]))
            self.a4.setText("0")
            self.a5.setText("0")
        elif len(result) == 4:
            self.a1.setText(str(round(float(result[0][1]), 2)))
            self.b1.setText(str(result[0][0][1]))
            self.c1.setText(str(result[0][3][1]) + "-" + str(result[0][3][3]))
            self.d1.setText(str(result[0][3][0]) + " To " + str(result[0][3][2]))
            self.e1.setText(str(result[0][2][1]) + "-" + str(result[0][2][3]))
            self.f1.setText(str(result[0][2][0]) + " To " + str(result[0][2][2]))
            self.a2.setText(str(round(float(result[1][1]), 2)))
            self.b2.setText(str(result[1][0][1]))
            self.c2.setText(str(result[1][3][1]) + "-" + str(result[1][3][3]))
            self.d2.setText(str(result[1][3][0]) + " To " + str(result[1][3][2]))
            self.e2.setText(str(result[1][2][1]) + "-" + str(result[1][2][3]))
            self.f2.setText(str(result[1][2][0]) + " To " + str(result[1][2][2]))
            self.a3.setText(str(round(float(result[2][1]), 2)))
            self.b3.setText(str(result[2][0][1]))
            self.c3.setText(str(result[2][3][1]) + "-" + str(result[2][3][3]))
            self.d3.setText(str(result[2][3][0]) + " To " + str(result[2][3][2]))
            self.e3.setText(str(result[2][2][1]) + "-" + str(result[2][2][3]))
            self.f3.setText(str(result[2][2][0]) + " To " + str(result[2][2][2]))
            self.a4.setText(str(round(float(result[3][1]), 2)))
            self.b4.setText(str(result[3][0][1]))
            self.c4.setText(str(result[3][3][1]) + "-" + str(result[3][3][3]))
            self.d4.setText(str(result[3][3][0]) + " To " + str(result[3][3][2]))
            self.e4.setText(str(result[3][2][1]) + "-" + str(result[3][2][3]))
            self.f4.setText(str(result[3][2][0]) + " To " + str(result[3][2][2]))
            self.a5.setText("0")
        elif len(result) >= 5:
            self.a1.setText(str(round(float(result[0][1]), 2)))
            self.b1.setText(str(result[0][0][1]))
            self.c1.setText(str(result[0][3][1]) + "-" + str(result[0][3][3]))
            self.d1.setText(str(result[0][3][0]) + " To " + str(result[0][3][2]))
            self.e1.setText(str(result[0][2][1]) + "-" + str(result[0][2][3]))
            self.f1.setText(str(result[0][2][0]) + " To " + str(result[0][2][2]))
            self.a2.setText(str(round(float(result[1][1]), 2)))
            self.b2.setText(str(result[1][0][1]))
            self.c2.setText(str(result[1][3][1]) + "-" + str(result[1][3][3]))
            self.d2.setText(str(result[1][3][0]) + " To " + str(result[1][3][2]))
            self.e2.setText(str(result[1][2][1]) + "-" + str(result[1][2][3]))
            self.f2.setText(str(result[1][2][0]) + " To " + str(result[1][2][2]))
            self.a3.setText(str(round(float(result[2][1]), 2)))
            self.b3.setText(str(result[2][0][1]))
            self.c3.setText(str(result[2][3][1]) + "-" + str(result[2][3][3]))
            self.d3.setText(str(result[2][3][0]) + " To " + str(result[2][3][2]))
            self.e3.setText(str(result[2][2][1]) + "-" + str(result[2][2][3]))
            self.f3.setText(str(result[2][2][0]) + " To " + str(result[2][2][2]))
            self.a4.setText(str(round(float(result[3][1]), 2)))
            self.b4.setText(str(result[3][0][1]))
            self.c4.setText(str(result[3][3][1]) + "-" + str(result[3][3][3]))
            self.d4.setText(str(result[3][3][0]) + " To " + str(result[3][3][2]))
            self.e4.setText(str(result[3][2][1]) + "-" + str(result[3][2][3]))
            self.f4.setText(str(result[3][2][0]) + " To " + str(result[3][2][2]))
            self.a5.setText(str(round(float(result[4][1]), 2)))
            self.b5.setText(str(result[4][0][1]))
            self.c5.setText(str(result[4][3][1]) + "-" + str(result[4][3][3]))
            self.d5.setText(str(result[4][3][0]) + " To " + str(result[4][3][2]))
            self.e5.setText(str(result[4][2][1]) + "-" + str(result[4][2][3]))
            self.f5.setText(str(result[4][2][0]) + " To " + str(result[4][2][2]))

        # Delete intermediate files
        os.remove("data_jwd.txt")
        os.remove("test.txt")
        os.remove("test_jwd.txt")

    def clear(self):  # Clear all
        self.load_dataset_filepath.clear()
        self.test_traj_input.clear()
        self.risk_prob_show.clear()
        self.a1.clear()
        self.a2.clear()
        self.a3.clear()
        self.a4.clear()
        self.a5.clear()
        self.b1.clear()
        self.b2.clear()
        self.b3.clear()
        self.b4.clear()
        self.b5.clear()
        self.c1.clear()
        self.c2.clear()
        self.c3.clear()
        self.c4.clear()
        self.c5.clear()
        self.d1.clear()
        self.d2.clear()
        self.d3.clear()
        self.d4.clear()
        self.d5.clear()
        self.e1.clear()
        self.e2.clear()
        self.e3.clear()
        self.e4.clear()
        self.e5.clear()
        self.f1.clear()
        self.f2.clear()
        self.f3.clear()
        self.f4.clear()
        self.f5.clear()
        self.notice_txt.setText("All information cleared")


if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    ui = mywindow()
    ui.show()
    sys.exit(app.exec_())


