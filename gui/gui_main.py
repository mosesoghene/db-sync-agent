import sys
from PySide6.QtWidgets import QApplication

from gui.utils.logger import setup_logger
from gui.views.main_window import MainWindow


def main():
    app = QApplication(sys.argv)
    app.setQuitOnLastWindowClosed(False)

    # Setup both GUI and file logging
    setup_logger()

    window = MainWindow()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
