import sys
import os
from PySide6.QtWidgets import QApplication
from PySide6.QtGui import QIcon
import logging
from .main_window import MainWindow

def setup_logging():
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Create file handler
    file_handler = logging.FileHandler('logs/app.log')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    root_logger.addHandler(file_handler)

def main():
    # Setup logging first
    setup_logging()

    # Create the application
    app = QApplication(sys.argv)
    app.setApplicationName("DB Sync Agent")
    app.setWindowIcon(QIcon("assets/icon.ico"))

    # Create and show the main window
    window = MainWindow()
    window.show()

    # Start the event loop
    sys.exit(app.exec_())
