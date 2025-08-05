#!/usr/bin/env python3
"""
MySQL Sync Agent GUI - Main Entry Point
"""

import sys
import os
from PySide6.QtWidgets import QApplication
from PySide6.QtCore import QDir
from PySide6.QtGui import QIcon

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from gui.main_window import MainWindow
from gui.utils.startup_manager import StartupManager


def main():
    """Main entry point for the GUI application"""
    app = QApplication(sys.argv)

    # Set application properties
    app.setApplicationName("MySQL Sync Agent")
    app.setApplicationVersion("1.0.0")
    app.setOrganizationName("SyncAgent")
    app.setOrganizationDomain("syncagent.local")

    # Set application icon
    app_icon = QIcon()
    # You can add different sizes of icons here
    # app_icon.addFile("icons/16x16.png", QSize(16, 16))
    # app_icon.addFile("icons/32x32.png", QSize(32, 32))
    # app_icon.addFile("icons/64x64.png", QSize(64, 64))
    app.setWindowIcon(app_icon)

    # Prevent the application from quitting when the last window is closed
    # This allows the app to run in system tray
    app.setQuitOnLastWindowClosed(False)

    # Create and show main window
    main_window = MainWindow()
    main_window.show()

    # Handle startup manager
    startup_manager = StartupManager()
    if not startup_manager.is_in_startup():
        if main_window.ask_add_to_startup():
            startup_manager.add_to_startup()

    return app.exec()


if __name__ == "__main__":
    sys.exit(main())