from PySide6.QtWidgets import QSystemTrayIcon, QMenu
from PySide6.QtGui import QAction, QIcon
from PySide6.QtCore import Slot


class SystemTrayIcon(QSystemTrayIcon):
    def __init__(self, window, icon_path=None):
        icon = QIcon(icon_path) if icon_path else window.windowIcon()
        super().__init__(icon, window)

        self.window = window
        self.setToolTip("DB Sync Agent")
        self.setVisible(True)

        menu = QMenu()

        show_action = QAction("Show")
        hide_action = QAction("Hide")
        exit_action = QAction("Exit")

        show_action.triggered.connect(self.show_window)
        hide_action.triggered.connect(self.hide_window)
        exit_action.triggered.connect(self.exit_app)

        menu.addAction(show_action)
        menu.addAction(hide_action)
        menu.addSeparator()
        menu.addAction(exit_action)

        self.setContextMenu(menu)
        self.setVisible(True)

    @Slot()
    def show_window(self):
        self.window.showNormal()
        self.window.raise_()
        self.window.activateWindow()

    @Slot()
    def hide_window(self):
        self.window.hide()

    @Slot()
    def exit_app(self):
        self.window.close()
