from PySide6.QtWidgets import (QMainWindow, QWidget, QVBoxLayout,
                               QHBoxLayout, QPushButton, QSystemTrayIcon, QMenu, QGroupBox, QDialog,
                               QApplication)
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QIcon
from .components.sync_controls import SyncControlPanel
from .components.log_viewer import LogViewer
from .components.config_editor import ConfigEditor
from .utils.startup_manager import StartupManager
from .utils.config_manager import load_gui_config, convert_config_for_core
from .utils.detailed_logger import logger
from core.runner import start_sync, stop_sync, run_one_time_sync
from core.connector import connect_mysql
import logging

class MainWindow(QMainWindow):
    closing = Signal()

    def __init__(self):
        super().__init__()
        # Initialize logger
        from .utils.detailed_logger import logger
        self.logger = logger

        self.setWindowTitle("DB Sync Agent")
        self.setMinimumSize(600, 400)  # Reduced window size

        # Initialize system tray
        self.tray_icon = QSystemTrayIcon(self)
        self.setup_tray_icon()

        # Initialize startup manager
        self.startup_manager = StartupManager()

        # Load initial configuration
        self.current_config = self.load_initial_config()

        # Setup UI
        self.setup_ui()

        # Add to startup by default
        self.startup_manager.add_to_startup()

    def load_initial_config(self):
        """Load config with defaults if needed"""
        config = load_gui_config()
        if not config.get('sync_pairs'):
            config['sync_pairs'] = [{
                'name': 'default',
                'local': {
                    'host': 'localhost',
                    'port': 3306,
                    'user': '',
                    'password': '',
                    'db': ''
                },
                'cloud': {
                    'host': 'localhost',
                    'port': 3306,
                    'user': '',
                    'password': '',
                    'db': ''
                }
            }]
        return config

    def setup_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)

        # Add sync control panel in horizontal layout
        control_layout = QHBoxLayout()
        self.sync_controls = SyncControlPanel()

        # Connect sync control signals to actual operations
        self.sync_controls.start_sync.connect(self.handle_start_sync)
        self.sync_controls.stop_sync.connect(self.handle_stop_sync)
        self.sync_controls.manual_sync.connect(self.handle_manual_sync)

        control_layout.addWidget(self.sync_controls)
        layout.addLayout(control_layout)

        # Add log viewer
        log_group = QGroupBox("Log Viewer")
        log_layout = QVBoxLayout(log_group)
        self.log_viewer = LogViewer()
        log_layout.addWidget(self.log_viewer)
        layout.addWidget(log_group)

        # Add settings button at the bottom
        settings_btn = QPushButton("Settings")
        settings_btn.clicked.connect(self.show_settings)
        layout.addWidget(settings_btn)

    def setup_tray_icon(self):
        # Create tray icon menu
        tray_menu = QMenu()
        show_action = tray_menu.addAction("Show")
        show_action.triggered.connect(self.show)
        quit_action = tray_menu.addAction("Quit")
        quit_action.triggered.connect(self.quit_application)

        # Setup tray icon
        self.tray_icon.setContextMenu(tray_menu)
        self.tray_icon.setIcon(QIcon("assets/icon.ico"))
        self.tray_icon.setToolTip("DB Sync Agent")
        self.tray_icon.activated.connect(self.tray_icon_activated)
        self.tray_icon.show()

    def closeEvent(self, event):
        if self.tray_icon.isVisible():
            self.hide()
            event.ignore()
        else:
            self.closing.emit()
            event.accept()

    def tray_icon_activated(self, reason):
        if reason == QSystemTrayIcon.ActivationReason.Trigger:
            if self.isVisible():
                self.hide()
            else:
                self.show()
                self.activateWindow()

    def get_current_config(self):
        return self.current_config

    def show_settings(self):
        from .components.password_dialog import PasswordDialog

        # Check if this is first run and password needs to be set up
        if PasswordDialog.needs_setup():
            setup_dialog = PasswordDialog(self, is_setup=True)
            if setup_dialog.exec() != QDialog.DialogCode.Accepted:
                return

        # Show password dialog
        pwd_dialog = PasswordDialog(self)
        if pwd_dialog.exec() != QDialog.DialogCode.Accepted:
            return

        # If password is correct, show settings
        self.config_editor = ConfigEditor(self)
        self.config_editor.configSaved.connect(self.on_config_saved)
        # Load initial configuration
        self.config_editor.load_config()
        self.config_editor.exec()  # Use exec() instead of show() for modal dialog

    def handle_manual_sync(self):
        try:
            self.logger.log_sync_operation(
                'Manual Sync',
                {'status': 'starting'},
            )
            gui_config = load_gui_config()
            core_config = convert_config_for_core(gui_config)

            # First manage triggers to ensure they are up to date
            self.manage_triggers(core_config)

            # Log sync configuration
            self.logger.log_sync_operation(
                'Manual Sync Configuration',
                {
                    'node_id': core_config["node_id"],
                    'sync_pairs': [
                        {
                            'name': pair['name'],
                            'local_db': pair['local']['db'],
                            'cloud_db': pair['cloud']['db']
                        }
                        for pair in core_config['sync_pairs']
                    ],
                    'tables': core_config['sync']['tables']
                }
            )

            run_one_time_sync(core_config, core_config["node_id"])
            self.logger.log_sync_operation(
                'Manual Sync',
                {'status': 'completed'}
            )
        except Exception as e:
            self.logger.log_sync_operation(
                'Manual Sync',
                {'status': 'failed'},
                error=str(e)
            )
        finally:
            self.sync_controls.manual_sync_button.setEnabled(True)

    def handle_start_sync(self):
        try:
            self.logger.log_sync_operation(
                'Start Scheduled Sync',
                {'status': 'starting'}
            )
            gui_config = load_gui_config()
            core_config = convert_config_for_core(gui_config)

            # Log scheduler configuration
            self.logger.log_sync_operation(
                'Scheduler Configuration',
                {
                    'interval': gui_config['sync'].get('interval', 300),
                    'enabled': gui_config['sync'].get('enabled', True),
                    'tables': core_config['sync']['tables']
                }
            )

            start_sync()
            self.logger.log_sync_operation(
                'Start Scheduled Sync',
                {'status': 'success'}
            )
        except Exception as e:
            self.logger.log_sync_operation(
                'Start Scheduled Sync',
                {'status': 'failed'},
                error=str(e)
            )
            self.sync_controls.on_stop()  # Reset UI state

    def handle_stop_sync(self):
        """Handle stop sync button click"""
        try:
            self.logger.log_sync_operation(
                'Stop Scheduled Sync',
                {'status': 'starting'}
            )

            # Call the core stop_sync function
            stop_sync()

            self.logger.log_sync_operation(
                'Stop Scheduled Sync',
                {'status': 'success'}
            )
        except Exception as e:
            self.logger.log_sync_operation(
                'Stop Scheduled Sync',
                {'status': 'failed'},
                error=str(e)
            )
            # Reset UI state in case of error
            self.sync_controls.start_button.setEnabled(True)
            self.sync_controls.stop_button.setEnabled(False)
            self.sync_controls.manual_sync_button.setEnabled(True)

    def on_config_saved(self, new_config):
        """Log configuration changes and update triggers"""
        try:
            self.current_config = new_config
            # Log configuration update
            self.logger.log_sync_operation(
                'Configuration Update',
                {
                    'sync_pairs': [
                        {
                            'name': pair['name'],
                            'local_db': pair['local']['db'],
                            'cloud_db': pair['cloud']['db']
                        }
                        for pair in new_config['sync_pairs']
                    ],
                    'sync_interval': new_config['sync'].get('interval'),
                    'sync_enabled': new_config['sync'].get('enabled'),
                    'table_count': len(new_config['sync'].get('tables', {}))
                }
            )

            # Save the updated configuration
            from .utils.config_manager import save_gui_config
            save_gui_config(new_config)

            # Update triggers based on new configuration
            self.manage_triggers(new_config)

        except Exception as e:
            self.logger.log_sync_operation(
                'Configuration Update',
                {'status': 'failed'},
                error=str(e)
            )

    def quit_application(self):
        """Properly close the application and clean up"""
        self.closing.emit()  # Emit closing signal for cleanup
        self.tray_icon.hide()  # Hide tray icon
        self.close()  # Close main window
        QApplication.quit()  # Quit application

    def manage_triggers(self, config=None):
        """Manage database triggers - drop existing and create new ones"""
        if not config:
            config = load_gui_config()

        core_config = convert_config_for_core(config)

        try:
            self.logger.log_sync_operation(
                'Trigger Management',
                {'status': 'starting', 'action': 'recreating triggers'}
            )

            # First drop all existing triggers
            self._drop_all_triggers(core_config)

            # Then recreate triggers for selected tables
            self._setup_new_triggers(core_config)

            self.logger.log_sync_operation(
                'Trigger Management',
                {'status': 'completed', 'action': 'triggers recreated'}
            )
        except Exception as e:
            self.logger.log_sync_operation(
                'Trigger Management',
                {'status': 'failed'},
                error=str(e)
            )
            raise

    def _drop_all_triggers(self, config):
        """Drop all existing triggers from sync pairs"""
        for pair in config["sync_pairs"]:
            self.logger.log_sync_operation(
                'Drop Triggers',
                {'sync_pair': pair['name'], 'status': 'starting'}
            )

            for db_type in ["local", "cloud"]:
                try:
                    conn = connect_mysql(pair[db_type])
                    db_name = pair[db_type]["db"]

                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE
                            FROM INFORMATION_SCHEMA.TRIGGERS
                            WHERE TRIGGER_SCHEMA = %s
                            AND TRIGGER_NAME LIKE 'trg_%%'
                        """, (db_name,))

                        triggers = cur.fetchall()

                        for trig in triggers:
                            drop_sql = f"DROP TRIGGER IF EXISTS `{trig['TRIGGER_NAME']}`"
                            cur.execute(drop_sql)
                            self.logger.log_sync_operation(
                                'Drop Trigger',
                                {
                                    'sync_pair': pair['name'],
                                    'database': db_type,
                                    'trigger': trig['TRIGGER_NAME'],
                                    'table': trig['EVENT_OBJECT_TABLE']
                                }
                            )

                    conn.close()
                except Exception as e:
                    self.logger.log_sync_operation(
                        'Drop Triggers',
                        {'sync_pair': pair['name'], 'database': db_type, 'status': 'failed'},
                        error=str(e)
                    )
                    raise

    def _setup_new_triggers(self, config):
        """Set up new triggers for all sync pairs"""
        from core.schema import ensure_change_log_table, setup_triggers

        for pair in config["sync_pairs"]:
            self.logger.log_sync_operation(
                'Setup Triggers',
                {'sync_pair': pair['name'], 'status': 'starting'}
            )

            local_node_id = self._generate_database_node_id(pair["name"], "local")
            cloud_node_id = self._generate_database_node_id(pair["name"], "cloud")

            # Get tables from sync configuration or use 'all'
            tables = config.get('sync', {}).get('tables', 'all')
            if not tables:  # If tables is empty dict or None
                tables = 'all'

            # Setup local database triggers
            try:
                local_conn = connect_mysql(pair["local"])
                local_db = pair["local"]["db"]

                ensure_change_log_table(local_conn)
                setup_triggers(local_conn, local_db, tables, local_node_id)

                local_conn.close()

                self.logger.log_sync_operation(
                    'Setup Triggers',
                    {
                        'sync_pair': pair['name'],
                        'database': 'local',
                        'node_id': local_node_id,
                        'tables': str(tables),
                        'status': 'completed'
                    }
                )
            except Exception as e:
                self.logger.log_sync_operation(
                    'Setup Triggers',
                    {'sync_pair': pair['name'], 'database': 'local', 'status': 'failed'},
                    error=str(e)
                )
                raise

            # Setup cloud database triggers
            try:
                cloud_conn = connect_mysql(pair["cloud"])
                cloud_db = pair["cloud"]["db"]

                ensure_change_log_table(cloud_conn)
                setup_triggers(cloud_conn, cloud_db, tables, cloud_node_id)

                cloud_conn.close()

                self.logger.log_sync_operation(
                    'Setup Triggers',
                    {
                        'sync_pair': pair['name'],
                        'database': 'cloud',
                        'node_id': cloud_node_id,
                        'tables': str(tables),
                        'status': 'completed'
                    }
                )
            except Exception as e:
                self.logger.log_sync_operation(
                    'Setup Triggers',
                    {'sync_pair': pair['name'], 'database': 'cloud', 'status': 'failed'},
                    error=str(e)
                )
                raise

    def _generate_database_node_id(self, sync_pair_name: str, db_type: str) -> str:
        """Generate a unique node ID for each database in the sync pair"""
        import uuid
        base_string = f"{sync_pair_name}_{db_type}"
        return uuid.uuid5(uuid.NAMESPACE_DNS, base_string).hex
