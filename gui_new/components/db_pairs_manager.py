from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                              QScrollArea, QGroupBox, QFormLayout, QLineEdit,
                              QSpinBox, QTabWidget, QMessageBox)
from .db_pair_selector import DBPairSelector
from core.connector import connect_mysql
import logging

class DatabasePairWidget(QWidget):
    def __init__(self, name="", parent=None):
        super().__init__(parent)
        self.logger = logging.getLogger(__name__)
        self.name = name
        self.setup_ui()
        self.last_valid_name = name  # Keep track of last valid name

    def setup_ui(self):
        layout = QVBoxLayout(self)

        # Add pair name field at the top
        name_layout = QHBoxLayout()
        name_group = QGroupBox("Database Pair Name")
        name_form = QFormLayout()
        self.pair_name = QLineEdit(self.name)
        self.pair_name.setPlaceholderText("Enter a unique name for this database pair")
        self.pair_name.textChanged.connect(self.on_name_changed)
        name_form.addRow("Name:", self.pair_name)
        name_group.setLayout(name_form)
        name_layout.addWidget(name_group)
        layout.addLayout(name_layout)

        # Create horizontal layout for database settings
        db_layout = QHBoxLayout()

        # Local (Source) database settings
        local_group = QGroupBox("Local Database")
        local_form = QFormLayout()

        self.local_host = QLineEdit()
        self.local_port = QSpinBox()
        self.local_port.setMaximum(65535)
        self.local_port.setValue(3306)
        self.local_user = QLineEdit()
        self.local_password = QLineEdit()
        self.local_password.setEchoMode(QLineEdit.EchoMode.Password)
        self.local_database = QLineEdit()

        local_form.addRow("Host:", self.local_host)
        local_form.addRow("Port:", self.local_port)
        local_form.addRow("User:", self.local_user)
        local_form.addRow("Password:", self.local_password)
        local_form.addRow("Database:", self.local_database)
        local_group.setLayout(local_form)
        db_layout.addWidget(local_group)

        # Add some spacing between the groups
        db_layout.addSpacing(20)

        # Cloud (Target) database settings
        cloud_group = QGroupBox("Cloud Database")
        cloud_form = QFormLayout()

        self.cloud_host = QLineEdit()
        self.cloud_port = QSpinBox()
        self.cloud_port.setMaximum(65535)
        self.cloud_port.setValue(3306)
        self.cloud_user = QLineEdit()
        self.cloud_password = QLineEdit()
        self.cloud_password.setEchoMode(QLineEdit.EchoMode.Password)
        self.cloud_database = QLineEdit()

        cloud_form.addRow("Host:", self.cloud_host)
        cloud_form.addRow("Port:", self.cloud_port)
        cloud_form.addRow("User:", self.cloud_user)
        cloud_form.addRow("Password:", self.cloud_password)
        cloud_form.addRow("Database:", self.cloud_database)
        cloud_group.setLayout(cloud_form)
        db_layout.addWidget(cloud_group)

        # Add database settings to main layout
        layout.addLayout(db_layout)

        # Table selector
        self.table_selector = DBPairSelector(self)
        layout.addWidget(self.table_selector)

        # Action buttons in horizontal layout
        button_layout = QHBoxLayout()

        # Test connection button
        test_btn = QPushButton("Test Connection & Load Tables")
        test_btn.clicked.connect(self.test_connection_and_load_tables)
        button_layout.addWidget(test_btn)

        layout.addLayout(button_layout)

    def get_config(self):
        """Get the configuration for this database pair"""
        return {
            'name': self.pair_name.text() or self.name,  # Use text field value, fallback to initial name
            'local': {
                'host': self.local_host.text(),
                'port': self.local_port.value(),
                'user': self.local_user.text(),
                'password': self.local_password.text(),
                'db': self.local_database.text()
            },
            'cloud': {
                'host': self.cloud_host.text(),
                'port': self.cloud_port.value(),
                'user': self.cloud_user.text(),
                'password': self.cloud_password.text(),
                'db': self.cloud_database.text()
            },
            'tables': self.table_selector.get_sync_config(),
            'tables_config': {
                'enabled_tables': [],
                'default_direction': 'bidirectional'
            }
        }

    def set_config(self, config):
        """Set the configuration for this database pair"""
        if config:
            self.pair_name.setText(config.get('name', ''))

            local = config.get('local', {})
            self.local_host.setText(local.get('host', ''))
            self.local_port.setValue(local.get('port', 3306))
            self.local_user.setText(local.get('user', ''))
            self.local_password.setText(local.get('password', ''))
            self.local_database.setText(local.get('db', ''))

            cloud = config.get('cloud', {})
            self.cloud_host.setText(cloud.get('host', ''))
            self.cloud_port.setValue(cloud.get('port', 3306))
            self.cloud_user.setText(cloud.get('user', ''))
            self.cloud_password.setText(cloud.get('password', ''))
            self.cloud_database.setText(cloud.get('db', ''))

    def test_connection_and_load_tables(self):
        try:
            # Test local connection
            local_config = {
                'host': self.local_host.text(),
                'port': self.local_port.value(),
                'user': self.local_user.text(),
                'password': self.local_password.text(),
                'db': self.local_database.text(),
                'database': self.local_database.text()
            }
            local_conn = connect_mysql(local_config)
            local_conn.close()

            # Test cloud connection
            cloud_config = {
                'host': self.cloud_host.text(),
                'port': self.cloud_port.value(),
                'user': self.cloud_user.text(),
                'password': self.cloud_password.text(),
                'db': self.cloud_database.text(),
                'database': self.cloud_database.text()
            }
            cloud_conn = connect_mysql(cloud_config)
            cloud_conn.close()

            # Load tables
            config = self.get_config()
            self.table_selector.load_tables({'sync_pairs': [config]})
            QMessageBox.information(self, "Success", "Database connections successful. Tables loaded.")

        except Exception as e:
            QMessageBox.critical(self, "Connection Error", f"Failed to connect to database: {str(e)}")

    def on_name_changed(self, new_name):
        """Handle name changes and update tab title"""
        if self.validate_name(new_name):
            self.last_valid_name = new_name
            # Find the tab widget and update the title
            parent = self.parent()
            while parent and not isinstance(parent, QTabWidget):
                parent = parent.parent()
            if parent:
                index = parent.indexOf(self)
                if index != -1:
                    parent.setTabText(index, new_name if new_name else "Unnamed Pair")

    def validate_name(self, name):
        """Check if the name is valid"""
        if not name:
            return False

        # Find the DBPairsManager instance
        parent = self.parent()
        while parent and not isinstance(parent, DBPairsManager):
            parent = parent.parent()

        if parent:
            return parent.is_name_unique(name, self)
        return True

class DBPairsManager(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.pairs = []
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout(self)

        # Create tab widget for database pairs
        self.tab_widget = QTabWidget()
        self.tab_widget.setTabsClosable(True)
        self.tab_widget.tabCloseRequested.connect(self.remove_pair)
        layout.addWidget(self.tab_widget)

        # Add button for new pair
        add_btn = QPushButton("Add Database Pair")
        add_btn.clicked.connect(self.add_pair)
        layout.addWidget(add_btn)

    def is_name_unique(self, name, exclude_widget=None):
        """Check if a pair name is unique"""
        for pair in self.pairs:
            if pair != exclude_widget and pair.pair_name.text() == name:
                QMessageBox.warning(
                    self,
                    "Invalid Name",
                    f"The name '{name}' is already in use. Please choose a unique name."
                )
                return False
        return True

    def add_pair(self):
        """Add a new database pair tab"""
        pair_num = self.tab_widget.count() + 1
        default_name = f"Pair {pair_num}"

        # Ensure the default name is unique
        counter = 1
        while not self.is_name_unique(default_name):
            counter += 1
            default_name = f"Pair {pair_num}_{counter}"

        pair = DatabasePairWidget(default_name)
        self.tab_widget.addTab(pair, default_name)
        self.pairs.append(pair)

    def remove_pair(self, index):
        """Remove a database pair tab"""
        if self.tab_widget.count() > 1:  # Keep at least one pair
            # Get the name for logging
            removed_name = self.pairs[index].pair_name.text()

            self.tab_widget.removeTab(index)
            self.pairs.pop(index)

            # Log the removal
            self.logger.info(f"Removed database pair: {removed_name}")
        else:
            QMessageBox.warning(self, "Warning", "Cannot remove the last database pair.")

    def get_all_configs(self):
        """Get configurations for all database pairs"""
        return [pair.get_config() for pair in self.pairs]

    def set_all_configs(self, configs):
        """Load configurations for all database pairs"""
        # Clear existing pairs
        while self.tab_widget.count() > 0:
            self.tab_widget.removeTab(0)
        self.pairs.clear()

        # Add pairs from config
        for i, config in enumerate(configs):
            pair = DatabasePairWidget(config.get('name', f'Pair {i+1}'))
            pair.set_config(config)
            self.tab_widget.addTab(pair, f"Database Pair {i+1}")
            self.pairs.append(pair)

        # Ensure at least one pair exists
        if not configs:
            self.add_pair()
