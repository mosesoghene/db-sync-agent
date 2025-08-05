import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
import json
import csv
import re
from typing import List, Dict
from PySide6.QtCore import QObject, Signal

class LogEvent:
    def __init__(self, timestamp, level, message, details=None):
        self.timestamp = timestamp
        self.level = level
        self.message = message
        self.details = details or {}

class LogMonitor(QObject):
    new_log_event = Signal(LogEvent)  # Signal emitted when new log entry is added
    log_cleared = Signal()  # Signal emitted when logs are cleared

class DetailedLogger:
    def __init__(self):
        self.monitor = LogMonitor()
        self.retention_days = 30  # Default retention period
        self.max_log_size = 10 * 1024 * 1024  # 10MB default max size
        self.log_filters = {
            'level': None,  # None means all levels
            'operation': None,  # None means all operations
            'status': None,  # None means all statuses
            'date_range': None,  # None means all dates
            'keyword': None,  # None means no keyword filter
            'source': None  # None means all sources
        }
        self.export_formats = ['json', 'csv', 'txt']
        self.monitored_events = set()  # Set of events to monitor in real-time
        self.setup_loggers()

    def setup_loggers(self):
        # Create logs directory in Documents
        self.docs_path = os.path.join(os.path.expanduser('~'), 'Documents', 'DB Sync Agent', 'logs')
        os.makedirs(self.docs_path, exist_ok=True)

        # Setup basic logger (for GUI)
        self.gui_logger = logging.getLogger('gui')
        self.gui_logger.setLevel(logging.INFO)
        gui_handler = logging.StreamHandler()
        gui_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        gui_handler.setFormatter(gui_formatter)
        self.gui_logger.addHandler(gui_handler)

        # Setup detailed logger
        self.detailed_logger = logging.getLogger('detailed')
        self.detailed_logger.setLevel(logging.DEBUG)

        # File handler for detailed logs
        self.detailed_file = os.path.join(self.docs_path, 'detailed.log')
        detailed_handler = logging.FileHandler(self.detailed_file)
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
        )
        detailed_handler.setFormatter(detailed_formatter)
        self.detailed_logger.addHandler(detailed_handler)

        # Initialize log count and check retention
        self.log_count = self.count_log_entries(self.detailed_file)
        self.cleanup_old_logs()

    def set_retention_period(self, days: int):
        """Set how many days to keep logs"""
        self.retention_days = days
        self.cleanup_old_logs()

    def set_max_log_size(self, size_mb: int):
        """Set maximum log file size in MB"""
        self.max_log_size = size_mb * 1024 * 1024
        self.check_log_size()

    def set_log_filter(self, filter_type: str, value: str):
        """Set a log filter"""
        self.log_filters[filter_type] = value

    def clear_log_filter(self, filter_type: str):
        """Clear a specific log filter"""
        self.log_filters[filter_type] = None

    def check_log_size(self):
        """Check if log file exceeds max size and rotate if needed"""
        if os.path.exists(self.detailed_file):
            if os.path.getsize(self.detailed_file) > self.max_log_size:
                self.rotate_log_file()

    def rotate_log_file(self):
        """Rotate the log file with timestamp"""
        if os.path.exists(self.detailed_file):
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = os.path.join(self.docs_path, f'detailed_{timestamp}.log')
            os.rename(self.detailed_file, backup_file)

            # Reset handlers with new file
            self.detailed_logger.handlers.clear()
            new_handler = logging.FileHandler(self.detailed_file)
            new_handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
            ))
            self.detailed_logger.addHandler(new_handler)

    def cleanup_old_logs(self):
        """Remove logs older than retention period"""
        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        for file in os.listdir(self.docs_path):
            if file.startswith('detailed_'):
                file_path = os.path.join(self.docs_path, file)
                file_date = datetime.fromtimestamp(os.path.getctime(file_path))
                if file_date < cutoff_date:
                    os.remove(file_path)

    def count_log_entries(self, log_file):
        try:
            with open(log_file, 'r') as f:
                return sum(1 for _ in f)
        except FileNotFoundError:
            return 0

    def backup_logs_if_needed(self):
        if self.log_count >= 500:
            docs_path = os.path.join(os.path.expanduser('~'), 'Documents', 'DB Sync Agent', 'logs')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = os.path.join(docs_path, f'detailed_{timestamp}.log')

            # Move current log to backup
            detailed_file = os.path.join(docs_path, 'detailed.log')
            if os.path.exists(detailed_file):
                os.rename(detailed_file, backup_file)

            # Reset log count
            self.log_count = 0

            # Clear handlers and setup new ones
            self.detailed_logger.handlers.clear()
            new_handler = logging.FileHandler(detailed_file)
            new_handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
            ))
            self.detailed_logger.addHandler(new_handler)

    def log_sync_operation(self, operation_type: str, details: Dict, error=None, level='INFO'):
        """Log a sync operation and emit signal for GUI updates"""
        if error:
            level = 'ERROR'
            message = f"{operation_type} failed: {str(error)}"
        else:
            status = details.get('status', 'successful')
            message = f"{operation_type} {status}"

        # Log to file
        log_method = getattr(self.detailed_logger, level.lower())
        log_method(message)

        # Create and emit log event for GUI
        log_event = LogEvent(
            timestamp=datetime.now().isoformat(),
            level=level,
            message=message,
            details=details
        )
        self.monitor.new_log_event.emit(log_event)

    def log_table_sync(self, table_name, direction, status='success', details=None, error=None):
        """Log table sync operation"""
        msg = {
            'table': table_name,
            'direction': direction,
            'status': status
        }
        if details:
            msg['details'] = details

        if error:
            msg['error'] = str(error)
            self.gui_logger.error(f"Failed to sync table {table_name}: {str(error)}")
            self.detailed_logger.error(json.dumps(msg, indent=2))
        else:
            self.gui_logger.info(f"Successfully synced table {table_name}")
            self.detailed_logger.info(json.dumps(msg, indent=2))

        self.log_count += 1
        self.backup_logs_if_needed()

    def export_detailed_logs(self, target_dir=None):
        """Export detailed logs to a specific directory"""
        if not target_dir:
            target_dir = os.path.join(os.path.expanduser('~'), 'Documents', 'DB Sync Agent', 'exports')

        os.makedirs(target_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        export_file = os.path.join(target_dir, f'detailed_log_export_{timestamp}.log')

        docs_path = os.path.join(os.path.expanduser('~'), 'Documents', 'DB Sync Agent', 'logs')
        source_file = os.path.join(docs_path, 'detailed.log')

        if os.path.exists(source_file):
            with open(source_file, 'r') as src, open(export_file, 'w') as dst:
                dst.write(src.read())

            return export_file
        return None

    def add_monitor_event(self, event_type: str):
        """Add an event type to real-time monitoring"""
        self.monitored_events.add(event_type)

    def remove_monitor_event(self, event_type: str):
        """Remove an event type from real-time monitoring"""
        self.monitored_events.discard(event_type)

    def apply_filters(self, log_entry: dict) -> bool:
        """Apply all active filters to a log entry"""
        if self.log_filters['level'] and log_entry.get('level') != self.log_filters['level']:
            return False

        if self.log_filters['operation'] and log_entry.get('operation') != self.log_filters['operation']:
            return False

        if self.log_filters['status'] and log_entry.get('status') != self.log_filters['status']:
            return False

        if self.log_filters['source'] and log_entry.get('source') != self.log_filters['source']:
            return False

        if self.log_filters['keyword']:
            keyword = self.log_filters['keyword'].lower()
            message = str(log_entry.get('message', '')).lower()
            if keyword not in message:
                return False

        if self.log_filters['date_range']:
            start_date, end_date = self.log_filters['date_range']
            log_date = datetime.fromisoformat(log_entry['timestamp'].split('.')[0])
            if not (start_date <= log_date <= end_date):
                return False

        return True

    def get_filtered_logs(self) -> List[Dict]:
        """Get logs applying all active filters"""
        filtered_logs = []
        with open(self.detailed_file, 'r') as f:
            for line in f:
                try:
                    log_entry = self._parse_log_line(line)
                    if self.apply_filters(log_entry):
                        filtered_logs.append(log_entry)
                except:
                    continue
        return filtered_logs

    def export_logs(self, format_type: str, output_path: str, filtered: bool = True):
        """Export logs in the specified format"""
        if format_type not in self.export_formats:
            raise ValueError(f"Unsupported format. Use one of {self.export_formats}")

        logs = self.get_filtered_logs() if filtered else self._read_all_logs()

        if format_type == 'json':
            with open(output_path, 'w') as f:
                json.dump(logs, f, indent=2)

        elif format_type == 'csv':
            if not logs:
                return

            headers = ['timestamp', 'level', 'message'] + list(logs[0].get('details', {}).keys())
            with open(output_path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                for log in logs:
                    row = {
                        'timestamp': log['timestamp'],
                        'level': log['level'],
                        'message': log['message']
                    }
                    row.update(log.get('details', {}))
                    writer.writerow(row)

        elif format_type == 'txt':
            with open(output_path, 'w') as f:
                for log in logs:
                    f.write(f"{log['timestamp']} - {log['level']} - {log['message']}\n")
                    if log.get('details'):
                        f.write(f"Details: {json.dumps(log['details'], indent=2)}\n")
                    f.write('-' * 80 + '\n')

    def _parse_log_line(self, line: str) -> Dict:
        """Parse a log line into a structured format"""
        pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - (\w+) - \[(.*?)\] - (.*)'
        match = re.match(pattern, line)
        if not match:
            raise ValueError("Invalid log line format")

        timestamp, level, source, message = match.groups()
        return {
            'timestamp': timestamp,
            'level': level,
            'source': source,
            'message': message.strip()
        }

    def log(self, level: str, message: str, details: Dict = None):
        """Generic logging method that ensures GUI updates"""
        # Log to file
        log_method = getattr(self.detailed_logger, level.lower())
        log_method(message)

        # Create and emit log event for GUI
        log_event = LogEvent(
            timestamp=datetime.now().isoformat(),
            level=level,
            message=message,
            details=details
        )
        self.monitor.new_log_event.emit(log_event)

# Create a global logger instance
logger = DetailedLogger()
