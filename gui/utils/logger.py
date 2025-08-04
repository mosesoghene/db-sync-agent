import os
import logging
from logging.handlers import RotatingFileHandler

# Shared in-memory buffer for GUI log viewer
log_buffer = []

class GuiLogHandler(logging.Handler):
    """Captures logs into memory for GUI display."""
    def emit(self, record):
        msg = self.format(record)
        log_buffer.append(msg)
        if len(log_buffer) > 500:
            log_buffer.pop(0)

def setup_logger(log_file="logs/app.log"):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger("sync_gui")
    logger.setLevel(logging.DEBUG)  # Capture everything

    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")

    # GUI log handler (INFO+ only)
    gui_handler = GuiLogHandler()
    gui_handler.setLevel(logging.INFO)
    gui_handler.setFormatter(formatter)
    logger.addHandler(gui_handler)

    # Rotating file handler (everything, including tracebacks)
    file_handler = RotatingFileHandler(log_file, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.propagate = False
    return logger
