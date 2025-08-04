import logging

log_buffer = []

def setup_logger():
    logger = logging.getLogger("gui")
    logger.setLevel(logging.INFO)
    return logger

class GuiLogHandler(logging.Handler):
    def emit(self, record):
        msg = self.format(record)
        log_buffer.append(msg)
        if len(log_buffer) > 500:
            log_buffer.pop(0)
