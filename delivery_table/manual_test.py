import logging
import sys

# Configure logging
logging.basicConfig(
    filename='app.log',  # Log to a file
    filemode='w',        # 'w' for overwrite, 'a' for append
    level=logging.DEBUG,  # Set the logging level
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Create a console handler for stdout
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Add the console handler to the root logger
logging.getLogger().addHandler(console_handler)

# Redirect stdout and stderr to logging
class StreamToLogger:
    def __init__(self, logger, level=logging.INFO):
        self.logger = logger
        self.level = level

    def write(self, message):
        if message.strip():  # Avoid logging empty messages
            self.logger.log(self.level, message.strip())

    def flush(self):
        pass  # Needed for Python 3 compatibility

# Redirect stdout and stderr
sys.stdout = StreamToLogger(logging.getLogger(), logging.INFO)
sys.stderr = StreamToLogger(logging.getLogger(), logging.ERROR)

# Example of logging various messages
print("This is a standard output message.")
logging.debug("This is a debug message.")
logging.info("This is an info message.")
logging.warning("This is a warning message.")
logging.error("This is an error message.")
logging.critical("This is a critical message.")
