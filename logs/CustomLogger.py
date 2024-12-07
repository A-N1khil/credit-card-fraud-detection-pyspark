import logging
import time
import os

class CustomLogger:
    def __init__(self, name):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        path = f'{name}.{time.time()}.log'
        if not os.path.exists(path):
            with open(path, 'w') as file:
                file.write('')
                pass
        handler = logging.FileHandler(path)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.info("Logger is set up")
        print("Logger is set up. Check producer.log for logs.")

    def get_logger(self):
        return self.logger

    def info(self, message):
        self.logger.info(message)

