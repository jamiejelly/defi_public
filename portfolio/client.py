
from logging import Handler,LogRecord

import requests
import config

class TelegramBotHandler(Handler):
    def __init__(self):
        super().__init__()
        self.token = config.TG_BOT_TOKEN
        self.chat_id = config.TG_CHAT_ID

    def emit(self, record: LogRecord):
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        params = {"chat_id": self.chat_id, "text": self.format(record)}
        try:
            # requests logs should be deactivated by the process logger
            requests.get(url, params=params)
        except Exception:
            # can't log here, otherwise infinite recursive loop
            return
