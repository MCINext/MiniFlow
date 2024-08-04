import logging
import pandas as pd
from io import StringIO


class AppNameComponentFilter(logging.Filter):
    def __init__(self, app_name: str, component: str):
        self.app_name = app_name
        self.component = component

    def filter(self, record):
        record.app_name = self.app_name
        record.component = self.component
        return True


def log_df_info(logger: logging.Logger, df: pd.DataFrame):
    buf = StringIO()
    df.info(buf=buf)
    info_str = buf.getvalue()
    logger.debug(f"dataframe info:\n {info_str}")
