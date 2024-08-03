import logging
import pandas as pd
from abc import ABC, abstractmethod
import boto3
from io import BytesIO
from prometheus_client import Counter
from etl.utils import AppNameComponentFilter, log_df_info

class DataLoader(ABC):
    def __init__(self, app_name: str):
        self.app_name = app_name
        self.logger = logging.getLogger(app_name + "-loader")
        self.logger.addFilter(AppNameComponentFilter(app_name, "data_loader"))
        self.load_count = Counter(
            "load_operations_total", "Total number of load operations", ["app_name"]
        )
        self.load_failures = Counter(
            "load_failures_total", "Total number of load failures", ["app_name"]
        )
        self.rows_loaded = Counter(
            "rows_loaded_total", "Total number of rows loaded", ["app_name"]
        )

    @abstractmethod
    def load(self) -> pd.DataFrame:
        pass


class LocalCSVLoader(DataLoader):
    def __init__(self, app_name: str, file_path: str):
        super().__init__(app_name)
        self.file_path = file_path

    def load(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(self.file_path)
            self.load_count.labels(app_name=self.app_name).inc()
            self.rows_loaded.labels(app_name=self.app_name).inc(len(df))
            self.logger.info(f"Loaded {len(df)} rows from {self.file_path}")
            return df
        except Exception as e:
            self.load_failures.labels(app_name=self.app_name).inc()
            self.logger.error(f"Failed to load data from {self.file_path}: {e}")
            raise


class CephS3Loader(DataLoader):
    def __init__(
        self,
        app_name: str,
        endpoint_url: str,
        bucket_name: str,
        file_key: str,
        access_key: str,
        secret_key: str,
    ):
        super().__init__(app_name)
        self.endpoint_url = endpoint_url
        self.bucket_name = bucket_name
        self.file_key = file_key
        self.access_key = access_key
        self.secret_key = secret_key

    def load(self) -> pd.DataFrame:
        try:
            session = boto3.Session(
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
            )
            s3 = session.client("s3", endpoint_url=self.endpoint_url)
            self.logger.info(
                f"Trying to load {self.file_key} in bucket {self.bucket_name} as a dataframe"
            )
            response = s3.get_object(Bucket=self.bucket_name, Key=self.file_key)
            file_content = response["Body"].read()
            df = pd.read_csv(BytesIO(file_content), low_memory=False, dtype=str)
            log_df_info(self.logger, df)
            self.load_count.labels(app_name=self.app_name).inc()
            self.rows_loaded.labels(app_name=self.app_name).inc(len(df))
            self.logger.info(
                f"Loaded {len(df)} rows from {self.file_key} in bucket {self.bucket_name}"
            )
            return df
        except Exception as e:
            self.load_failures.labels(app_name=self.app_name).inc()
            self.logger.error(
                f"Failed to load data from {self.file_key} in bucket {self.bucket_name}: {e}"
            )
            raise
    def set_key(self, key): 
        self.file_key = key
