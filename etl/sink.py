import logging
import pandas as pd
from abc import ABC, abstractmethod
from prometheus_client import Counter
from sqlalchemy import create_engine
from etl.utils import AppNameComponentFilter, log_df_info
import clickhouse_connect

class DataSink(ABC):
    def __init__(self, app_name: str):
        self.app_name = app_name
        self.logger = logging.getLogger(app_name + "-sink")
        self.logger.addFilter(AppNameComponentFilter(app_name, "sink"))  # noqa: F821
        self.sink_count = Counter(
            "sink_operations_total", "Total number of sink operations", ["app_name"]
        )
        self.sink_failures = Counter(
            "sink_failures_total", "Total number of sink failures", ["app_name"]
        )

    @abstractmethod
    def sink(self, df: pd.DataFrame) -> None:
        pass



class ClickHouseSink(DataSink):
    def __init__(
        self,
        app_name: str,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        table: str,
    ):
        super().__init__(app_name)
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.table = table

    def sink(self, df: pd.DataFrame) -> None:
        try:
            client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                database=self.database,
            )
            log_df_info(self.logger, df)
            self.logger.info(
                f"Trying to insrt data into ClickHouse table: {self.table}"
            )
            client.insert_df(self.table, df, self.database)
            self.sink_count.labels(app_name=self.app_name).inc()
            self.logger.info(
                f"Data successfully inserted into ClickHouse table {self.table}"
            )
        except Exception as e:
            self.sink_failures.labels(app_name=self.app_name).inc()
            self.logger.error(
                f"Failed to insert data into ClickHouse table {self.table}: {e}"
            )
            raise


class PostgreSQLSink:
    def __init__(self, app_name: str, connection_string: str, table_name: str):
        super().__init__(app_name)
        self.engine = create_engine(connection_string)
        self.table_name = table_name

    def sink(self, df: pd.DataFrame) -> None:
        try:
            self.logger.info(
                f"Inserting data into PostgreSQL table {self.table_name}..."
            )
            df.to_sql(
                self.table_name,
                self.engine,
                if_exists="append",
                index=False,
                method="multi",
            )
            self.sink_count.labels(app_name=self.app_name).inc()
            self.logger.info("Data successfully inserted into PostgreSQL.")
        except Exception as e:
            self.sink_failures.labels(app_name=self.app_name).inc()
            self.logger.error(f"Failed to insert data into PostgreSQL: {e}")
            raise


class FileSink(DataSink):
    def __init__(self, app_name: str, file_path: str):
        super().__init__(app_name)
        self.file_path = file_path

    def sink(self, df: pd.DataFrame) -> None:
        try:
            df.to_csv(self.file_path, index=False)
            self.sink_count.labels(app_name=self.app_name).inc()
            self.logger.info(f"Data successfully written to {self.file_path}")
        except Exception as e:
            self.sink_failures.labels(app_name=self.app_name).inc()
            self.logger.error(f"Failed to write data to {self.file_path}: {e}")
            raise
