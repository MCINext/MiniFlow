import logging
import logging.config
import pandas as pd
from abc import ABC, abstractmethod
import yaml
from miniflow.loader import DataLoader
from miniflow.sink import DataSink
from miniflow.utils import AppNameComponentFilter


class ETL(ABC):
    def __init__(
        self,
        app_name: str,
        loaders: dict[str, DataLoader],
        sinks: dict[str, DataSink],
        logging_config_path: str = None,
    ):
        self.loaders = loaders
        self.sinks = sinks
        self.logger = logging.getLogger(app_name + "transform")
        self.logger.addFilter(AppNameComponentFilter(app_name, "transform"))
        self.configure_logging(logging_config_path)

    def configure_logging(self, config_path: str = None):
        if config_path:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
            logging.config.dictConfig(config)
        else:
            logging.basicConfig(level=logging.INFO)
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(app_name)s - %(component)s - %(levelname)s - %(message)s"
                )
            )
            self.logger.addHandler(handler)

    def run(self):
        self.logger.info("Running ETL process...")
        loaded_data = self._load_all()
        transformed_data = self._transform(loaded_data)
        self._sink_all(transformed_data)
        self.logger.info("Done!")

    def _load_all(self) -> dict[str, pd.DataFrame]:
        self.logger.info("Loading data from all sources...")
        data_frames = {name: loader.load() for name, loader in self.loaders.items()}
        return data_frames

    @abstractmethod
    def _transform(
        self, data_frames: dict[str, pd.DataFrame]
    ) -> dict[str, pd.DataFrame]:
        pass

    def _sink_all(self, transformed_data: dict[str, pd.DataFrame]) -> None:
        self.logger.info("Sinking data to all destinations...")
        for name, sink in self.sinks.items():
            # if name in transformed_data:
            self.logger.info(f"Sinking {name} data")
            sink.sink(transformed_data[name])
            self.logger.info(f"Sinking {name} finished")
