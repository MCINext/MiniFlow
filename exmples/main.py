from etl.etl import ETL
from etl.loader import LocalCSVLoader
from etl.sink import FileSink

class MyETL(ETL):
    def _transform(self, data_frames):
        transformed_data = {}
        for name, df in data_frames.items():
            df['transformed_column'] = df['column1'] * 2
            transformed_data[name] = df
        return transformed_data

def main():
    logging_config_path = 'logging-config.yaml'
    loaders = {'example_loader': LocalCSVLoader()}
    sinks = {'example_sink': FileSink()}

    etl_process = MyETL("MyApp", loaders, sinks, logging_config_path)
    etl_process.run()

if __name__ == "__main__":
    main()
