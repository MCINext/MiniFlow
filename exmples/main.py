from miniflow.etl import ETL
from miniflow.loader import LocalCSVLoader
from miniflow.sink import FileSink

class MyETL(ETL):
    def _transform(self, data_frames):
        transformed_data = {}
        csv_data = data_frames.get('csv')
        csv_data['transformed_column'] = csv_data['column1'] * 2
        transformed_data['file'] = csv_data
        return transformed_data

def main():
    app_name = "MyApp"
    logging_config_path = 'logging-config.yaml'
    loaders = {'csv': LocalCSVLoader(app_name, 'sample.csv')}
    sinks = {'file': FileSink(app_name, 'output.csv')}

    etl_process = MyETL("MyApp", loaders, sinks, logging_config_path)
    etl_process.run()

if __name__ == "__main__":
    main()
