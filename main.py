from factories.web_traffic_factory import WebTrafficFactory
import string
import logging


def script():
    """Fetch, Transform and export csv files a-z"""

    # Assign Web Traffic Factory object containing modularized ETL functionality;
    # transformations, validations, config settings, etc.
    factory_context = WebTrafficFactory(config_file_path='configs/web_traffic_config.yml')

    for letter in string.ascii_lowercase:
        logging.info("Processing letter-file: {}".format(letter))
        src_file_name = f'{letter}.csv'
        factory_context.transform_csv(src_file_name)

    factory_context.export_results()


if __name__ == '__main__':
    script()
