# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import requests
import yaml
import os
import pandas as pd
from io import StringIO


class WebTrafficFactory:
    """ Factory object containing modularized ETL functionality; transformations, validations, config settings, etc."""

    def __init__(self, config_file_path):
        self.config_file_path = config_file_path
        self.config = yaml.safe_load(open(self.config_file_path, 'r')).get('configuration')
        self.root_url = self.config.get('root_url')
        self.config_schema = self.config.get('source_schema')
        self.output_location = self.config.get('output_location')
        self.output_df = pd.DataFrame()
        self.verify_yaml_config()

    def transform_csv(self, src_file_name):
        """ Fetch csv file, perform transformations and append results to factory's output_df """

        full_url = os.path.join(self.root_url, src_file_name)
        response = requests.get(full_url)
        df = pd.read_csv(StringIO(response.text))

        # Verify source system schema matches configuration file schema
        self.verify_schema(df)

        # Remove unused columns
        df = df.drop(columns=['drop', 'user_agent'])

        # Transformation logic
        df = df.pivot_table(index='user_id', columns='path', values='length', aggfunc='sum').fillna(0)

        # Append transformed df to output_df
        if self.output_df.empty:
            self.output_df = df
        else:
            self.output_df = self.output_df.add(df, fill_value=0)

    def export_results(self):
        self.output_df.to_csv(self.output_location)

    def verify_schema(self, df):
        """ Ensure that the source system schema matches the expected schema from config file """

        source_system_schema = df.columns.tolist()
        if source_system_schema == self.config_schema:
            return True
        else:
            raise ValueError(f"Unexpected schema received from the source; expected {self.config_schema}, but returned {df.columns.tolist()}")

    def verify_yaml_config(self):
        """ Ensure that the Web Traffic yaml config file has the required values for Factory instantiation """

        wt_config = self.config
        required_values = ['root_url', 'source_schema', 'output_location']

        for value in required_values:
            if value not in wt_config:
                raise ValueError("Missing required yaml configuration parameter: {}".format(value))
        return True
