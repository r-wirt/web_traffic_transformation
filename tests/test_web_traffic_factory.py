import unittest

import pandas as pd

from factories.web_traffic_factory import WebTrafficFactory
from unittest.mock import patch
from pandas.testing import assert_frame_equal


class TestWebTrafficFactory(unittest.TestCase):

    @patch.object(WebTrafficFactory, '__init__', lambda self: None)
    def setUp(self):
        self.factory = WebTrafficFactory()
        self.factory.config = {'source_schemapurposefultypo': ['column1', 'column2', 'column3'],
                               'root_urlurlurl': 'http://example.com',
                               'output_location': '/example/output'}
        self.factory.root_url = 'http://example.com'
        self.factory.config_schema = ['drop', 'length', 'path', 'user_agent', 'user_id']
        self.factory.output_df = pd.DataFrame()
        self.input_df = pd.DataFrame(
            {'drop': {0: True, 1: False, 2: False, 3: True},
             'length': {0: 7, 1: 11, 2: 4, 3: 12},
             'path': {0: '/', 1: '/', 2: '/', 3: '/tutorial/intro'},
             'user_agent': {0: 'Browser 1', 1: 'Browser 2', 2: 'Browser 3', 3: 'Browser 4'},
             'user_id': {0: 378, 1: 220, 2: 449, 3: 220}})

        self.factory.text = '''drop,length,path,user_agent,user_id
1,7,/,"Browser One"	,378
0,11,/,"Browser One",220
0,4,/,"Browser Three",449
1,12,/tutorial/intro,"Browser Seven",220'''

    @patch('requests.get')
    @patch('pandas.read_csv')
    def test_transform_csv(self, mock_pandas_read_csv, mock_requests_get_response):

        mock_requests_get_response.return_value = self.factory
        mock_pandas_read_csv.return_value = self.input_df

        self.factory.transform_csv('test_file.csv')

        indices = [220, 378, 449]
        expected_data = {'/': [11.0, 7.0, 4.0], '/tutorial/intro': [12.0, 0.0, 0.0]}
        expected_df = pd.DataFrame(data=expected_data, index=indices)
        expected_df.index.name = 'user_id'
        expected_df.columns.name = 'path'

        assert_frame_equal(self.factory.output_df, expected_df)

    def test_verify_schema(self):
        assert (self.factory.verify_schema(self.input_df))

    def test_verify_yaml_config(self):
        with self.assertRaises(ValueError):
            self.factory.verify_yaml_config()