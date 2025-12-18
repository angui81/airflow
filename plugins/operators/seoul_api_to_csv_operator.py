from airflow.models import BaseOperator
from airflow.sdk.bases.hook import BaseHook
from pprint import pprint
import pandas as pd


class SeoulApiToCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path', 'file_name', 'base_dt')

    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        from airflow.models import Variable

        super().__init__(**kwargs)
        self.http_conn_id = 'pulbic_api'
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{ var.value.apikey_openapi_seoul_go_kr }}/json' + dataset_nm
        self.base_dt = base_dt
        pprint(f'***** self : { self }')
        api_key = Variable.get("apikey_openapi_seoul_go_kr")
        pprint(f'***** api_key : { api_key }')

    def execute(self, context):
        import os

        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'{connection.host}:{connection.port}/{self.endpoint}'

        self.log.info(f'***** self.base_url : { self.base_url}')
        self.log.info(f'***** connection.host : { connection.host }')
        self.log.info(f'***** connection.port : { connection.port }')
        self.log.info(f'***** self.endpoint : { self.endpoint }')

        total_row_df = pd.DataFrame()
        start_row = 1
        end_row = 1000

        while True:
            self.log.info(f'***** 시작 : { start_row }')
            self.log.info(f'***** 끝 : { end_row }')
            row_df = self._call_api(self.base_url, start_row, end_row)
            self.log.info(f'***** row_df : { row_df }')
            total_row_df = pd.concat([total_row_df, row_df])

            if len(row_df) < 1000:
                break
            else:
                start_row = end_row + 1
                end_row += 1000

        if not os.path.exists(self.path):
            os.system(f'mkdir -p {self.path}')

        total_row_df.to_csv()

    def _call_api(self, base_url, start_row, end_row):
        import requests
        import json

        headers = {'Content-Type': 'application/json', 'charset': 'utf-8', 'Accept': '*/*'}

        request_url = f'{base_url}/{start_row}/{end_row}/'

        pprint(f'##### base_url: {base_url}')
        pprint(f'##### request_url: {request_url}')

        if self.base_dt is not None:
            request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'

        response = requests.get(request_url, headers)
        contents = json.load(response.text)

        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get('row')
        row_df = pd.DataFrame(row_data)

        return row_df
