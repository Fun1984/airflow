from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd

class SeoulApiToCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path', 'file_name', 'base_dt')

    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm
        self.base_dt = base_dt
    
    def execute(self, context):
        import os

        connection = BaseHook.get_connection(self.http_conn_id) #UI화면에 만든 connection정보를 끌어올 수 있게해줌
        self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}'

        total_row_df = pd.DataFrame()
        start_row = 1
        end_row = 1000
        while True : 
            self.log.info(f'시작:{start_row}')
            self.log.info(f'끝:{end_row}')
            row_df = self._call_api(self.base_url, start_row, end_row)
            total_row_df = pd.concat([total_row_df, row_df])
            if len(row_df) < 1000: #한순간 끝자락은 1000건이 안된단 건, 끝자락이란 소리임.
                break
            else :
                start_row = end_row + 1
                end_row += 1000

        print("SSSSSSSSSSSSSSSSSSSSS ", os.path.exists(self.path))
        if not os.path.exists(self.path):
            print(self.path)
            os.system(f'mkdir -p {self.path}')
        total_row_df.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)

    def _call_api(self, base_url, start_row, end_row):
        import requests
        import json

        headers = {'Content-Type': 'application/json',
                   'charset':'utf-8',
                   'Accept':'*/*'}
        
        request_url = f'{base_url}/{start_row}/{end_row}/202511'
        if self.base_dt is not None:
            request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'
        response = requests.get(request_url, headers)
        contents = json.loads(response.text)

        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get('row')
        row_df = pd.DataFrame(row_data)
        print(row_df)

        return row_df
