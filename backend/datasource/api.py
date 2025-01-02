import requests
from contracts.schema import GenericSchema, CompraSchema
from io import BytesIO
from typing import List
import pandas as pd
import pyarrow.parquet as pq
import datetime


class APICollector:
    def __init__ (self, schema, aws):
        self._schema = schema
        self._aws = aws
        self._buffer = None
        return



        return
    
    def start(self, param):
        
        response = self.getData(param)
        print("Obtive dados")
        response = self.extractData(response)
        print("Exportar dados")
        response = self.transformDF(response)
        print(response)
        response = self.convertToParquet(response)
        print(response)

        if self._buffer is not None:
            file_name = self.fileName()
            print(file_name)

            self._aws.upload_file(response, file_name)
            return True



        return False

    def getData(self, param):
        if param > 1:
            response = requests.get(f'http://127.0.0.1:8000/gerar_compra/{param}').json()
        else:
            response = requests.get('http://127.0.0.1:8000/gerar_compra/').json()
        return response
    
    def extractData(self, response):

        result: List[GenericSchema] = []
        for item in response:
            index = {}
            for key, value in self._schema.items():
                if type(item.get(key))== value:
                    index[key] = item[key]
                else:
                    index[key] = None
            result.append(index)


        return result
    
    def transformDF(self, response):
        result = pd.DataFrame(response)

        return result
    
    def convertToParquet(self, response):
        self._buffer = BytesIO()
        try:
            response.to_parquet(self._buffer)
            return self._buffer
        except:
            print("Erro ao transformar o DF em parquet")
            self._buffer= None

    def fileName(self):
        date_atual = datetime.datetime.now().isoformat()
        match = date_atual.split(".")                                                       
        return f"api/api-response-compra{match[0]}.parquet"
    