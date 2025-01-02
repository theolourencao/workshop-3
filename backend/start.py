from datasource.api import APICollector
from contracts.schema import GenericSchema, CompraSchema
from aws.client import S3Client

import schedule

schema = CompraSchema
aws = S3Client()

def apiCollector(schema, aws, repeat):
    response = APICollector(schema, aws).start(repeat)

    print("Execução Realizada")
    return

schedule.every(1).minute.do(apiCollector, schema, aws, 50)

while True:
    
