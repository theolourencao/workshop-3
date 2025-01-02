from fastapi import FastAPI
from faker import Faker
import pandas as pd
import random

app = FastAPI()
fake = Faker()

file_name = 'backend/fakeapi/products.csv'
df = pd.read_csv(file_name)
df['indice'] = range(1, len(df)+1)
df.set_index('indice', inplace=True)



@app.get("/gerar_compra/{numero_registro}")
async def gerar_compra(numero_registro: int):
    
    if numero_registro <1:
        return {"error": "O número deve ser maior que 1"}
    
    answer= []

    for _ in range(numero_registro):

        index = random.randint(1, len(df)-1)
        tuple = df.iloc[index]
        person = {
                "client": fake.name(),
                "creditcard": fake.credit_card_provider(),
                "product_name": tuple["product_name"],
                "ean": fake.ean(),
                "price": round(float(tuple["price"].lstrip('$').replace(',','')),2),
                "store": 11,        
                "datetime": fake.iso8601(),
                "clientPosition" : fake.location_on_land()}
        answer.append(person)

    return answer