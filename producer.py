import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import loads,dumps
import json

producer = KafkaProducer(bootstrap_servers =['{public ip of ec2 instance}:9092'],
                           value_serializer = lambda x:dumps(x).encode('utf-8'))

producer.send('stocktopic',value={'name':'pawan'})

df = pd.read_csv(".\\stockdata.csv")

while True:
    dict_stock = df.sample(1).to_dict(orient="records")[0]
    producer.send("stocktopic",value = dict_stock)
    sleep(1)

producer.flush()