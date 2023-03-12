from kafka import KafkaConsumer
from time import sleep
import json
from json import dumps,loads
from s3fs import S3FileSystem
import boto3


consumer = KafkaConsumer(
    'stocktopic',
     bootstrap_servers=['{public ip of ec2 instance}:9092'],
    value_deserializer=lambda x: loads(x.decode('utf-8')))

s3 = boto3.resource('s3')
# for bucket in s3.buckets.all():
#     print(bucket.name)

# s3 = S3FileSystem(anon=False)

# data = open('C:\\Users\\pawan\\Desktop\\kafka_stock_project\\pawan.txt', 'rb')
# s3.Bucket('stockdata-pawan').put_object(Key='pawan.txt', Body=data)
# this is for testing
for count, i in enumerate(consumer):
    # with s3.open("s3://stockdata-pawan/stock_market_{}.json".format(count),'w') as file:
    #     json.dump(i.value, file)
    data  = json.dumps(i.value)
    s3.Bucket('stockdata-pawan').put_object(Key='stock_market_{}.json'.format(count), Body=data)



#     s3.Bucket('stockdata-pawan').put_object(Key='stock_market_{}.json'.format(count), Body= json.dump(i.value))
    # with s3.open("s3://stockdata-pawan/stock_market_{}.json".format(count),'w') as file:
    #     json.dump(i.value, file)