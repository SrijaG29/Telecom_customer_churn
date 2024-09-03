import random
import time
from datetime import datetime
import json
from kafka import KafkaProducer
import time,random,uuid

''' |customerID|gender|SeniorCitizen|Partner|Dependents|tenure|PhoneService|MultipleLines   |InternetService|OnlineSecurity     
|OnlineBackup       |DeviceProtection  |TechSupport        |StreamingTV        |StreamingMovies    |Contract      
|PaperlessBilling|PaymentMethod            |MonthlyCharges|TotalCharges|Churn|'''


__bootstrap_server = "localhost:9092"
def send_to_kafka(details):
    customer_json_data = json.dumps(details, indent=4)
    producer = KafkaProducer(bootstrap_servers =__bootstrap_server)
    producer.send('customer_data',key=bytes(str(uuid.uuid4()),'utf-8'),value =bytes(str(customer_json_data),'utf-8'))
    producer.close()
    print(details)
    print('Posted the message')

def random_customer_details():
    # Generate customer details
    
    customer_details = {
        'customerID': input('Enter customerID'),
        'gender': input('Enter gender'),
        'SeniorCitizen': input('Enter SeniorCitizen'),
        'Partner': input('Enter Partner'),
        'Dependents': input('Enter Dependents'),
        'tenure': input('Enter tenure'),
        'PhoneService': input('Enter PhoneService'),
        'MultipleLines': input('Enter MultipleLines'),
        'InternetService': input('Enter InternetService'),
        'OnlineSecurity': input('Enter OnlineSecurity'),
        'OnlineBackup': input('Enter OnlineBackup'),
        'DeviceProtection': input('Enter DeviceProtection'),
        'TechSupport': input('Enter TechSupport'),
        'StreamingTV': input('Enter StreamingTV'),
        'StreamingMovies': input('Enter StreamingMovies'),
        'Contract': input('Enter Contract'),
        'PaperlessBilling': input('Enter PaperlessBilling'),
        'PaymentMethod': input('Enter PaymentMethod'),
        'MonthlyCharges': input('Enter MonthlyCharges'),
        'TotalCharges': input('Enter TotalCharges')
    }
    
    return customer_details


details = random_customer_details()
# print(details)
# print(len(details))
send_to_kafka(details)


































# {
#     'user_id': 'u9',
#     'product_id': 'u4',
#     'timestamp': '2020-05-09 18:34:51',
#     'action': 'user_click'
# }

# json_schema = (
#     StructType(
#         [
#             StructField('user_id',StringType(),True),
#             StructField('product_id',StringType(),True),
#             StructField('timestamp',StringType(),True),
#             StructField('action',StringType(),True),
#         ]
#     )
# )
