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
    tenure = str(random.randint(0, 100))
    MonthlyCharges = str(random.randint(1, 100))
    
    # Create customer details dictionary
    # customer_details = {
    #     'customerID': 'ABC-' + ''.join([str(random.randint(0, 9)) for _ in range(4)]),
    #     'gender': random.choice(('Male', 'Female')),
    #     'SeniorCitizen': str(random.randint(0, 1)),
    #     'Partner': random.choice(('Yes', 'No')),
    #     'Dependents': random.choice(('Yes', 'No')),
    #     'tenure': tenure,
    #     'PhoneService': random.choice(('Yes', 'No')),
    #     'MultipleLines': random.choice(('Yes', 'No', 'No phone service')),
    #     'InternetService': random.choice(('DSL', 'Fiber optic', 'No')),
    #     'OnlineSecurity': random.choice(('Yes', 'No', 'No internet service')),
    #     'OnlineBackup': random.choice(('Yes', 'No', 'No internet service')),
    #     'DeviceProtection': random.choice(('Yes', 'No', 'No internet service')),
    #     'TechSupport': random.choice(('Yes', 'No', 'No internet service')),
    #     'StreamingTV': random.choice(('Yes', 'No', 'No internet service')),
    #     'StreamingMovies': random.choice(('Yes', 'No', 'No internet service')),
    #     'Contract': random.choice(('Month-to-month', 'One year', 'Two year')),
    #     'PaperlessBilling': random.choice(('Yes', 'No')),
    #     'PaymentMethod': random.choice(('Bank transfer (automatic)', 'Credit card (automatic)', 'Electronic check', 'Mailed check')),
    #     'MonthlyCharges': MonthlyCharges
    # }
    # customer_details['TotalCharges'] = str(int(tenure) * int(MonthlyCharges))

    details = [['7590-VHVEG','Female',0,'Yes','No',1,'No','No','DSL','No','Yes','No','No','No','No','Month-to-month','Yes','Electronic check',29 ,29]]

    customer_details = {
        'customerID': 'ABC-' + ''.join([str(random.randint(0, 9)) for _ in range(4)]),
        'gender': random.choice(('Male', 'Female')),
        'SeniorCitizen': '0',
        'Partner': 'Yes',
        'Dependents': 'No',
        'tenure': '1',
        'PhoneService': 'No',
        'MultipleLines': 'No',
        'InternetService': 'DSL',
        'OnlineSecurity': 'No',
        'OnlineBackup': 'Yes',
        'DeviceProtection': 'No',
        'TechSupport': 'No',
        'StreamingTV': 'No',
        'StreamingMovies': 'No',
        'Contract': 'Month-to-month',
        'PaperlessBilling': 'Yes',
        'PaymentMethod': 'Electronic check',
        'MonthlyCharges': '29',
        'TotalCharges': '29'
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