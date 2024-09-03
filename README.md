In this project, I have created a streaming platform where telecom customer details like tenure, MonthlyCharges, InternetService, etc., can be sent to predict whether they will churn or not.To train this model, I used the dataset from [Kaggle](https://www.kaggle.com/datasets/blastchar/telco-customer-churn).

I employed three models to make predictions: Logistic Regression, Decision Tree Classifier, and Random Forest Classifier. The accuracies of these models are quite close to each other. The Logistic Regression model has an accuracy of 0.8007, the Decision Tree Classifier has an accuracy of 0.7941, and the Random Forest Classifier has an accuracy of 0.8022.

**Best Performing Model:** The Random Forest Classifier has the highest accuracy at 0.8022, slightly outperforming the Logistic Regression model (0.8007) and the Decision Tree Classifier (0.7941).

I have created two PySpark files: `Customer_Kafka_streaming.ipynb` for streaming data and `Customer_Pyspark_streaming.ipynb` for predicting whether a customer will churn or not, using the Random Forest Classifier.

**Steps to Install:**

1. Clone this repository: [Telecom_customer_churn](https://github.com/SrijaG29/Telecom_customer_churn.git)
2. Install Kafka and PySpark.
3. Create a Kafka topic using the following command:
   ```
   kafka-topics --create --topic <topic_name> --bootstrap-server localhost:<your_port_number>
   ```
4. Run `Customer_Kafka_streaming.ipynb` and `Customer_Pyspark_streaming.ipynb`, changing the Kafka ports and topic name as needed.
5. Run the `customer_details_random.py` script to push data into the Kafka topic.
6. You can now see the predicted results in the console.
