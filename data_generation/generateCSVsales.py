import csv
import random
from faker import Faker

fake = Faker()

numRows = 1000
fileName = "../data_bda/sales_data.csv"
headers = ["Date", "StoreID", "ProductID", "QuantitySold", "Revenue"]

def introduceErrors(value, errorProbability=0.1):
    if random.random() < errorProbability:
        errorType = random.choice(["null", "empty", "wrongFormat"])
        if errorType == "null":
            return None
        elif errorType == "empty":
            return ""
        elif errorType == "wrongFormat":
            return str(value) + "_err"
    return value

with open(fileName, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(headers)
    
    for _ in range(numRows):
        date = introduceErrors(fake.date_between(start_date="-1y", end_date="today").strftime("%Y-%m-%d"))
        storeID = introduceErrors(random.randint(1, 100))
        productID = introduceErrors(fake.bothify(text='??-####'))
        quantitySold = introduceErrors(random.randint(1, 50))
        revenue = introduceErrors(round(random.uniform(5.0, 500.0), 2))
        
        writer.writerow([date, storeID, productID, quantitySold, revenue])

print(f"Archivo {fileName} generado con éxito.")