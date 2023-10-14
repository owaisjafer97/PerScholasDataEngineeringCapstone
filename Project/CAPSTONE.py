#IMPORTING ALL REQUIRED PACKAGES AND CLASSES
import mysql.connector as mydbconnection #to establish a connection with the WorkBench
from mysql.connector import Error #To show us the error in a try except block
import pandas as pd #for dataframe
import matplotlib.pyplot as plt #for making graphs
import json #for reading the credentials.json
import requests #for getting data from a API
from pyspark.sql.functions import concat_ws #concat with space
from pyspark.sql.functions import concat #normal concat (joining strings)
from pyspark.sql.functions import initcap #for title case
from pyspark.sql.functions import lower #to convert to lower case
from pyspark.sql.functions import lit #to add a constant value column across a dataframe
import random #to generate area codes
from pyspark.sql import SparkSession #to initiate spark sessions
from pyspark.sql.functions import lpad #to convert month and date to 2 digit values in case it is a single digit value
import time
import re
import datetime
import warnings
warnings.filterwarnings("ignore")

#this is used to read the credentials, username and password in the credentials.json file
with open("credentials.json", "r") as credentials_file:
    credentials = json.load(credentials_file)

#PART 1 CAPSTONE
#Here we establish a connection with the database and create a database called creditcard_capstone

conn = mydbconnection.connect(user=credentials['user'],password=credentials['password'])
cursor = conn.cursor()
cursor.execute("CREATE DATABASE `creditcard_capstone`")

#Here we initiate a spark session named CAPSTONE

spark = SparkSession.builder.appName('CAPSTONE').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#to get all possible correct area codes for the mobile number in list form
AREA_CODE_LIST = [i for i in range(201,990)] #area codes start from 201 and go to 989
for j in (211,311,411,511,611,711,811,911): #these numbers are not valid area codes
    AREA_CODE_LIST.remove(j)

#to get all possible USA zip codes in list form
US_CITIES_ZIPCODES_DF = pd.read_csv("uszips.csv")
US_CITIES_ZIPCODES_LIST = US_CITIES_ZIPCODES_DF['zip'].to_list()

#to get a list of all USA cities
US_CITIES_DF = pd.read_csv("uscities.csv")
US_CITIES_LIST = US_CITIES_DF['city'].tolist()

state_abbreviation_to_name_dictionary = {'AL': 'Alabama','AK': 'Alaska','AZ': 'Arizona','AR': 'Arkansas','CA': 'California','CO': 'Colorado','CT': 'Connecticut','DE': 'Delaware','FL': 'Florida','GA': 'Georgia','HI': 'Hawaii','ID': 'Idaho','IL': 'Illinois','IN': 'Indiana','IA': 'Iowa','KS': 'Kansas','KY': 'Kentucky','LA': 'Louisiana','ME': 'Maine','MD': 'Maryland','MA': 'Massachusetts','MI': 'Michigan','MN': 'Minnesota','MS': 'Mississippi','MO': 'Missouri','MT': 'Montana','NE': 'Nebraska','NV': 'Nevada','NH': 'New Hampshire','NJ': 'New Jersey','NM': 'New Mexico','NY': 'New York','NC': 'North Carolina','ND': 'North Dakota','OH': 'Ohio','OK': 'Oklahoma','OR': 'Oregon','PA': 'Pennsylvania','RI': 'Rhode Island','SC': 'South Carolina','SD': 'South Dakota','TN': 'Tennessee','TX': 'Texas','UT': 'Utah','VT': 'Vermont','VA': 'Virginia','WA': 'Washington','WV': 'West Virginia','WI': 'Wisconsin','WY': 'Wyoming'}

#Here we are building a database called cdw_sapp_custmer using Spark and adding it to the workbench after the required mapping is done

CUSTOMER_DF = spark.read.json("cdw_sapp_custmer.json") #reading the JSON file in Spark
CUSTOMER_DF = CUSTOMER_DF.withColumn("FIRST_NAME", initcap(CUSTOMER_DF["FIRST_NAME"])) #making first name to a title case
CUSTOMER_DF = CUSTOMER_DF.withColumn("MIDDLE_NAME", lower(CUSTOMER_DF["MIDDLE_NAME"])) #making the middle name to lower case
CUSTOMER_DF = CUSTOMER_DF.withColumn("LAST_NAME", initcap(CUSTOMER_DF["LAST_NAME"])) #making last name to a title case
CUSTOMER_DF = CUSTOMER_DF.withColumn("FULL_STREET_ADDRESS", concat_ws(",", CUSTOMER_DF["APT_NO"], CUSTOMER_DF["STREET_NAME"])) #concatenating apartment and street columns to generate a full street address separated by a comma
CUSTOMER_DF = CUSTOMER_DF.withColumn("AREA_CODE", lit(random.choice(AREA_CODE_LIST))) #intermediate area code column which will be concatenated to the mobile number in the SQL query and formatted as per mapping document
CUSTOMER_DF = CUSTOMER_DF.withColumn("AREA_CODE", concat(lit("("),CUSTOMER_DF['AREA_CODE'],lit(")"))) #adding () to the area code as per formatting required
CUSTOMER_DF = CUSTOMER_DF.withColumn("CUST_PHONE",concat(CUSTOMER_DF["CUST_PHONE"].substr(1,3),lit("-"),CUSTOMER_DF["CUST_PHONE"].substr(4,7))) #formatting number as per requirements
CUSTOMER_DF.createOrReplaceTempView("CUSTOMERDF") #creating a temporary database in spark to execute queries
CUSTOMER_DF = spark.sql("""SELECT CAST(SSN AS INT), 
                        CAST(FIRST_NAME AS STRING), 
                        CAST(MIDDLE_NAME AS STRING), 
                        CAST(LAST_NAME AS STRING), 
                        CAST(CREDIT_CARD_NO AS STRING) AS Credit_card_no, 
                        CAST(FULL_STREET_ADDRESS AS STRING), 
                        CAST(CUST_CITY AS STRING), 
                        CAST(CUST_STATE AS STRING), 
                        CAST(CUST_COUNTRY AS STRING), 
                        CAST(CONCAT(AREA_CODE, CUST_PHONE) AS STRING) AS CUST_PHONE, 
                        CAST(CUST_ZIP AS INT), 
                        CAST(CUST_EMAIL AS STRING), 
                        CAST(LAST_UPDATED AS TIMESTAMP) 
                        FROM CUSTOMERDF""") #query to get all data required as per mapping document and put into a dataframe

#Once the dataframe is created, we have to store it into the creditcard_capstone database. This can be done using the following code : 

CUSTOMER_DF.write.format("jdbc") \
  .mode("append") \
  .option("url", credentials['url']) \
  .option("dbtable", "creditcard_capstone.CDW_SAPP_CUSTOMER") \
  .option("user", credentials['user']) \
  .option("password", credentials['password']) \
  .save()

#To ensure that we know that the database contains the table
print("\nx----------------CDW_SAPP_CUSTOMER CREATED AND IN CREDITCARD_CAPSTONE DATABASE---------------x \n")
CUSTOMER_DF.show()

BRANCH_DF = spark.read.json("cdw_sapp_branch.json")
BRANCH_DF = BRANCH_DF.withColumn("BRANCH_CODE", BRANCH_DF["BRANCH_CODE"])
BRANCH_DF = BRANCH_DF.withColumn("BRANCH_NAME", BRANCH_DF['BRANCH_NAME'])
BRANCH_DF = BRANCH_DF.withColumn("BRANCH_STREET", BRANCH_DF['BRANCH_STREET'])
BRANCH_DF = BRANCH_DF.withColumn("BRANCH_CITY", BRANCH_DF['BRANCH_CITY'])
BRANCH_DF = BRANCH_DF.withColumn("BRANCH_STATE", BRANCH_DF['BRANCH_STATE'])
BRANCH_DF = BRANCH_DF.withColumn("BRANCH_ZIP", BRANCH_DF['BRANCH_ZIP'])
BRANCH_DF = BRANCH_DF.fillna(99999,subset = ['BRANCH_ZIP']) #to fill null values with 99999 in the branch_zip column
BRANCH_DF = BRANCH_DF.withColumn("BRANCH_PHONE", concat(lit("("),BRANCH_DF['BRANCH_PHONE'].substr(1,3),lit(")"),BRANCH_DF['BRANCH_PHONE'].substr(4,3),lit("-"),BRANCH_DF['BRANCH_PHONE'].substr(7,4)))
BRANCH_DF = BRANCH_DF.withColumn("LAST_UPDATED", BRANCH_DF['LAST_UPDATED'])
BRANCH_DF.createOrReplaceTempView("BRANCHDF")
BRANCH_DF = spark.sql("""SELECT CAST(BRANCH_CODE AS INT), 
                      CAST(BRANCH_NAME AS STRING), 
                      CAST(BRANCH_STREET AS STRING), 
                      CAST(BRANCH_CITY AS STRING), 
                      CAST(BRANCH_STATE AS STRING), 
                      CAST(BRANCH_ZIP AS INT), 
                      CAST(BRANCH_PHONE AS STRING), 
                      CAST(LAST_UPDATED AS TIMESTAMP) 
                      FROM BRANCHDF""")


BRANCH_DF.write.format("jdbc") \
  .mode("append") \
  .option("url", credentials['url']) \
  .option("dbtable", "creditcard_capstone.CDW_SAPP_BRANCH") \
  .option("user", credentials['user']) \
  .option("password", credentials['password']) \
  .save()

#To ensure that we know that the database contains the table
print("x---------------CDW_SAPP_BRANCH CREATED AND IN CREDITCARD_CAPSTONE DATABASE----------------x\n")
BRANCH_DF.show()

CREDIT_DF = spark.read.json("cdw_sapp_credit.json")
CREDIT_DF = CREDIT_DF.withColumn("CUST_CC_NO", CREDIT_DF["CREDIT_CARD_NO"])
CREDIT_DF = CREDIT_DF.withColumn("DAY", CREDIT_DF['DAY'])
CREDIT_DF = CREDIT_DF.withColumn("MONTH", CREDIT_DF['MONTH'])
CREDIT_DF = CREDIT_DF.withColumn("YEAR", CREDIT_DF['YEAR'])
CREDIT_DF = CREDIT_DF.withColumn("TIMEID", concat(CREDIT_DF['YEAR'], lpad(CREDIT_DF['MONTH'],2,"0"), lpad(CREDIT_DF['DAY'],2,"0")))
CREDIT_DF = CREDIT_DF.withColumn("CUST_SSN", CREDIT_DF['CUST_SSN'])
CREDIT_DF = CREDIT_DF.withColumn("BRANCH_CODE", CREDIT_DF['BRANCH_CODE'])
CREDIT_DF = CREDIT_DF.withColumn("TRANSACTION_TYPE", CREDIT_DF['TRANSACTION_TYPE'])
CREDIT_DF = CREDIT_DF.withColumn("TRANSACTION_VALUE", CREDIT_DF['TRANSACTION_VALUE'])
CREDIT_DF = CREDIT_DF.withColumn("TRANSACTION_ID", CREDIT_DF['TRANSACTION_ID'])
CREDIT_DF.createOrReplaceTempView("CREDITDF")
CREDIT_DF = spark.sql("""SELECT CAST(CUST_CC_NO AS STRING), 
                      CAST(TIMEID AS STRING), 
                      CAST(CUST_SSN AS INT), 
                      CAST(BRANCH_CODE AS INT), 
                      CAST(TRANSACTION_TYPE AS STRING), 
                      CAST(TRANSACTION_VALUE AS DOUBLE), 
                      CAST(TRANSACTION_ID AS INT)
                      FROM CREDITDF""")

CREDIT_DF.write.format("jdbc") \
  .mode("append") \
  .option("url", credentials['url']) \
  .option("dbtable", "creditcard_capstone.CDW_SAPP_CREDIT_CARD") \
  .option("user", credentials['user']) \
  .option("password", credentials['password']) \
  .save()

#To ensure that we know that the database contains the table
print("x----------------CDW_SAPP_CREDIT_CARD CREATED AND IN CREDITCARD_CAPSTONE DATABASE----------------x\n")
CREDIT_DF.show()

print("This marks the end of Part 1 of the Capstone. \n")
print('x-----------------------------------------------------------------------------------------------x \n')
state_abbreviation_to_name_dictionary = {'AL': 'Alabama','AK': 'Alaska','AZ': 'Arizona','AR': 'Arkansas','CA': 'California','CO': 'Colorado','CT': 'Connecticut','DE': 'Delaware','FL': 'Florida','GA': 'Georgia','HI': 'Hawaii','ID': 'Idaho','IL': 'Illinois','IN': 'Indiana','IA': 'Iowa','KS': 'Kansas','KY': 'Kentucky','LA': 'Louisiana','ME': 'Maine','MD': 'Maryland','MA': 'Massachusetts','MI': 'Michigan','MN': 'Minnesota','MS': 'Mississippi','MO': 'Missouri','MT': 'Montana','NE': 'Nebraska','NV': 'Nevada','NH': 'New Hampshire','NJ': 'New Jersey','NM': 'New Mexico','NY': 'New York','NC': 'North Carolina','ND': 'North Dakota','OH': 'Ohio','OK': 'Oklahoma','OR': 'Oregon','PA': 'Pennsylvania','RI': 'Rhode Island','SC': 'South Carolina','SD': 'South Dakota','TN': 'Tennessee','TX': 'Texas','UT': 'Utah','VT': 'Vermont','VA': 'Virginia','WA': 'Washington','WV': 'West Virginia','WI': 'Wisconsin','WY': 'Wyoming'}
month_number_to_name_dictionary = {'01': 'January','02': 'February','03': 'March','04': 'April','05': 'May','06': 'June','07': 'July','08': 'August','09': 'September','10': 'October','11': 'November','12': 'December'}
#PART 2 CAPSTONE
#Here we have to build a console based program to ask the user for inputs and generate outputs based on inputs from user on transactional or customer data
#TRANSACTIONAL DATA : TRANSACTIONS FOR A GIVEN ZIP CODE IN A GIVEN MONTH AND YEAR ORDERED BY TRANSACTION DAY IN DESCENDING ORDER
print("Welcome to the console python program. Here you can find out information about almost anything (within reason). \n Please ensure your data is being entered correctly.")
def transactions_by_zip_month_year():
    while True:
        try:
            conn = mydbconnection.connect(database='creditcard_capstone', user=credentials['user'], password=credentials['password'], port=3306)
            cursor = conn.cursor()

            input_zip = input("\nEnter the ZIP code : ")
            while not input_zip.isnumeric() or len(str(input_zip)) != 5:
                input_zip = input("Please enter a valid zipcode : ")

            input_month = input("\nEnter the month : ")
            input_month = input_month.zfill(2)
            while int(input_month) < 1 or int(input_month) > 12 or not input_month.isnumeric():
                input_month = input("Please enter a valid month number (1-12) : ")
                
            input_year = input("\nEnter the year : ")
            while int(input_year) < 1950 or int(input_year) > 2023 or not input_year.isnumeric():
                input_year = input("Please enter a valid year (1950-2023) : ")

            transactions_by_zip_month_year_query = f"""SELECT 
            SUBSTRING(CDW_SAPP_CREDIT_CARD.TIMEID, 7, 2), 
            CDW_SAPP_CREDIT_CARD.TRANSACTION_ID, 
            CDW_SAPP_CREDIT_CARD.TRANSACTION_TYPE, 
            CONCAT('$', CDW_SAPP_CREDIT_CARD.TRANSACTION_VALUE) 
            FROM CDW_SAPP_CREDIT_CARD JOIN CDW_SAPP_CUSTOMER 
            ON CDW_SAPP_CREDIT_CARD.CUST_SSN = CDW_SAPP_CUSTOMER.SSN 
            WHERE CDW_SAPP_CUSTOMER.CUST_ZIP = '{input_zip}' 
            AND SUBSTRING(CDW_SAPP_CREDIT_CARD.TIMEID, 5, 2) = '{input_month}' 
            AND SUBSTRING(CDW_SAPP_CREDIT_CARD.TIMEID, 1, 4) = '{input_year}' 
            ORDER BY SUBSTRING(CDW_SAPP_CREDIT_CARD.TIMEID, 7, 2) DESC;"""
            
            cursor.execute(transactions_by_zip_month_year_query)
            transactions_by_zip_month_year_result = cursor.fetchall()
            
            if len(transactions_by_zip_month_year_result) == 0:
                print("\nNo transaction details found for the above zip code, month and year")
            
            for row in transactions_by_zip_month_year_result:
                transaction_day = row[0]
                transaction_id = row[1]
                transaction_type = row[2]
                transaction_value = row[3]
                
                print(f"TRANSACTION DAY: {transaction_day}, TRANSACTION ID: {transaction_id}, TRANSACTION TYPE: {transaction_type}, TRANSACTION VALUE : {transaction_value}")
            
            break

        except Error as e:
            print(f"""Error: '{e}'""")
        
        finally:
            cursor.close()
            conn.close()

#TRANSACTIONAL DATA : GET THE TRANSACTION COUNT, TRANSACTION SUM ON THE BASIS OF TRANSACTION TYPE ENTERED BY USER
def transactions_by_type():
    while True:
        try:
            conn = mydbconnection.connect(database='creditcard_capstone', user=credentials['user'], password=credentials['password'], port=3306)
            cursor = conn.cursor()

            input_category = input("\nEnter a category from the following - Entertainment, Bills, Healthcare, Education, Gas, Test, Grocery : "  )
            input_category = input_category.title()
            while input_category not in ['Entertainment', 'Bills', 'Healthcare', 'Education', 'Gas', 'Test', 'Grocery']:
                input_category = input("Please enter a valid category : ")
                input_category = input_category.title()

            transactions_by_type_query = f"""SELECT 
            CDW_SAPP_CREDIT_CARD.TRANSACTION_TYPE, 
            COUNT(CDW_SAPP_CREDIT_CARD.TRANSACTION_TYPE), 
            CONCAT('$', ROUND(SUM(CDW_SAPP_CREDIT_CARD.TRANSACTION_VALUE), 2)) 
            FROM CDW_SAPP_CREDIT_CARD 
            WHERE CDW_SAPP_CREDIT_CARD.TRANSACTION_TYPE = '{input_category}';"""

            cursor.execute(transactions_by_type_query)
            transactions_by_type_result = cursor.fetchall()
            
            for row in transactions_by_type_result:
                transaction_category = row[0]
                transaction_count = row[1]
                transaction_sum = row[2]

                print(f"\nCATEGORY: {transaction_category}, NUMBER OF TRANSACTIONS: {transaction_count}, TRANSACTION SUM: {transaction_sum}")

            break
        
        except Error as e:
            print(f"""Error: '{e}'""")
        
        finally:
            cursor.close()
            conn.close()


#TRANSACTIONAL DATA : GET THE TRANSACTION COUNT AND TRANSACTION SUM ON THE BASIS OF STATE ENTERED BY USER
def transactions_by_branch():
    while True:
        try:
            conn = mydbconnection.connect(database='creditcard_capstone', user=credentials['user'], password=credentials['password'], port=3306)
            cursor = conn.cursor()
            input_state_abbreviation = input("\nPlease enter a 2 lettered state abbreviation : ")
            input_state_abbreviation = input_state_abbreviation.upper()
            while input_state_abbreviation not in ['AL','AK', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'MD', 'MN', 'ME', 'MA', 'MI', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'SD', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'TX', 'TN', 'UT', 'VT', 'VA', 'WA', 'WY', 'WI', 'WV']:
                input_state_abbreviation = input("Please enter a valid state abbreviation : ")

            transactions_by_branch_query = f"""SELECT 
            CDW_SAPP_BRANCH.BRANCH_STATE, 
            COUNT(CDW_SAPP_CREDIT_CARD.TRANSACTION_VALUE), 
            CONCAT('$', ROUND(SUM(CDW_SAPP_CREDIT_CARD.TRANSACTION_VALUE), 2)) 
            FROM CDW_SAPP_CREDIT_CARD JOIN CDW_SAPP_BRANCH 
            ON CDW_SAPP_CREDIT_CARD.BRANCH_CODE = CDW_SAPP_BRANCH.BRANCH_CODE 
            WHERE CDW_SAPP_BRANCH.BRANCH_STATE = '{input_state_abbreviation}';"""
            
            cursor.execute(transactions_by_branch_query)
            transactions_by_branch_result = cursor.fetchall()
            
            for row in transactions_by_branch_result:
                branch_state = row[0]
                branch_transaction_count = row[1]
                branch_transaction_sum = row[2]
                if branch_state is None and branch_transaction_sum is None:
                    branch_state = state_abbreviation_to_name_dictionary[input_state_abbreviation]
                    branch_transaction_sum = "$0.00"
                    print(f"\nSTATE: {branch_state}, NUMBER OF TRANSACTIONS: {branch_transaction_count}, TRANSACTION SUM: {branch_transaction_sum}")
                else:
                    print(f"\nSTATE: {state_abbreviation_to_name_dictionary[branch_state]}, NUMBER OF TRANSACTIONS: {branch_transaction_count}, TRANSACTION SUM: {branch_transaction_sum}")
            
            break

        except Error as e:
            print(f"""Error: '{e}'""")
        
        finally:
            cursor.close()
            conn.close()

#CUSTOMER DATA : TO CHECK IF A CUSTOMER EXISTS IN THE SYSTEM AND OUTPUTS THE DETAILS ON THE BASIS OF CCN OR SSN ENTERED BY THE USER
def customer_details():
    while True:
        try:
            conn = mydbconnection.connect(database='creditcard_capstone', user=credentials['user'], password=credentials['password'], port=3306)
            cursor = conn.cursor()
            input_ssn_or_ccn = input("\nYou can choose to search for a customer using their card number or their social security number. Enter CCN to search by card number or SSN to search by SSN : ")
            input_ssn_or_ccn = input_ssn_or_ccn.upper()
            
            while input_ssn_or_ccn not in ['SSN','CCN']:
                input_ssn_or_ccn = input("Invalid way to find customer info, please enter CCN or SSN to find customer details : ")
                input_ssn_or_ccn = input_ssn_or_ccn.upper()

            if str(input_ssn_or_ccn) == 'CCN':
                input_card_number = input("\nEnter the 16 digit credit card number : ")
                while len(input_card_number) != 16 or not input_card_number.isnumeric():
                    input_card_number = input("Please enter a valid 16 digit card number : ")

                customer_details_query = f"""SELECT * 
                FROM CDW_SAPP_CUSTOMER 
                WHERE CDW_SAPP_CUSTOMER.Credit_card_no = '{input_card_number}';"""
                
                cursor.execute(customer_details_query)
                customer_details_result = cursor.fetchall()
                
                if len(customer_details_result) == 0:
                    print("\nNo customer details found for this credit card number")
                
                for row in customer_details_result:
                    cust_ssn = row[0]
                    cust_first_name = row[1]
                    cust_middle_name = row[2]
                    cust_last_name = row[3]
                    cust_credit_card_no = row[4]
                    cust_address = row[5]
                    cust_city = row[6]
                    cust_state = row[7]
                    cust_country = row[8]
                    cust_phone = row[9]
                    cust_zip = row[10]
                    cust_email = row[11]
                    cust_last_updated = row[12]
                    print(f"\n SSN: {cust_ssn}\n FIRST NAME: {cust_first_name}\n MIDDLE NAME: {cust_middle_name}\n LAST NAME: {cust_last_name}\n CREDIT CARD NUMBER: {cust_credit_card_no}\n ADDRESS: {cust_address}\n CITY: {cust_city}\n STATE: {cust_state}\n COUNTRY: {cust_country}\n PHONE: {cust_phone}\n ZIP: {cust_zip}\n EMAIL: {cust_email}\n LAST UPDATED: {cust_last_updated}")
                    

            elif str(input_ssn_or_ccn).upper() == 'SSN':
                input_ssn = input("\nEnter the 9 digit Social Security Number : ")
                while len(input_ssn) != 9 or not input_ssn.isnumeric():
                    input_ssn = input("Please enter a valid 9 digit Social Security Number : ")

                transactions_query = f"""SELECT * 
                FROM CDW_SAPP_CUSTOMER 
                WHERE CDW_SAPP_CUSTOMER.SSN = '{input_ssn}';"""
                
                cursor.execute(transactions_query)
                customer_detail_results = cursor.fetchall()

                if len(customer_detail_results) == 0:
                    print("\nNo customer details found for the above Social Security Number.")

                for row in customer_detail_results:
                    cust_ssn = row[0]
                    cust_first_name = row[1]
                    cust_middle_name = row[2]
                    cust_last_name = row[3]
                    cust_credit_card_no = row[4]
                    cust_address = row[5]
                    cust_city = row[6]
                    cust_state = row[7]
                    cust_country = row[8]
                    cust_phone = row[9]
                    cust_zip = row[10]
                    cust_email = row[11]
                    cust_last_updated = row[12]
                    
                    print(f"\n SSN: {cust_ssn}\n FIRST NAME: {cust_first_name}\n MIDDLE NAME: {cust_middle_name}\n LAST NAME: {cust_last_name}\n CREDIT CARD NUMBER: {cust_credit_card_no}\n ADDRESS: {cust_address}\n CITY: {cust_city}\n STATE: {cust_state}\n COUNTRY: {cust_country}\n PHONE: {cust_phone}\n ZIP: {cust_zip}\n EMAIL: {cust_email}\n LAST UPDATED: {cust_last_updated}")

            else:
                print("Invalid input")
            
            break
        
        except Error as e:
            print(f"""Error: '{e}'""")
        
        finally:
            cursor.close()
            conn.close()

#CUSTOMER DETAILS : UPDATE THE CUSTOMER DETAILS ON THE BASIS OF SSN ENTERED

def update_details():
    while True:
        try:
            conn = mydbconnection.connect(database='creditcard_capstone', user=credentials['user'], password=credentials['password'], port=3306)
            cursor = conn.cursor()
            input_social_security = input("Use the customer whose records you want to update using their SSN. Remember, we will be updating information about this customer : ")

            
            while len(input_social_security) != 9 or not input_social_security.isnumeric():
                input_social_security = input("Please enter a valid 9 dight SSN : ")

            customer_ssn_exist_check_query = f"""SELECT * 
            FROM CDW_SAPP_CUSTOMER 
            WHERE CDW_SAPP_CUSTOMER.SSN = {input_social_security};"""

            cursor.execute(customer_ssn_exist_check_query)
            customer_exist_check_result = cursor.fetchall()

            if len(customer_exist_check_result) == 0:
                input_social_security = input("\nA customer with this SSN does not bank with us. Please enter a SSN of an existing customer : ")

            for row in customer_exist_check_result:
                cust_ssn = row[0]
                cust_first_name = row[1]
                cust_middle_name = row[2]
                cust_last_name = row[3]
                cust_credit_card_no = row[4]
                cust_address = row[5]
                cust_city = row[6]
                cust_state = row[7]
                cust_country = row[8]
                cust_phone = row[9]
                cust_zip = row[10]
                cust_email = row[11]
                cust_last_updated = row[12]
                
                print(f"\nCustomer Details prior to update\n SSN: {cust_ssn}\n FIRST NAME: {cust_first_name}\n MIDDLE NAME: {cust_middle_name}\n LAST NAME: {cust_last_name}\n CREDIT CARD NUMBER: {cust_credit_card_no}\n ADDRESS: {cust_address}\n CITY: {cust_city}\n STATE: {cust_state}\n COUNTRY: {cust_country}\n PHONE: {cust_phone}\n ZIP: {cust_zip}\n EMAIL: {cust_email}\n LAST UPDATED: {cust_last_updated}\n")
            
            change_first_name_or_not = input("\nDo you want to update the customer's first name? Hit Y for yes or any other character for no : ")
            if change_first_name_or_not in ['y','Y']:
                ufirst_name = input("Please enter the updated first name : ")
                while not ufirst_name.isalpha():
                    ufirst_name = input("Please enter a valid first name : ")
                ufirst_name = ufirst_name.title()
            else:
                ufirst_name = cust_first_name
            
            change_middle_name_or_not = input("\nDo you want to update the customer's middle name? Hit Y for yes or any other character for no : ")
            if change_middle_name_or_not in ['y','Y']:
                umiddle_name = input("Please enter the updated middle name : ")
                while not umiddle_name.isalpha():
                    umiddle_name = input("Please enter a valid middle name : ")
                umiddle_name = umiddle_name.lower()
            else:
                umiddle_name = cust_middle_name

            change_last_name_or_not = input("\nDo you want to update the customer's last name? Hit Y for yes or any other character for no : ")
            if change_last_name_or_not in ['y','Y']:
                ulast_name = input("Please enter the updated last name : ")
                while not ulast_name.isalpha():
                    ulast_name = input("Please enter a valid last name : ")
                ulast_name = ulast_name.title()
            else:
                ulast_name = cust_last_name
            
            change_ccn_or_not = input("\nDo you want to update the customer's credit card number? Hit Y for yes or any other character for no : ")
            if change_ccn_or_not in ['y','Y']:
                ucc_no = input("Please enter the updated credit card number : ")
                while len(ucc_no) != 16 or not ucc_no.isnumeric():
                    ucc_no = input("Please enter a 16 digit credit card number seen on your new card : ")
            else:
                ucc_no = cust_credit_card_no

            change_address_or_not = input("\nDo you want to update the customer's address? Hit Y for yes or any other character for no : ")
            if change_address_or_not in ['y','Y']:
                uaddress = input("Please enter the updated address in the format (number),(street name) : ")
            else:
                uaddress = cust_address

            change_city_or_not = input("\nDo you want to update the customer's city? Hit Y for yes or any other character for no : ")
            if change_city_or_not in ['y','Y']:
                ucity = input("Please enter the updated city : ")
                while ucity.title() not in US_CITIES_LIST:
                    ucity = input("This city doesn't exist in the USA. Please enter a valid city in USA : ")
                ucity = ucity.title()
            else:
                ucity = cust_city
            
            change_state_or_not = input("\nDo you want to update the customer's state? Hit Y for yes or any other character for no : ")
            if change_state_or_not in ['y','Y']:
                ustate = input("Please enter the updated state abbreviation : ")
                ustate = ustate.upper()
                while ustate not in ['AL','AK', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'MD', 'MN', 'ME', 'MA', 'MI', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'SD', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'TX', 'TN', 'UT', 'VT', 'VA', 'WA', 'WY', 'WI', 'WV']:
                    ustate = input("Please enter a valid state abbreviation : ")
                ustate = ustate.upper()
            else:
                ustate = cust_state
            
            change_phone_or_not = input("\nDo you want to update the customer's phone number? Hit Y for yes or any other character for no : ")
            if change_phone_or_not in ['y','Y']:
                uphone = input("Please enter the updated phone number : ")
                while len(uphone) != 10 or not uphone.isnumeric():
                    uphone = input("Please enter a valid mobile number : ")
                while int(uphone[0:3]) not in AREA_CODE_LIST:
                    uphone = input("Please enter a mobile number with a valid area code : ")
                uphone = "(" + str(uphone[0:3]) + ")" + str(uphone[3:6]) + "-" + uphone[6:10]
            else:
                uphone = cust_phone
            
            change_zip_or_not = input("\nDo you want to update the customer's zipcode? Hit Y for yes or any other character for no : ")
            if change_zip_or_not in ['y','Y']:
                uzip = input("Please enter the updated zip : ")
                while not uzip.isnumeric() or int(uzip) not in US_CITIES_ZIPCODES_LIST:
                    uzip = input("Please enter a valid zipcode in the USA : ")
            else:
                uzip = cust_zip
            
            change_email_or_not = input("\nDo you want to update the customer's email? Hit Y for yes or any other character for no : ")
            if change_email_or_not in ['y','Y']:
                uemail = input("Please enter the updated email address : ")
                email_format = r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"
                while not re.fullmatch(email_format,uemail):
                    uemail = input("Please enter a vaild email address : ")
            else:
                uemail = cust_email
            
            utime = datetime.datetime.now()
            
            update_details_query = f"""UPDATE CDW_SAPP_CUSTOMER 
            SET CDW_SAPP_CUSTOMER.FIRST_NAME = '{ufirst_name}', 
            CDW_SAPP_CUSTOMER.MIDDLE_NAME = '{umiddle_name}', 
            CDW_SAPP_CUSTOMER.LAST_NAME = '{ulast_name}', 
            CDW_SAPP_CUSTOMER.Credit_card_no = '{ucc_no}', 
            CDW_SAPP_CUSTOMER.FULL_STREET_ADDRESS = '{uaddress}', 
            CDW_SAPP_CUSTOMER.CUST_CITY = '{ucity}', 
            CDW_SAPP_CUSTOMER.CUST_STATE = '{ustate}', 
            CDW_SAPP_CUSTOMER.CUST_PHONE = '{uphone}', 
            CDW_SAPP_CUSTOMER.CUST_ZIP = '{uzip}', 
            CDW_SAPP_CUSTOMER.CUST_EMAIL = '{uemail}',
            CDW_SAPP_CUSTOMER.LAST_UPDATED = '{utime}'
            WHERE CDW_SAPP_CUSTOMER.SSN = '{input_social_security}';"""
            
            cursor.execute(update_details_query)
            
            select_update_details_query = f"""SELECT * 
            FROM CDW_SAPP_CUSTOMER WHERE CDW_SAPP_CUSTOMER.SSN = '{input_social_security}';"""
            
            cursor.execute(select_update_details_query)
            select_update_details_result = cursor.fetchall()
            
            for row in select_update_details_result:
                cust_ssn = row[0]
                cust_first_name = row[1]
                cust_middle_name = row[2]
                cust_last_name = row[3]
                cust_credit_card_no = row[4]
                cust_address = row[5]
                cust_city = row[6]
                cust_state = row[7]
                cust_country = row[8]
                cust_phone = row[9]
                cust_zip = row[10]
                cust_email = row[11]
                cust_last_updated = row[12]
                
                print(f"\nCustomer Details after update\n SSN: {cust_ssn}\n FIRST NAME: {cust_first_name}\n MIDDLE NAME: {cust_middle_name}\n LAST NAME: {cust_last_name}\n CREDIT CARD NUMBER: {cust_credit_card_no}\n ADDRESS: {cust_address}\n CITY: {cust_city}\n STATE: {cust_state}\n COUNTRY: {cust_country}\n PHONE: {cust_phone}\n ZIP: {cust_zip}\n EMAIL: {cust_email}\n LAST UPDATED: {cust_last_updated}\n")
                print("\nData Updated Successfully")
            
            break

        except Error as e:
            print(f"""Error: '{e}'""")
        
        finally:
            cursor.close()
            conn.close()

#GENERATE A MONTHLY BILL ON THE BASIS OF CREDIT CARD NUMBER ENTERED AND MONTH AND YEAR ENTERED
def monthly_bill():
    while True:
        try:
            conn = mydbconnection.connect(database='creditcard_capstone', user=credentials['user'], password=credentials['password'], port=3306)
            cursor = conn.cursor()
            
            input_card_number = input("\nPlease enter your credit card number to generate a monthly bill for : ")
            while len(input_card_number) != 16 or not input_card_number.isnumeric():
                input_card_number = input("Please enter a valid 16 digit card number : ")
                
            input_month = input("\nEnter a month to generate a monthly bill for : ")
            if len(input_month) == 1:
                input_month = input_month.zfill(2)
            while int(input_month) < 1 or int(input_month) > 12 or not input_month.isnumeric():
                input_month = input("Please enter a valid month number (1-12) : ")
                
            input_year = input("\nEnter a year to generate a monthly bill for : ")
            while int(input_year) < 1950 or int(input_year) > 2023 or not input_year.isnumeric():
                input_year = input("Please enter a valid year (1950-2023) : ")
                
            monthly_bill_query = f"""SELECT 
            CONCAT('$', ROUND(SUM(CDW_SAPP_CREDIT_CARD.TRANSACTION_VALUE), 2)) 
            FROM CDW_SAPP_CREDIT_CARD 
            WHERE SUBSTRING(CDW_SAPP_CREDIT_CARD.TIMEID, 5, 2) = '{input_month}' 
            AND SUBSTRING(CDW_SAPP_CREDIT_CARD.TIMEID, 1, 4) = '{input_year}' 
            AND CDW_SAPP_CREDIT_CARD.CUST_CC_NO = '{input_card_number}';"""
            
            cursor.execute(monthly_bill_query)
            monthly_bill_result = cursor.fetchall()
            
            for row in monthly_bill_result:
                bill_amount = row[0]
                if bill_amount is None:
                    bill_amount = "$0.00"
                print(f"\nBILL AMOUNT: {bill_amount} FOR THE MONTH OF {month_number_to_name_dictionary[input_month]} OF THE YEAR {input_year}")
            
            break

        except Error as e:
            print(f"""Error: '{e}'""")
        
        finally:
            cursor.close()
            conn.close()

#TO FIND TRANSACTION DETAILS ON THE BASIS OF A RANGE OF DATES
def transaction_in_range():
    while True:
        try:
            conn = mydbconnection.connect(database='creditcard_capstone', user=credentials['user'], password=credentials['password'], port=3306)
            cursor = conn.cursor()
            input_card_number = input("\nPlease enter your credit card number to find the transaction data of transactions made between two dates : ")
            while len(input_card_number) != 16 or not input_card_number.isnumeric():
                input_card_number = input("Please enter a valid 16 digit card number : ")

            start_date = input("\nPlease enter the start date for the range in the format xxxx/xx/xx (YYYY/MM/DD) : ")
            start_date_parts = start_date.split('/')
            
            while len(start_date_parts) != 3:
                start_date = input("The date formatting is not correct, please reenter your date with the correct format (YYYY/MM/DD) : ")
            
            start_date_year = start_date_parts[0]
            start_date_month = start_date_parts[1]
            start_date_day = start_date_parts[2]
            
            while int(start_date_year) < 1950 or int(start_date_year) > 2023 or len(str(start_date_year)) > 4 or not start_date_year.isnumeric():
                start_date_year = input("The date year is not correct, please reenter the correct year (1950-2023) : ")
            
            if len(start_date_month) == 1:
                start_date_month = "0" + str(start_date_month)
            while int(start_date_month) < 1 or int(start_date_month) > 12 or len(str(start_date_month)) > 2 or not start_date_month.isnumeric():
                start_date_month = input("The date month is not correct, please reenter the correct month (01-12) : ")

            if len(start_date_day) == 1:
                start_date_day = "0" + str(start_date_day)
            while int(start_date_day) < 1 or int(start_date_day) > 28 or len(str(start_date_day)) > 2 or not start_date_day.isnumeric():
                start_date_day = input("The date day is not correct, please reenter the correct day (01-28) : ")
            
            start_date_id = str(start_date_year) + str(start_date_month) + str(start_date_day)

            end_date = input("\nPlease enter the end date for the range in the format xxxx/xx/xx (YYYY/MM/DD) : ")
            end_date_parts = end_date.split('/')

            while len(end_date_parts) != 3:
                start_date = input("The date formatting is not correct, please reenter your date with the correct format (YYYY/MM/DD) : ")
            
            end_date_year = end_date_parts[0]
            end_date_month = end_date_parts[1]
            end_date_day = end_date_parts[2]

            while int(end_date_year) < 1950 or int(end_date_year) > 2023 or len(str(end_date_year)) > 4 or not end_date_year.isnumeric():
                end_date_year = input("The date year is not correct, please reenter the correct year (1950-2023) : ")
            
            while len(end_date_month) == 1:
                end_date_month = "0" + str(end_date_month)
            while int(end_date_month) < 1 or int(end_date_month) > 12 or len(end_date_month) > 2 or len(str(start_date_month)) > 2 or not end_date_month.isnumeric():
                end_date_month = input("The date month is not correct, please reenter the correct month (01/12) : ")

            while len(end_date_day) == 1:
                end_date_day = "0" + str(end_date_day)
            while int(end_date_day) < 1 or int(end_date_day) > 28 or len(str(end_date_day)) > 2 or not end_date_day.isnumeric():
                end_date_day = input("The date day is not correct, please reenter the correct day (01-28) : ")
            
            end_date_id = str(end_date_year) + str(end_date_month) + str(end_date_day)

            transaction_in_range_query = f"""SELECT 
            CONCAT(SUBSTRING(CDW_SAPP_CREDIT_CARD.TIMEID,1,4),'/', 
            SUBSTRING(CDW_SAPP_CREDIT_CARD.TIMEID,5,2),'/', 
            SUBSTRING(CDW_SAPP_CREDIT_CARD.TIMEID,7,2)),
            CDW_SAPP_CREDIT_CARD.TRANSACTION_ID, 
            CDW_SAPP_CREDIT_CARD.TRANSACTION_TYPE, 
            CONCAT('$', ROUND(CDW_SAPP_CREDIT_CARD.TRANSACTION_VALUE, 2)) 
            FROM CDW_SAPP_CREDIT_CARD 
            WHERE CDW_SAPP_CREDIT_CARD.TIMEID BETWEEN '{start_date_id}' AND '{end_date_id}' 
            AND CDW_SAPP_CREDIT_CARD.CUST_CC_NO = '{input_card_number}' 
            ORDER BY CDW_SAPP_CREDIT_CARD.TIMEID DESC;"""
            
            cursor.execute(transaction_in_range_query)
            transaction_in_range_result = cursor.fetchall()

            print(f"The transactions in the given date range : {start_date} - {end_date}")

            for row in transaction_in_range_result:
                transaction_timeid = row[0]
                transaction_id = row[1]
                transaction_type = row[2]
                transaction_value = row[3]
                print(f"\nDATE: {transaction_timeid}, TRANSACTION ID: {transaction_id}, TRANSACTION TYPE: {transaction_type}, TRANSACTION VALUE: {transaction_value}")
            
            break

        except Error as e:
            print(f"""Error: '{e}'""")
        finally:
            cursor.close()
            conn.close()


def console_program():
    while True:
        user_input = input("\nPlease enter T to see Transaction Data\nC to see Customer Data\nX to exit out of the program : ")
        if user_input in ['t','T']:
            user_input1 = input("\nEnter 1 to display the transactions made by customers living in a given zip code for a given month and year ordered by day in descending order\n2 to display the number and total values of transactions for a given type \n 3 to display the total number and total values of transactions for branches in a given state : ")

            if str(user_input1) == '1':
                print("\nTransactions will be displayed on the basis of zipcode, month and year ordered by day in descending order")
                transactions_by_zip_month_year()
            elif str(user_input1) == '2':
                print("\nTransaction count and sum will be displayed for a given category")
                transactions_by_type()
            elif str(user_input1) == '3':
                print("\nTransaction count and sum will be displayed for a given state")
                transactions_by_branch()
            else:
                print("\nInvalid input")

        elif user_input in ['c','C']:
            user_input2 = input("\nEnter 1 to check the existing account details of a customer \n 2 to modify the existing account details of a customer \n 3 to generate a monthly bill for a credit card number for a given month and year \n 4 to display the transactions made by a customer between two dates ordered by year, month, and day in descending order : ")

            if str(user_input2) == '1':
                print("\nDetails of an existing customer will be displayed")
                customer_details()
            elif str(user_input2) == '2':
                print("\nUpdating current customer details")
                update_details()
            elif str(user_input2) == '3':
                print("\nMonthly bill will be generated")
                monthly_bill()
            elif str(user_input2) == '4':
                print("\nTransaction details will be displayed in a given range of dates")
                transaction_in_range()
            else:
                print("\nInvalid input")
        
        elif user_input in ['x','X']:
            print("\nThanks for using the console program.")
            break

        else:
            print("\nInvalid input,please try again")
            continue        

if __name__ == "__main__":
    console_program()

print("This marks the end of Part 2 of the Capstone. \n")
print('x---------------------------------------------------------------------------------------x \n')
time.sleep(5)

conn = mydbconnection.connect(database='creditcard_capstone', user=credentials['user'], password=credentials['password'], port=3306)
transactions_query = f"""SELECT 
CDW_SAPP_CREDIT_CARD.TRANSACTION_TYPE AS `TRANSACTION TYPE`, 
COUNT(CDW_SAPP_CREDIT_CARD.TRANSACTION_ID) AS `TRANSACTION COUNT` 
FROM CDW_SAPP_CREDIT_CARD 
GROUP BY CDW_SAPP_CREDIT_CARD.TRANSACTION_TYPE;"""

TRANSACTION_DF = pd.read_sql(transactions_query,conn)

X = TRANSACTION_DF['TRANSACTION TYPE']
Y = TRANSACTION_DF['TRANSACTION COUNT']

plt.figure(figsize=(15, 15))
ax = plt.axes()
plt.bar(X,Y)

bars = plt.gca().patches
for bar in bars:
    if bar.get_height() == max(TRANSACTION_DF['TRANSACTION COUNT']):
        bar.set_color('red')

for bar in bars:
    plt.annotate(f'{bar.get_height()}', xy=(bar.get_x() + bar.get_width()/2, bar.get_height()), xytext=(0, 4), textcoords="offset points", ha='center', va='center')

plt.xlabel("Transaction Type")
plt.ylabel("Number Of Transactions")
plt.xticks(rotation = 30)
plt.yticks([6000,6100,6200,6300,6400,6500,6600,6700,6800,6900])
plt.ylim(6000,6900)
plt.title("Number of transactions vs category")

ax.yaxis.grid(True, linestyle = '--')
ax.xaxis.grid(True, linestyle = '--')

plt.savefig("Number_Of_Transactions_vs_Category.png")
plt.show()


time.sleep(3)

transactions_query = f"""SELECT 
CDW_SAPP_CUSTOMER.CUST_STATE AS `STATE`, 
COUNT(CDW_SAPP_CUSTOMER.CUST_STATE) AS `NUMBER OF CUSTOMERS` 
FROM CDW_SAPP_CUSTOMER 
GROUP BY CDW_SAPP_CUSTOMER.CUST_STATE;"""

STATE_DF = pd.read_sql(transactions_query,conn)

X = STATE_DF['STATE'].map(state_abbreviation_to_name_dictionary)
Y = STATE_DF['NUMBER OF CUSTOMERS']

plt.figure(figsize=(10, 10))
ax = plt.axes()
plt.bar(X,Y)

bars = plt.gca().patches
for bar in bars:
    if bar.get_height() == max(STATE_DF['NUMBER OF CUSTOMERS']):
        bar.set_color('red')

for bar in bars:
    plt.annotate(f'{bar.get_height()}', xy=(bar.get_x() + bar.get_width()/2, bar.get_height()), xytext=(0, 0.5), textcoords="offset points", ha='center', va='bottom')

plt.xlabel("States")
plt.ylabel("Number Of Customers")
plt.xticks(rotation=90)
plt.title("Number Of Customers vs State")

ax.xaxis.grid(True, linestyle = '--')
ax.yaxis.grid(True, linestyle = '--')

plt.savefig("Number_Of_Customers_vs_State.png")
plt.show()


time.sleep(3)

transactions_sum_query = f"""SELECT 
ROUND(SUM(CDW_SAPP_CREDIT_CARD.TRANSACTION_VALUE),2) AS `TRANSACTION SUM`,
CONCAT(CDW_SAPP_CUSTOMER.FIRST_NAME,' ',CDW_SAPP_CUSTOMER.MIDDLE_NAME,' ',CDW_SAPP_CUSTOMER.LAST_NAME) AS `CUSTOMER NAME` 
FROM CDW_SAPP_CREDIT_CARD JOIN CDW_SAPP_CUSTOMER 
ON CDW_SAPP_CREDIT_CARD.CUST_SSN = CDW_SAPP_CUSTOMER.SSN
GROUP BY  `CUSTOMER NAME`
ORDER BY `TRANSACTION SUM` DESC LIMIT 10;"""

TOPTENCUSTOMER_DF = pd.read_sql(transactions_sum_query,conn)

X = TOPTENCUSTOMER_DF['CUSTOMER NAME']
Y = TOPTENCUSTOMER_DF['TRANSACTION SUM']

plt.figure(figsize=(15, 15))
ax = plt.axes()
plt.bar(X,Y)

bars = plt.gca().patches
for bar in bars:
    if bar.get_height() == max(TOPTENCUSTOMER_DF['TRANSACTION SUM']):
        bar.set_color('red')

for bar in bars:
    plt.annotate(f'${bar.get_height()}', xy=(bar.get_x() + bar.get_width()/2, bar.get_height()), xytext=(0, 4), textcoords="offset points", ha='center', va='center')
plt.xlabel("Customer Name")
plt.ylabel("Transaction Sum")
plt.ylim(5000,5700)
plt.xticks(rotation = 90)
plt.title("Transaction Sum vs Customer")

ax.yaxis.grid(True, linestyle = '--')
ax.xaxis.grid(True, linestyle = '--')

plt.savefig("Transaction_Sum_vs_Customer.png")
plt.show()


time.sleep(3)

conn.close()

print("This marks the end of Part 3 of the Capstone. \n")
print('x-----------------------------------------------------------------------------------------x \n')

url = 'https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json'
response = requests.get(url)
loan_data = response.json()
print(f"The status code for the above API response : {response.status_code}")
LOAN_DF = spark.read.json("raw.githubusercontent.com_platformps_LoanDataset_main_loan_data.json")

LOAN_DF.write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/classicmodels") \
  .option("dbtable", "creditcard_capstone.CDW_SAPP_loan_application") \
  .option("user", "root") \
  .option("password", "password") \
  .save()

print("This marks the end of Part 4 of the Capstone. \n")
print('x-----------------------------------------------------------------------------------------x \n')

conn = mydbconnection.connect(database='creditcard_capstone', user=credentials['user'], password=credentials['password'], port=3306)

self_employed_approved_query = f"""SELECT 
COUNT(CDW_SAPP_loan_application.Application_Status) AS `APPROVED SELF EMPLOYED` 
FROM CDW_SAPP_loan_application 
WHERE CDW_SAPP_loan_application.Application_Status = "Y" 
AND CDW_SAPP_loan_application.Self_Employed = "Yes";"""

self_employed_query = f"""SELECT 
COUNT(CDW_SAPP_loan_application.Self_Employed) AS `TOTAL SELF EMPLOYED` 
FROM CDW_SAPP_loan_application 
WHERE CDW_SAPP_loan_application.Self_Employed = "Yes";"""

SELF_EMPLOYED_APPROVED_DF = pd.read_sql(self_employed_approved_query,conn)
SELF_EMPLOYED_DF = pd.read_sql(self_employed_query,conn)

self_employed_approved = SELF_EMPLOYED_APPROVED_DF['APPROVED SELF EMPLOYED'][0]
self_employed_not_approved = SELF_EMPLOYED_DF['TOTAL SELF EMPLOYED'][0] - SELF_EMPLOYED_APPROVED_DF['APPROVED SELF EMPLOYED'][0]

plt.figure(figsize=(15, 15))
plt.pie([self_employed_approved, self_employed_not_approved], labels=["Approved","Not Approved"], autopct='%1.2f%%', startangle=180, colors=['teal','orange'])
plt.title("Percentage Self Employed Approved for Loan")
plt.legend(loc = 'upper right')
plt.savefig("Percentage_Self_Employed_Approved_for_Loan.png")
plt.show()

time.sleep(3)

rejected_married_male_query = f"""SELECT 
COUNT(CDW_SAPP_loan_application.Application_Status) AS `REJECTED MARRIED MALE` 
FROM CDW_SAPP_loan_application 
WHERE CDW_SAPP_loan_application.Application_Status = "N" 
AND CDW_SAPP_loan_application.Gender = "Male" AND CDW_SAPP_loan_application.Married = "Yes";"""

total_married_male_query = f"""SELECT 
COUNT(CDW_SAPP_loan_application.Gender) AS `TOTAL MARRIED MALE` 
FROM CDW_SAPP_loan_application 
WHERE CDW_SAPP_loan_application.Gender = "Male" AND CDW_SAPP_loan_application.Married = "Yes";"""

REJECTED_MARRIED_MALE_DF = pd.read_sql(rejected_married_male_query,conn)
TOTAL_MARRIED_MALE_DF = pd.read_sql(total_married_male_query,conn)

rejected_married_male = REJECTED_MARRIED_MALE_DF['REJECTED MARRIED MALE'][0]
approved_married_male = TOTAL_MARRIED_MALE_DF['TOTAL MARRIED MALE'][0] - REJECTED_MARRIED_MALE_DF['REJECTED MARRIED MALE'][0]

plt.figure(figsize=(15,15))
plt.pie([rejected_married_male, approved_married_male], labels=["Not Approved", "Approved"], autopct='%1.2f%%', startangle=180, colors=['teal','orange'])
plt.title("Percentage Married Male Approved for Loan")
plt.legend(loc = 'lower right')
plt.savefig("Percentage_Married_Male_Approved_For_Loan.png")
plt.show()


time.sleep(3)

month_number_to_name_dictionary = {'01': 'January','02': 'February','03': 'March','04': 'April','05': 'May','06': 'June','07': 'July','08': 'August','09': 'September','10': 'October','11': 'November','12': 'December'}

transaction_volume_query = f"""SELECT 
SUBSTRING(CDW_SAPP_CREDIT_CARD.TIMEID, 5, 2) AS MONTH, 
COUNT(CDW_SAPP_CREDIT_CARD.TRANSACTION_VALUE) AS `NUMBER OF TRANSACTIONS`, 
ROUND(SUM(CDW_SAPP_CREDIT_CARD.TRANSACTION_VALUE), 2) AS `TRANSACTION TOTAL` 
FROM CDW_SAPP_CREDIT_CARD 
GROUP BY SUBSTRING(CDW_SAPP_CREDIT_CARD.TIMEID, 5, 2) 
ORDER BY `TRANSACTION TOTAL` DESC LIMIT 3;"""

TRANSACTION_VOLUME_DF = pd.read_sql(transaction_volume_query,conn)

Y = TRANSACTION_VOLUME_DF['TRANSACTION TOTAL']
X = TRANSACTION_VOLUME_DF['MONTH'].map(month_number_to_name_dictionary)

plt.figure(figsize=(15, 15))
ax = plt.axes()
plt.bar(X,Y)

bars = plt.gca().patches
for bar in bars:
    if bar.get_height() == max(TRANSACTION_VOLUME_DF['TRANSACTION TOTAL']):
        bar.set_color('red')

for bar in bars:
    plt.annotate(f'${bar.get_height()}', xy=(bar.get_x() + bar.get_width()/2, bar.get_height()), xytext=(0, 4), textcoords="offset points", ha='center', va='center')

plt.xlabel("Month")
plt.ylabel("Transaction Volume")
plt.xticks()
plt.yticks([200000,200500,201000,201500,202000,202500,203000],['$200,000','$200,500','$201,000','$201,500','$202,000','$202,500','$203,000'])
plt.ylim(200000,203000)
plt.title("Max Transaction Volume vs Month")

ax.yaxis.grid(True, linestyle = '--')
ax.xaxis.grid(True, linestyle = '--')

plt.savefig("Max_Transaction_Volume_vs_Month.png")
plt.show()


time.sleep(3)

healtcare_query = f"""SELECT 
CAST(CDW_SAPP_BRANCH.BRANCH_CODE AS CHAR(3)) AS `BRANCH CODE`, 
ROUND(SUM(CDW_SAPP_CREDIT_CARD.TRANSACTION_VALUE),2) AS `TRANSACTION TOTAL`,
CDW_SAPP_BRANCH.BRANCH_CITY, 
CDW_SAPP_BRANCH.BRANCH_STATE,
CDW_SAPP_BRANCH.BRANCH_ZIP
FROM CDW_SAPP_CREDIT_CARD JOIN CDW_SAPP_BRANCH 
ON CDW_SAPP_CREDIT_CARD.BRANCH_CODE = CDW_SAPP_BRANCH.BRANCH_CODE 
WHERE CDW_SAPP_CREDIT_CARD.TRANSACTION_TYPE = "Healthcare" 
GROUP BY `BRANCH CODE`, CDW_SAPP_BRANCH.BRANCH_CITY,CDW_SAPP_BRANCH.BRANCH_STATE,CDW_SAPP_BRANCH.BRANCH_ZIP 
ORDER BY `TRANSACTION TOTAL` DESC LIMIT 10;"""

HEALTHCARE_DF = pd.read_sql(healtcare_query,conn)


X = HEALTHCARE_DF.apply(lambda row: f"{row['BRANCH CODE']}\n{row['BRANCH_CITY']}\n{row['BRANCH_STATE']}\n{row['BRANCH_ZIP']}", axis=1)
Y = HEALTHCARE_DF['TRANSACTION TOTAL']

plt.figure(figsize=(15, 15))
ax = plt.axes()
plt.bar(X,Y)

bars = plt.gca().patches
for bar in bars:
    if bar.get_height() == max(HEALTHCARE_DF['TRANSACTION TOTAL']):
        bar.set_color('red')

for bar in bars:
    plt.annotate(f'${bar.get_height()}', xy=(bar.get_x() + bar.get_width()/2, bar.get_height()), xytext=(0, 4), textcoords="offset points", ha='center', va='center')

plt.xlabel("Branch Code")
plt.ylabel("Healtcare Transaction Volume")
plt.yticks([3500,3600,3700,3750,3800,3900,4000,4050,4150,4200,4300,4400,4500],['$3,500','$3,600','$3,700','$3,750','$3,800','$3,900','$4,000','$4,050','$4,150','$4,200','$4,300','$4,400','$4,500'])
plt.ylim(3500,4500)
plt.title("Healthcare Transaction Volume vs Branch")

ax.yaxis.grid(True, linestyle = '--')
ax.xaxis.grid(True, linestyle = '--')

plt.savefig("Healthcare_Transaction_Volume_vs_Branch.png")
plt.show()


time.sleep(3)

conn.close()

print("This marks the end of the Capstone. \n")
print('x------------------------------------------------------------------------------------------------x\n')