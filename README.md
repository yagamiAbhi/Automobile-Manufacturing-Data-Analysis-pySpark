# Automobile-Manufacturing-Data-Analysis-pySpark
Automobile Manufacturing Company Data Analysis Using PySpark

**Automobile manufacturing scenario**

Consider a leading two-wheeler manufacturing unit that produces two-wheeler components for the designs provided by various automobile companies.

HDFS, Spark Core, and Spark SQL components are used to store and process the order details.

Below are the datasets used for data analytics.

**OrderBook.csv:**

Contains the orders data given by various companies to manufacturer.

**Schema:**

OrderID:STRING,CompanyID:STRING,partsRequired:STRING,OrderQuantity:LONG,CostOfPart:DOUBLE,OrderDate:STRING,ExpectedDeliveryDate:STRING,DeliveryStatus:STRING


**CompanyDetails.csv:**

Contains information about various customer companies.

**Schema:**

CompanyID: STRING, CompanyName: STRING, CompanyLocation: STRING, CompanyContact: LONG, EstablishedYear: INT



Below are the data analysis requirements to be implemented:

* Create Namedtuple for the schema provided for both the datasets.
* Load both the csv files into RDDs CompanyDetailsRDD and OrderBookRDD accordingly.
* Convert both the datasets to DataFrames.
* Fetch the details of all the companies that are at least 15 years old.
* Fetch the name of the company who have ordered “Brakes”.
* Find the total number of “Exhaust Pipes” that have been ordered from “New York” location.
* Find the total count of all the orders that has been “Delivered”.
* Find the total count of all the orders that has been “Pending”.
* Find the total quantity and cost of all the parts ordered from “North Carolina” location.
* Display the start date, end date and company name who have ordered for “Exhaust Pipes”.
