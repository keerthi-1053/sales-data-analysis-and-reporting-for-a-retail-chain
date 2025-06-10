

-- ### Phase 1: Data Collection and Database Setup

-- 1. **Creating the Database and Tables**:  
--   You start by creating the database and tables. The table structures are defined with relevant columns for transactions (`customer_id`, `trans_date`, `tran_amount`) and responses (`customer_id`, `response`).

  drop table Retail_Data_Response
   CREATE DATABASE Retail_db;
   USE Retail_db;
   
   CREATE TABLE Retail_Data_Transactions (
       customer_id VARCHAR(255),
       trans_date VARCHAR(255),
       tran_amount INT
   );
   
   CREATE TABLE Retail_Data_Response (
       customer_id VARCHAR(255),
       response INT
   );
   ```

  -- Show all data
select * from Retail_Data_Response;
select * from Retail_Data_Transactions;

-- Phase 2 & 3: Data Cleaning, Preparation, and Analysis

-- 2. **Indexing for Optimization**:  
   we’ve created an index on `customer_id` to speed up queries involving this column, which is crucial for large datasets.

   ```sql
 CREATE INDEX idx_customer_id ON Retail_Data_Transactions(customer_id);
 SET STATISTICS PROFILE ON;

SELECT * FROM Retail_Data_Transactions WHERE Customer_id = 'CS5295';

SET STATISTICS PROFILE OFF;

   ```

3. **Handling Missing Values**:  
   Setting missing transaction amounts to `0` is a good step for cleaning. However, it might be better to investigate why there are missing values and determine if imputing with `0` is appropriate.

   ```sql
   UPDATE Retail_Data_Transactions
   SET tran_amount = 0
   WHERE tran_amount IS NULL;
   ```

4. **Outlier Removal Using Percentiles**:  
   Using the interquartile range (IQR) method to remove outliers ensures more robust analysis.

   ```sql
   WITH Quartiles AS (
       SELECT 
           PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY tran_amount) OVER () AS Q1, 
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY tran_amount) OVER () AS Q3
       FROM Retail_Data_Transactions
   )
   DELETE t
   FROM Retail_Data_Transactions t
   CROSS APPLY (
       SELECT Q3 + 1.5 * (Q3 - Q1) AS Threshold
       FROM Quartiles
   ) q
   WHERE t.tran_amount > q.Threshold;
   ```

5. **Aggregations and Filtering**:  
   You’ve done various analyses like total sales, average transaction amount, transactions per customer, and revenue by customer.

   ```sql
   SELECT customer_id, SUM(tran_amount) AS total_sales
   FROM Retail_Data_Transactions
   GROUP BY customer_id;
   ```

6. **Customer Segmentation**:  
   Segmenting customers based on spending is a key part of your analysis.

   ```sql
   SELECT customer_id, SUM(tran_amount) AS total_spent
   FROM Retail_Data_Transactions
   GROUP BY customer_id
   HAVING SUM(tran_amount) >= 2000
   ORDER BY total_spent DESC;
   ```

7. **Advanced Analysis: RFM (Recency, Frequency, Monetary)**

   RFM analysis is highly effective for customer segmentation and targeting.

   ```sql
   SELECT customer_id,
          DATEDIFF(DAY, MAX(trans_date), CAST(GETDATE() AS DATE)) AS recency,
          COUNT(*) AS frequency,
          SUM(tran_amount) AS monetary
   FROM Retail_Data_Transactions
   GROUP BY customer_id;
   ```

   You then combine these metrics to segment customers into categories like "Best Customer," "Loyal Customer," etc.

   ```sql
WITH CustomerMetrics AS (
    SELECT 
        customer_id,
        DATEDIFF(DAY, MAX(trans_date), CAST(GETDATE() AS DATE)) AS recency,
        COUNT(*) AS frequency,
        SUM(tran_amount) AS monetary
    FROM Retail_Data_Transactions
    GROUP BY customer_id
)
SELECT 
    MIN(recency) AS min_recency,
    MAX(recency) AS max_recency,
    MIN(frequency) AS min_frequency,
    MAX(frequency) AS max_frequency,
    MIN(monetary) AS min_monetary,
    MAX(monetary) AS max_monetary
FROM CustomerMetrics;


----------------------------------------------
SELECT customer_id,
       CASE
           WHEN recency <= 3500 AND frequency >= 20 AND monetary >= 2000 THEN 'Best Customer'
           WHEN recency <= 3800 AND frequency >= 10 AND monetary >= 1000 THEN 'Loyal Customer'
           WHEN recency <= 4200 AND frequency >= 5 AND monetary >= 500 THEN 'Potential Customer'
           ELSE 'At-Risk Customer'
       END AS segment
FROM (
    SELECT customer_id,
           DATEDIFF(DAY, MAX(trans_date), CAST(GETDATE() AS DATE)) AS recency,
           COUNT(*) AS frequency,
           SUM(tran_amount) AS monetary
    FROM Retail_Data_Transactions
    GROUP BY customer_id
) AS RFM
ORDER BY segment;


ALTER TABLE Retail_Data_Transactions
DROP COLUMN segment;

ALTER TABLE Retail_Data_Transactions
ADD segment VARCHAR(50);

--update 
WITH RFM AS (
    SELECT customer_id,
           DATEDIFF(DAY, MAX(trans_date), GETDATE()) AS recency,
           COUNT(*) AS frequency,
           SUM(tran_amount) AS monetary
    FROM Retail_Data_Transactions
    GROUP BY customer_id
)
UPDATE Retail_Data_Transactions
SET segment = CASE
                  WHEN RFM.recency <= 3500 AND RFM.frequency >= 20 AND RFM.monetary >= 2000 THEN 'Best Customer'
                  WHEN RFM.recency <= 3800 AND RFM.frequency >= 10 AND RFM.monetary >= 1000 THEN 'Loyal Customer'
                  WHEN RFM.recency <= 4200 AND RFM.frequency >= 5 AND RFM.monetary >= 500 THEN 'Potential Customer'
                  ELSE 'At-Risk Customer'
              END
FROM Retail_Data_Transactions AS T
JOIN RFM ON T.customer_id = RFM.customer_id;

   ```

### Additional Considerations and Next Steps

1. **CLV (Customer Lifetime Value) Calculation**:  
   You can enhance your analysis by predicting the lifetime value of customers. This metric helps prioritize customer retention strategies.

   ```sql
   SELECT customer_id,
          AVG(tran_amount) * COUNT(*) * AVG(DATEDIFF(DAY, trans_date, GETDATE())) AS clv
   FROM Retail_Data_Transactions
   GROUP BY customer_id;
   ```

2. **Revenue Growth Analysis**:  
   Tracking revenue growth across different periods provides insights into customer behavior trends.

3. **Day, Month, Year Extraction and Date Analysis**:  
   Adding columns for day, month, and year will improve the flexibility of your time-based analysis.

   ```sql
   ALTER TABLE Retail_Data_Transactions
ADD trans_day AS DAY(trans_date);

   ```

4. **Segmentation Based on Transaction Status**:  
   Classifying transactions as high, medium, or low helps in understanding the distribution of customer spending.

   ```sql
   SELECT customer_id, tran_amount,
          CASE
              WHEN tran_amount >= 80 THEN 'High Transaction'
              WHEN tran_amount >= 40 AND tran_amount < 80 THEN 'Medium Transaction'
              ELSE 'Low Transaction'
          END AS 'Transaction Status'
   FROM Retail_Data_Transactions;
   ```

This script covers a comprehensive data pipeline, from collection to advanced analysis, providing valuable insights into customer behavior and spending patterns.


```

### 3. **Total Sales for the Project Period**

```sql
SELECT SUM(tran_amount) AS total_sales
FROM Retail_Data_Transactions;
```

### 4. **Top 5 Customers by Sales Performance**

```sql
SELECT customer_id, SUM(tran_amount) AS total_sales
FROM Retail_Data_Transactions
GROUP BY customer_id
ORDER BY total_sales DESC
LIMIT 5;
```

### 5. **Highest Sales Value Recorded in 2013**

```sql
SELECT MAX(tran_amount) AS highest_sales_2013
FROM Retail_Data_Transactions
WHERE YEAR(trans_date) = 2013;
```

### 6. **Lowest Sales in 2015**

```sql
SELECT MIN(tran_amount) AS lowest_sales_2015
FROM Retail_Data_Transactions
WHERE YEAR(trans_date) = 2015;
```

### 7. **Customer Recency Analysis (Highest and Lowest)**

```sql
-- For highest recency (most recent purchase)
SELECT customer_id, MAX(trans_date) AS last_purchase_date
FROM Retail_Data_Transactions
GROUP BY customer_id
ORDER BY last_purchase_date DESC
LIMIT 1;

-- For lowest recency (oldest purchase)
SELECT customer_id, MIN(trans_date) AS first_purchase_date
FROM Retail_Data_Transactions
GROUP BY customer_id
ORDER BY first_purchase_date ASC
LIMIT 1;
```

### 8. **Segment Analysis**

If segment information is stored, you would join the tables and calculate the sales proportion by segment:

SELECT name
FROM sys.columns
WHERE object_id = OBJECT_ID('Retail_Data_Transactions');
--================================================================================================================================================================================================
============================================================================================================================================================
===========================================================================================================================================
SELECT name
FROM sys.columns
WHERE object_id = OBJECT_ID('Retail_Data_Response');

```sql
-- Assuming there is a segment field in your dataset
SELECT segment, SUM(tran_amount) AS segment_sales,
    (SUM(tran_amount) / (SELECT SUM(tran_amount) FROM Retail_Data_Transactions)) * 100 AS sales_percentage
FROM Retail_Data_Transactions
GROUP BY segment
ORDER BY segment_sales DESC;
```

### 9. **Top Customer Responses**

```sql
SELECT customer_id, COUNT(response) AS response_count
FROM Retail_Data_Response
GROUP BY customer_id
ORDER BY response_count DESC
LIMIT 2;
```

### 10. **Monetary Distribution by Frequency**

To calculate frequency and monetary values:

```sql
WITH FrequencyTable AS (
    SELECT customer_id, COUNT(*) AS frequency, SUM(tran_amount) AS monetary
    FROM Retail_Data_Transactions
    GROUP BY customer_id
)
SELECT frequency, SUM(monetary) AS total_monetary_value
FROM FrequencyTable
GROUP BY frequency
ORDER BY total_monetary_value DESC;
```


=========================================================================================
Certainly! Here’s a deeper dive into advanced analysis techniques that can be applied using SQL and Python for your project:

### Advanced Analysis Techniques

1. **RFM (Recency, Frequency, Monetary) Analysis**:
   RFM analysis helps in segmenting customers based on how recently they purchased (Recency), how often they purchase (Frequency), and how much they spend (Monetary). This is crucial for customer segmentation and targeting marketing efforts.

   - **Recency**: Calculate the number of days since the last purchase.

     ```sql
     SELECT customer_id, 
       DATEDIFF(DAY, MAX(trans_date), GETDATE()) AS recency
FROM Retail_Data_Transactions
GROUP BY customer_id;

--Add the recency Column to the Table
-- First, add the recency column to the Retail_Data_Transactions table:

ALTER TABLE Retail_Data_Transactions
ADD recency INT;

ALTER TABLE Retail_Data_Transactions
ADD frequency INT,
    monetary DECIMAL(18, 2);

--	Update frequency, monetary 
WITH RFM AS (
    SELECT customer_id,
           DATEDIFF(DAY, MAX(trans_date), GETDATE()) AS recency,
           COUNT(*) AS frequency,
           SUM(tran_amount) AS monetary
    FROM Retail_Data_Transactions
    GROUP BY customer_id
)
UPDATE Retail_Data_Transactions
SET recency = RFM.recency,
    frequency = RFM.frequency,
    monetary = RFM.monetary
FROM Retail_Data_Transactions AS T
JOIN RFM ON T.customer_id = RFM.customer_id;


     ```

--	 Update the recency Column
---Now, update the recency column with the calculated values:



WITH RFM AS (
    SELECT customer_id, 
           DATEDIFF(DAY, MAX(trans_date), GETDATE()) AS recency
    FROM Retail_Data_Transactions
    GROUP BY customer_id
)
UPDATE Retail_Data_Transactions
SET recency = RFM.recency
FROM Retail_Data_Transactions AS T
JOIN RFM ON T.customer_id = RFM.customer_id;

   - **Frequency**: Count the total number of transactions per customer.

     ```sql
     SELECT customer_id, COUNT(*) AS frequency
     FROM Retail_Data_Transactions
     GROUP BY customer_id;
     ```

   - **Monetary**: Calculate the total spending of each customer.

     ```sql
     SELECT customer_id, SUM(tran_amount) AS monetary
     FROM Retail_Data_Transactions
     GROUP BY customer_id;
     ```

   - **Combining RFM Values**:

     ```sql
     SELECT customer_id, 
       DATEDIFF(DAY, MAX(trans_date), GETDATE()) AS recency,
       COUNT(*) AS frequency,
       SUM(tran_amount) AS monetary
FROM Retail_Data_Transactions
GROUP BY customer_id;

     ```

--   - **RFM Segmentation**: You can further segment customers by assigning scores (e.g., 1-5) to each metric and categorize them into different segments like “High Value”, “Churn Risk”, etc.

--2. **Customer Lifetime Value (CLV) Calculation**:
--   CLV predicts the total revenue a business can expect from a customer over the duration of the relationship. 

--   - Basic CLV Calculation:

    
     SELECT customer_id, 
       AVG(tran_amount) * COUNT(*) * AVG(DATEDIFF(DAY, trans_date, GETDATE())) AS clv
FROM Retail_Data_Transactions
GROUP BY customer_id;

  

--   - You can refine CLV models by incorporating factors like customer retention rate and discount rate.

--3. **Churn Prediction Analysis**:
--   Churn analysis identifies customers who are likely to stop purchasing. Using SQL and later applying machine learning models can help identify at-risk customers.

  -- - Identify Inactive Customers:

    
SELECT customer_id
FROM Retail_Data_Transactions
GROUP BY customer_id
HAVING DATEDIFF(DAY, MAX(trans_date), GETDATE()) > 180;

     ```



--4. **Cohort Analysis with Retention Rate**:
 --  Cohort analysis groups customers based on their first purchase date and tracks their behavior over time (e.g., retention).

 --  - First Purchase Cohort:

     
    WITH First_Purchase AS (
    SELECT customer_id, MIN(trans_date) AS first_purchase
    FROM Retail_Data_Transactions
    GROUP BY customer_id
)
SELECT YEAR(first_purchase) AS cohort_year,
       MONTH(first_purchase) AS cohort_month,
       COUNT(customer_id) AS cohort_size
FROM First_Purchase
GROUP BY YEAR(first_purchase), MONTH(first_purchase);


 --  - Retention Rate Calculation:

   WITH Cohort AS (
    SELECT customer_id,
           MIN(trans_date) AS first_purchase
    FROM Retail_Data_Transactions
    GROUP BY customer_id
),
Monthly_Purchases AS (
    SELECT customer_id,
           YEAR(trans_date) * 100 + MONTH(trans_date) AS purchase_month
    FROM Retail_Data_Transactions
)
SELECT YEAR(first_purchase) AS cohort_year,
       MONTH(first_purchase) AS cohort_month,
       COUNT(DISTINCT Cohort.customer_id) AS initial_customers,
       SUM(CASE WHEN Monthly_Purchases.purchase_month = (YEAR(first_purchase) * 100 + MONTH(first_purchase) + 1) THEN 1 ELSE 0 END) AS retained_customers
FROM Cohort
JOIN Monthly_Purchases ON Cohort.customer_id = Monthly_Purchases.customer_id
GROUP BY YEAR(first_purchase), MONTH(first_purchase);

     ```

  

--5. **Time Series Forecasting**:
--   Predict future sales based on historical data using techniques like ARIMA, Exponential Smoothing, or Prophet.

 --  - **Decomposing Time Series**:

     
     SELECT YEAR(trans_date) AS year,
       MONTH(trans_date) AS month,
       SUM(tran_amount) AS monthly_sales
FROM Retail_Data_Transactions
GROUP BY YEAR(trans_date), MONTH(trans_date)
ORDER BY YEAR(trans_date), MONTH(trans_date);

     ```

--   - **Moving Averages and Exponential Smoothing**: Implement advanced forecasting techniques using Python libraries (e.g., `statsmodels`).

---6. **Customer Segmentation Using Clustering**:
--   Apply clustering techniques (like K-Means, DBSCAN) to segment customers based on RFM or other relevant metrics.

 --  - Use SQL to extract the relevant features:

     
SELECT customer_id, 
       DATEDIFF(DAY, MAX(trans_date), GETDATE()) AS recency,
       COUNT(*) AS frequency,
       SUM(tran_amount) AS monetary
FROM Retail_Data_Transactions
GROUP BY customer_id;

  

  -----------------------------------------------------------------------------------------------------------------------------
--Year and Month:
SELECT 
  customer_id,
  trans_date,
  YEAR(trans_date) AS year,
  MONTH(trans_date) AS month
FROM Retail_Data_Transactions;



- **Day of Week**:
  ```sql
  SELECT 
    customer_id,
    trans_date,
    FORMAT(trans_date, 'dddd') AS day_of_week
FROM Retail_Data_Transactions;

  ```

- **Transaction Count**:
  ```sql
  SELECT 
    customer_id,
    COUNT(*) AS transaction_count
  FROM Retail_Data_Transactions
  GROUP BY customer_id;
  ```

- **Average Transaction Amount**:
  ```sql
  SELECT 
    customer_id,
    AVG(tran_amount) AS average_transaction_amount
  FROM Retail_Data_Transactions
  GROUP BY customer_id;
  ```
---Add New Columns

ALTER TABLE Retail_Data_Transactions
ADD new_year_column INT,
    month INT,
    day_of_week VARCHAR(20),
    transaction_count INT,
    average_transaction_amount DECIMAL(18, 2);



ALTER TABLE Retail_Data_Transactions
ADD recency_bucket VARCHAR(50);

	--update 
	-- Update Year and Month
UPDATE Retail_Data_Transactions
SET year = YEAR(trans_date),
    month = MONTH(trans_date);

-- Update Day of Week
UPDATE Retail_Data_Transactions
SET day_of_week = FORMAT(trans_date, 'dddd');

-- Update Transaction Count
WITH TransactionCount AS (
    SELECT customer_id,
           COUNT(*) AS transaction_count
    FROM Retail_Data_Transactions
    GROUP BY customer_id
)
UPDATE Retail_Data_Transactions
SET transaction_count = TC.transaction_count
FROM Retail_Data_Transactions AS T
JOIN TransactionCount AS TC ON T.customer_id = TC.customer_id;

-- Update Average Transaction Amount
WITH AverageTransaction AS (
    SELECT customer_id,
           AVG(tran_amount) AS average_transaction_amount
    FROM Retail_Data_Transactions
    GROUP BY customer_id
)
UPDATE Retail_Data_Transactions
SET average_transaction_amount = AT.average_transaction_amount
FROM Retail_Data_Transactions AS T
JOIN AverageTransaction AS AT ON T.customer_id = AT.customer_id;


SELECT 
    customer_id,
    CASE 
        WHEN DATEDIFF(DAY, trans_date, GETDATE()) <= 30 THEN 'Last 30 Days'
        WHEN DATEDIFF(DAY, trans_date, GETDATE()) <= 60 THEN 'Last 60 Days'
        WHEN DATEDIFF(DAY, trans_date, GETDATE()) <= 90 THEN 'Last 90 Days'
        ELSE 'More than 90 Days'
    END AS recency_bucket
FROM Retail_Data_Transactions;



----Frequency Bucket
---Categorize customers based on the number of transactions or how often they make purchases.



SELECT 
    customer_id,
    CASE 
        WHEN transaction_count >= 10 THEN 'High Frequency'
        WHEN transaction_count >= 5 THEN 'Medium Frequency'
        ELSE 'Low Frequency'
    END AS frequency_bucket
FROM (
    SELECT 
        customer_id,
        COUNT(*) AS transaction_count
    FROM Retail_Data_Transactions
    GROUP BY customer_id
) AS transaction_summary;


---Monetary Bucket
---Categorize customers based on their total spending.


SELECT 
    customer_id,
    CASE 
        WHEN total_spend >= 1000 THEN 'High Value'
        WHEN total_spend >= 500 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS monetary_bucket
FROM (
    SELECT 
        customer_id,
        SUM(tran_amount) AS total_spend
    FROM Retail_Data_Transactions
    GROUP BY customer_id
) AS spend_summary;


--4. Customer Lifetime Value (CLV)
---Estimate CLV as the total amount spent by the customer over their lifetime.

SELECT 
    customer_id,
    SUM(tran_amount) AS CLV
FROM Retail_Data_Transactions
GROUP BY customer_id;


--5. Churn Probability
--Estimate churn probability based on recency and frequency. This is more complex and often requires statistical or machine learning models. However, you can create a basic heuristic using recency and frequency buckets.
WITH customer_summary AS (
    SELECT 
        customer_id,
        CASE 
            WHEN DATEDIFF(DAY, MAX(trans_date), GETDATE()) <= 30 THEN 'Last 30 Days'
            WHEN DATEDIFF(DAY, MAX(trans_date), GETDATE()) <= 60 THEN 'Last 60 Days'
            WHEN DATEDIFF(DAY, MAX(trans_date), GETDATE()) <= 90 THEN 'Last 90 Days'
            ELSE 'More than 90 Days'
        END AS recency_bucket,
        CASE 
            WHEN COUNT(*) >= 20 THEN 'High Frequency'
            WHEN COUNT(*) >= 10 THEN 'Medium Frequency'
            ELSE 'Low Frequency'
        END AS frequency_bucket
    FROM Retail_Data_Transactions
    GROUP BY customer_id
)
SELECT 
    customer_id,
    CASE 
        WHEN recency_bucket = 'Last 30 Days' AND frequency_bucket = 'High Frequency' THEN 'Low Churn Probability'
        WHEN recency_bucket IN ('Last 30 Days', 'Last 60 Days') AND frequency_bucket = 'Medium Frequency' THEN 'Medium Churn Probability'
        ELSE 'High Churn Probability'
    END AS churn_probability
FROM customer_summary;

--6. Engagement Score
--Combine recency, frequency, and monetary value to create an engagement score.


WITH customer_summary AS (
    SELECT 
        customer_id,
        CASE 
            WHEN DATEDIFF(DAY, MAX(trans_date), GETDATE()) <= 30 THEN 'Last 30 Days'
            WHEN DATEDIFF(DAY, MAX(trans_date), GETDATE()) <= 60 THEN 'Last 60 Days'
            WHEN DATEDIFF(DAY, MAX(trans_date), GETDATE()) <= 90 THEN 'Last 90 Days'
            ELSE 'More than 90 Days'
        END AS recency_bucket,
        CASE 
            WHEN COUNT(*) >= 20 THEN 'High Frequency'
            WHEN COUNT(*) >= 10 THEN 'Medium Frequency'
            ELSE 'Low Frequency'
        END AS frequency_bucket,
        CASE 
            WHEN SUM(tran_amount) >= 2000 THEN 'High Value'
            WHEN SUM(tran_amount) >= 1000 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS monetary_bucket
    FROM Retail_Data_Transactions
    GROUP BY customer_id
)
SELECT 
    customer_id,
    CASE 
        WHEN recency_bucket = 'Last 30 Days' AND frequency_bucket = 'High Frequency' AND monetary_bucket = 'High Value' THEN 10
        WHEN recency_bucket IN ('Last 30 Days', 'Last 60 Days') AND frequency_bucket IN ('Medium Frequency', 'High Frequency') AND monetary_bucket IN ('Medium Value', 'High Value') THEN 7
        ELSE 4
    END AS engagement_score
FROM customer_summary;


--Adding column to table

ALTER TABLE Retail_Data_Transactions
ADD frequency_bucket VARCHAR(50),
    monetary_bucket VARCHAR(50),
    CLV DECIMAL(18, 2),
    churn_probability VARCHAR(50),
    engagement_score INT;


	---updating column
	WITH transaction_summary AS (
    SELECT 
        customer_id,
        COUNT(*) AS transaction_count,
        SUM(tran_amount) AS total_spend
    FROM Retail_Data_Transactions
    GROUP BY customer_id
)
UPDATE Retail_Data_Transactions
SET frequency_bucket = CASE 
                          WHEN transaction_summary.transaction_count >= 10 THEN 'High Frequency'
                          WHEN transaction_summary.transaction_count >= 5 THEN 'Medium Frequency'
                          ELSE 'Low Frequency'
                       END,
    monetary_bucket = CASE 
                          WHEN transaction_summary.total_spend >= 1000 THEN 'High Value'
                          WHEN transaction_summary.total_spend >= 500 THEN 'Medium Value'
                          ELSE 'Low Value'
                      END
FROM Retail_Data_Transactions AS T
JOIN transaction_summary ON T.customer_id = transaction_summary.customer_id;


---update CLV
WITH clv_summary AS (
    SELECT 
        customer_id,
        SUM(tran_amount) AS CLV
    FROM Retail_Data_Transactions
    GROUP BY customer_id
)
UPDATE Retail_Data_Transactions
SET CLV = clv_summary.CLV
FROM Retail_Data_Transactions AS T
JOIN clv_summary ON T.customer_id = clv_summary.customer_id;



---Update churn_probability


WITH customer_summary AS (
    SELECT 
        customer_id,
        CASE 
            WHEN DATEDIFF(DAY, MAX(trans_date), GETDATE()) <= 30 THEN 'Last 30 Days'
            WHEN DATEDIFF(DAY, MAX(trans_date), GETDATE()) <= 60 THEN 'Last 60 Days'
            WHEN DATEDIFF(DAY, MAX(trans_date), GETDATE()) <= 90 THEN 'Last 90 Days'
            ELSE 'More than 90 Days'
        END AS recency_bucket,
        CASE 
            WHEN COUNT(*) >= 10 THEN 'High Frequency'
            WHEN COUNT(*) >= 5 THEN 'Medium Frequency'
            ELSE 'Low Frequency'
        END AS frequency_bucket
    FROM Retail_Data_Transactions
    GROUP BY customer_id
)
UPDATE Retail_Data_Transactions
SET churn_probability = CASE 
                            WHEN customer_summary.recency_bucket = 'Last 30 Days' AND customer_summary.frequency_bucket = 'High Frequency' THEN 'Low Churn Probability'
                            WHEN customer_summary.recency_bucket IN ('Last 30 Days', 'Last 60 Days') AND customer_summary.frequency_bucket = 'Medium Frequency' THEN 'Medium Churn Probability'
                            ELSE 'High Churn Probability'
                        END
FROM Retail_Data_Transactions AS T
JOIN customer_summary ON T.customer_id = customer_summary.customer_id;



-----------Update engagement_score


WITH customer_summary AS (
    SELECT 
        customer_id,
        CASE 
            WHEN DATEDIFF(DAY, MAX(trans_date), GETDATE()) <= 30 THEN 'Last 30 Days'
            WHEN DATEDIFF(DAY, MAX(trans_date), GETDATE()) <= 60 THEN 'Last 60 Days'
            WHEN DATEDIFF(DAY, MAX(trans_date), GETDATE()) <= 90 THEN 'Last 90 Days'
            ELSE 'More than 90 Days'
        END AS recency_bucket,
        CASE 
            WHEN COUNT(*) >= 10 THEN 'High Frequency'
            WHEN COUNT(*) >= 5 THEN 'Medium Frequency'
            ELSE 'Low Frequency'
        END AS frequency_bucket,
        CASE 
            WHEN SUM(tran_amount) >= 1000 THEN 'High Value'
            WHEN SUM(tran_amount) >= 500 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS monetary_bucket
    FROM Retail_Data_Transactions
    GROUP BY customer_id
)
UPDATE Retail_Data_Transactions
SET engagement_score = CASE 
                           WHEN customer_summary.recency_bucket = 'Last 30 Days' AND customer_summary.frequency_bucket = 'High Frequency' AND customer_summary.monetary_bucket = 'High Value' THEN 10
                           WHEN customer_summary.recency_bucket IN ('Last 30 Days', 'Last 60 Days') AND customer_summary.frequency_bucket IN ('Medium Frequency', 'High Frequency') AND customer_summary.monetary_bucket IN ('Medium Value', 'High Value') THEN 7
                           ELSE 4
                       END
FROM Retail_Data_Transactions AS T
JOIN customer_summary ON T.customer_id = customer_summary.customer_id;


select * from Retail_Data_Response

SELECT SUM(CAST(response AS int)) AS total_response
FROM Retail_Data_Response;

