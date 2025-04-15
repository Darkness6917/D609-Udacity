# STEDI Human Balance Analytics

##  Project Purpose
The **STEDI Human Balance Analytics** project demonstrates a complete ETL (Extract, Transform, Load) pipeline using **Amazon Web Services (AWS)**. The goal is to process human balance-related sensor data and customer records to prepare a **clean, trusted, and curated dataset** that can be used for machine learning applications.
This project simulates a real-world scenario where raw JSON files are uploaded to Amazon S3, transformed using AWS Glue Studio, and analyzed using Amazon Athena. The curated datasets can be used by data scientists to build predictive models that analyze human stability and fall risk.

## Datasets Used
Three key datasets in JSON format:
1. **Customer**
   - Fields: email, firstName, lastName, phone, serialNumber, shareWithPublicAsOfDate, shareWithResearchAsOfDate, etc.
   - Purpose: Includes user profile info and consent dates
2. **Accelerometer**
   - Fields: timestamp, user (email), x, y, z
   - Purpose: Sensor readings related to body movement
3. **Step Trainer**
   - Fields: sensorReadingTime, serialNumber, distanceFromObject
   - Purpose: Measures the distance from a detected object at a point in time




## AWS Architecture Overview
         Raw JSON (S3 Landing Zone) >  AWS Glue (Transform raw > trusted > curated) > Trusted Customer / Accelerometer Data > Curated  Data (Joins for ML) > Athena SQL (Validation & Querying)

Step 1: Upload JSON Files to S3
Create an S3 bucket or use an existing one
Create the following folders:
customer/landing/
accelerometer/landing/
step_trainer/landing/
Upload the corresponding JSON files into each folder

Step 2: Create Glue Tables from S3 JSON
Go to AWS Glue → Tables → Add Table → "From S3"
Create external tables for:
customer_landing
accelerometer_landing
step_trainer_landing

	Step 3: Run Glue Jobs (Landing → Trusted)
customer_landing_to_trusted.py
Filters out rows where shareWithResearchAsOfDate IS NULL
Output: customer/trusted/
accelerometer_landing_to_trusted.py
Joins landing data with customer_trusted on email
Drops unmatched users (inner join)
Output: accelerometer/trusted/
step_trainer_trusted.py
Moves step trainer data to trusted
Output: step_trainer/trusted/

	Step 4: Run Glue Jobs (Trusted → Curated)
customer_trusted_to_curated.py
Join customer_trusted and accelerometer_trusted on email
Keep only customer columns
Output: customer/curated/
machine_learning_curated.py
Join step_trainer_trusted with accelerometer_trusted on timestamp fields
Output: machine_learning/curated/

Step 5: Validate with Athena Queries
Run the following in Athena and confirm row counts:
SELECT COUNT(*) FROM customer_landing;        -- Expected: 956
SELECT COUNT(*) FROM accelerometer_landing;   -- Expected: 81273
SELECT COUNT(*) FROM step_trainer_landing;    -- Expected: 28680
SELECT COUNT(*) FROM customer_trusted;        -- Expected: 482
SELECT COUNT(*) FROM accelerometer_trusted;   -- Expected: 32025
SELECT COUNT(*) FROM step_trainer_trusted;    -- Expected: 14460
SELECT COUNT(*) FROM customer_curated;        -- Expected: 482
SELECT COUNT(*) FROM machine_learning_curated;-- Expected: 43681
