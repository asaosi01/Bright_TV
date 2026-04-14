-- Databricks notebook source
-- View user_profiles_dataset data
SELECT * 
FROM workspace.default.user_profiles
LIMIT 10;

-- How many NULL value does Gender have
SELECT Gender, COUNT(*) AS NOofGender
FROM workspace.default.user_profiles
GROUP BY Gender;

-- How many NULL value does Race have
SELECT Race, COUNT(*) AS NOofRace
FROM workspace.default.user_profiles
GROUP BY Race;

-- Check for province we have 
SELECT 
      DISTINCT Province
FROM workspace.default.user_profiles
WHERE Province = ' ';

-- Check for unique Race we have 
SELECT DISTINCT Race
FROM workspace.default.user_profiles;

-- Check for unique gender we have 
SELECT DISTINCT Gender
FROM workspace.default.user_profiles;

-- Check for unique age we have 
SELECT DISTINCT age
FROM workspace.default.user_profiles;

-- View viewership_dataset data
SELECT * 
FROM workspace.default.viewership
LIMIT 20;

-- Check for unique Channel we have 
SELECT DISTINCT Channel
FROM workspace.default.viewership;

-- Convert RecordDate to date and  Duration to time
SELECT 
      RecordDate,
      to_date(to_timestamp(RecordDate)) AS The_Date,
      Duration,
      date_format(to_timestamp(Duration), 'HH:mm:ss') AS The_time,
      date_format(from_utc_timestamp(RecordDate, 'Africa/Johannesburg'), 'HH:mm:ss') AS SA_Time
FROM workspace.default.viewership;

-- Join the two tables 
SELECT u.*,
      v.Channel,
      v.RecordDate, 
      v.Duration
FROM user_profiles u 
LEFT JOIN viewership v ON u.userid = v.UserID0;

-- Apply all above codes
-- Duration is stored as 'HH:mm:ss' text in v.Duration
-- Step 1: convert to total minutes
WITH joined AS (
  SELECT
    u.UserID,
    CASE
      WHEN u.Province = ' ' THEN 'Coming_Soon'
      WHEN u.Province = 'None' THEN 'Not Provided'
      ELSE u.Province
    END AS Province,

    CASE
      WHEN u.Gender = ' ' THEN 'non-binary'
      WHEN u.Gender = 'None' THEN 'LGBT+'
      ELSE u.Gender
    END AS Gender,

    CASE
      WHEN u.Race = ' ' OR u.Race = 'None' THEN 'Mixed'
      ELSE u.Race
    END AS Race,

    u.Age,
    CASE
      WHEN u.Age BETWEEN 0 AND 12 THEN 'Childhood'
      WHEN u.Age BETWEEN 13 AND 18 THEN 'Teenager'
      WHEN u.Age BETWEEN 19 AND 35 THEN 'Youth'
      WHEN u.Age BETWEEN 36 AND 60 THEN 'Adult'
      ELSE 'Senior_citizen'
    END AS Age_Group,

    IFNULL(v.Channel, 'Not_Viewing') AS Channel,

    CASE
      WHEN v.RecordDate IS NULL THEN '2016-03-31'
      ELSE TO_DATE(TO_TIMESTAMP(v.RecordDate))
    END AS The_Date,

    CASE
      WHEN v.RecordDate IS NULL THEN DATE_FORMAT(CURRENT_TIMESTAMP() + INTERVAL '2' HOUR, 'HH:mm:ss')
      ELSE DATE_FORMAT(v.RecordDate + INTERVAL '2' HOUR, 'HH:mm:ss')
    END AS SA_Time,

    COALESCE(date_format(to_timestamp(v.Duration, 'HH:mm:ss'), 'HH:mm:ss'), '00:00:00') AS Duration,

    -- compute duration in minutes once
    hour(to_timestamp(v.Duration, 'HH:mm:ss')) * 60
      + minute(to_timestamp(v.Duration, 'HH:mm:ss'))
      + second(to_timestamp(v.Duration, 'HH:mm:ss')) / 60.0 AS duration_minutes,

    -- your Time_Bucket logic (kept as-is, but placed inside the SELECT)
    CASE
      WHEN v.RecordDate IS NULL THEN
        CASE
          WHEN EXTRACT(HOUR FROM CURRENT_TIMESTAMP) BETWEEN 0 AND 5 THEN 'Midnight'
          WHEN EXTRACT(HOUR FROM CURRENT_TIMESTAMP) BETWEEN 6 AND 11 THEN 'Morning'
          WHEN EXTRACT(HOUR FROM CURRENT_TIMESTAMP) BETWEEN 12 AND 17 THEN 'Afternoon'
          ELSE 'Evening'
        END
      ELSE
        CASE
          WHEN EXTRACT(HOUR FROM TO_TIMESTAMP(v.RecordDate)) BETWEEN 0 AND 5 THEN 'Midnight'
          WHEN EXTRACT(HOUR FROM TO_TIMESTAMP(v.RecordDate)) BETWEEN 6 AND 11 THEN 'Morning'
          WHEN EXTRACT(HOUR FROM TO_TIMESTAMP(v.RecordDate)) BETWEEN 12 AND 17 THEN 'Afternoon'
          ELSE 'Evening'
        END
    END AS Time_Bucket
  FROM viewership v
  FULL JOIN user_profiles u
    ON v.UserID0 = u.userid
)
SELECT
  *,
  CASE
    WHEN duration_minutes <= 10 THEN 10
    WHEN duration_minutes <= 20 THEN 20
    WHEN duration_minutes <= 30 THEN 30
    WHEN duration_minutes <= 40 THEN 40
    WHEN duration_minutes <= 50 THEN 50
    ELSE 60
  END AS Duration_buckets
FROM joined
ORDER BY UserID;

