-- 1. Given table T_A with a varchar field “Customer_ID”, 
-- write a script that gives you the values of "Customer_ID" that are non-unique.
SELECT customer_id
  FROM T_A
 GROUP BY 1
HAVING COUNT(1)>1;

-- 2. Given tables T_A and T_B, both with a varchar field “Customer_ID”, 
-- write a script that finds the elements in T_A that are not in T_B
SELECT *
FROM T_A
WHERE customer_id NOT IN (SELECT customer_id FROM T_B);

-- 3. Given table T_A that has fields 
-- “Customer_ID”, “Claim_type”, “Claim_value”, “Claim_timestamp” 
-- write a script that creates a new table, T_A_last10, 
-- where you store the sum-total of the last 10 paid claims (“Claim_value”>0) 
-- for each customer and claim type.
CREATE TABLE T_A_last10 AS (
    
    WITH add_rank AS (
        SELECT customer_id
               , claim_type
               , claim_value
               , ROW_NUMBER() OVER (PARTITION BY customer_id, claim_type ORDER BY claim_timestamp ASC) AS claim_number
          FROM T_A
         WHERE claim_value > 0
    )
    SELECT customer_id
           , claim_type
           , SUM(claim_value) AS total_last10_claim_value
      FROM add_rank
     WHERE claim_number <= 10 -- last 10 paid claims
     GROUP BY 1,2

);

-- 4. Given table T_A that has fields 
-- “Customer_ID”, “Claim_type”, “Claim_value”, “Claim_timestamp” 
-- write a script that creates a new table, T_A_Claim_Value_With_Trend, 
-- that adds a new field representing the percentage increase/decrease in claim value 
-- compared to the preceding one for each customer and claim type.
CREATE TABLE T_A_Claim_Value_With_Trend AS (

    WITH add_previous_claim_value AS (
        SELECT customer_id
               , claim_type
               , claim_value
               , claim_timestamp
               , LAG(claim_value) OVER (PARTITION BY customer_id, claim_type ORDER BY claim_timestamp ASC) AS previous_claim_value
          FROM T_A
    )
    SELECT customer_id
           , claim_type
           , claim_value
           , claim_timestamp
           , IFF(previous_claim_value = 0, NULL, (claim_value - previous_claim_value)/previous_claim_value) AS claim_increase_pct
      FROM add_previous_claim_value

);

-- 5. Given table T_A that has fields 
-- “Customer_ID”, “Trip_Score”, “Trip_km”, “Trip_timestamp” 
-- write a script that creates a new table, “T_A_Trip_Score_Rolling_Average_Last80km”, 
-- that adds a new field named “Score_RAvg_Last80km” 
-- representing the distance-weighted average score 
-- for the last 80 km the user has driven up to (and including) the last trip, 
-- rounded to second decimal point.
CREATE TABLE T_A_Trip_Score_Rolling_Average_Last80km AS (

    WITH add_rolling_sum AS (

        SELECT SUM(trip_km) OVER (PARTITION BY customer_id ORDER BY trip_timestamp DESC) AS km_rolling_sum -- starting from the last trip
            , *
        FROM T_A
        
    )
    , flag_relevant_trips AS (

        SELECT km_rolling_sum
            , km_rolling_sum < 80 OR LAG(km_rolling_sum) OVER (PARTITION BY customer_id ORDER BY trip_timestamp DESC) < 80 AS is_last_80km
            , *
        FROM add_rolling_sum
        
    )
    , calculate_score AS (

        SELECT customer_id
            , ROUND(SUM(trip_score * trip_km) / SUM(trip_km), 2) AS Score_RAvg_Last80km
        FROM flag_relevant_trips
        WHERE is_last_80km -- last 80 km the user has driven, including last trip
        GROUP BY 1
        
    )
    SELECT * FROM calculate_score

);