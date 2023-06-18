-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT 
    FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', _table_suffix)) AS month,
    SUM(totals.visits) AS visits,
    SUM(totals.pageviews) AS pageviews,
    SUM(totals.transactions) AS transactions,
    SUM(totals.totalTransactionRevenue) / POW(10, 6) AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _table_suffix BETWEEN '20170101' AND '20170331'
GROUP BY month
ORDER BY month



-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT 
    trafficSource.source,
    SUM(totals.visits) AS total_visits,
    SUM(totals.bounces) AS total_no_of_bounces,
    SUM(totals.bounces) / SUM(totals.visits)*100 AS bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY trafficSource.source
ORDER BY total_visits DESC



-- Query 3: Revenue by traffic source by week, by month in June 2017
#standardSQL
WITH A AS (SELECT 
              'Month' AS time_type,
              FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', _table_suffix)) AS time,
              trafficSource.source,
              SUM(totals.totalTransactionRevenue) / POW(10, 6) AS revenue
           FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
           WHERE _table_suffix BETWEEN '20170601' AND '20170630'
           GROUP BY time, trafficSource.source),

     B AS (SELECT 
              'Week' AS time_type,
              FORMAT_DATE('%Y%W',PARSE_DATE('%Y%m%d', _table_suffix)) AS time,
              trafficSource.source,
              SUM(totals.totalTransactionRevenue) / POW(10, 6) AS revenue
           FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
           WHERE _table_suffix BETWEEN '20170601' AND '20170630'
           GROUP BY time, trafficSource.source)

SELECT *
FROM A 
UNION ALL
SELECT *
FROM B  



--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
WITH A AS (SELECT
              FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', _table_suffix)) AS month,
              SUM(totals.pageviews) / COUNT(DISTINCT FullVisitorId) AS avg_pageviews_purchase
           FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
           WHERE (_table_suffix BETWEEN '20170601' AND '20170731')
                 AND (totals.transactions>=1)
           GROUP BY month),

     B AS (SELECT
              FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', _table_suffix)) AS month,
              SUM(totals.pageviews) / COUNT(DISTINCT FullVisitorId) AS avg_pageviews_non_purchase
           FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
           WHERE (_table_suffix BETWEEN '20170601' AND '20170731')
                 AND (totals.transactions IS NULL)
           GROUP BY month)

SELECT 
    A.month,
    A.avg_pageviews_purchase,
    B.avg_pageviews_non_purchase,
FROM A
JOIN B ON A.month=B.month
ORDER BY month



-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
SELECT
    FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', _table_suffix)) AS Month,
    SUM(totals.transactions) / COUNT(DISTINCT FullVisitorId) AS Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE (_table_suffix BETWEEN '20170701' AND '20170731')
      AND (totals.transactions>=1)
GROUP BY Month



-- Query 06: Average amount of money spent per session
#standardSQL
SELECT
    FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', _table_suffix)) AS Month,
    ROUND(SUM(totals.totalTransactionRevenue) / COUNT(totals.visits), 2) AS avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE (_table_suffix BETWEEN '20170701' AND '20170731')
      AND (totals.transactions IS NOT NULL)
GROUP BY Month



-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)
#standardSQL
WITH A AS (SELECT
              FullVisitorId
           FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
                UNNEST (hits) AS hits,
                UNNEST (hits.product) AS product
           WHERE (_table_suffix BETWEEN '20170701' AND '20170731')
             AND (product.v2ProductName="YouTube Men's Vintage Henley")
             AND (product.productRevenue IS NOT NULL))

SELECT 
    product.v2ProductName AS other_purchased_products,
    SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
     UNNEST (hits) AS hits,
     UNNEST (hits.product) AS product
WHERE (_table_suffix BETWEEN '20170701' AND '20170731')
  AND (product.productRevenue IS NOT NULL)
  AND (FullVisitorId IN (SELECT FullVisitorId
                         FROM A))
  AND product.v2ProductName<>"YouTube Men's Vintage Henley"
GROUP BY other_purchased_products
ORDER BY quantity DESC



--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
WITH A AS (SELECT
              FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', _table_suffix)) AS month,
              COUNT(product.v2ProductName) AS num_product_view
           FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
                UNNEST (hits) AS hits,
                UNNEST (hits.product) AS product
           WHERE (_table_suffix BETWEEN '20170101' AND '20170331')
           AND hits.ecommerceaction.action_type = '2'
           GROUP BY month),

     B AS (SELECT
              FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', _table_suffix)) AS month,
              COUNT(product.v2ProductName) AS num_addtocart
           FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
                UNNEST (hits) AS hits,
                UNNEST (hits.product) AS product
           WHERE (_table_suffix BETWEEN '20170101' AND '20170331')
           AND hits.ecommerceaction.action_type = '3'
           GROUP BY month),     

     C AS (SELECT
              FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', _table_suffix)) AS month,
              COUNT(product.v2ProductName) AS num_purchase
           FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
                UNNEST (hits) AS hits,
                UNNEST (hits.product) AS product
           WHERE (_table_suffix BETWEEN '20170101' AND '20170331')
           AND hits.ecommerceaction.action_type = '6'
           GROUP BY month),

     D AS (SELECT 
              A.month,
              A.num_product_view,
              B.num_addtocart,
              C.num_purchase
           FROM A
           JOIN B ON A.month=B.month
           JOIN C ON A.month=C.month
           ORDER BY A.month)

SELECT 
    *,
    ROUND((num_addtocart/num_product_view)*100, 2) AS add_to_cart_rate,
    ROUND((num_purchase/num_product_view)*100, 2) AS purchase_rate
FROM D
ORDER BY month 
