query_stats1 = '''BEGIN;
                   DROP TABLE IF EXISTS '''
query_stats2 = ''';
                 CREATE TABLE '''
query_stats3 = ''' AS 
SELECT *, (100*sum_amount/SUM(sum_amount) 
           OVER (PARTITION BY timestamp, source)) AS "sale_%",
           (avg_price-LAG(avg_price) OVER(PARTITION BY source, type ORDER BY timestamp))/
           LAG(avg_price) OVER(PARTITION BY source, type ORDER BY timestamp)*100 avg_price_change 
  FROM
    (SELECT timestamp, source, type, 
        AVG(price) AS avg_price, MIN(price) AS min_price,
        MAX(price) AS max_price, SUM(amount) as sum_amount
       FROM crypto_hourly 
      GROUP BY timestamp, source, type
      ORDER BY timestamp, source) stats;
COMMIT;'''