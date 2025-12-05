use magist123;
SELECT 
    YEAR(order_purchase_timestamp) AS year_,
    MONTH(order_purchase_timestamp) AS month_,
    COUNT(customer_id)
FROM
    orders
GROUP BY year_ , month_
ORDER BY year_ , month_;

SELECT 
    first_year AS year_,
    first_month AS month_,
    COUNT(customer_id) AS new_customers
FROM first_purchase
GROUP BY first_year, first_month
ORDER BY first_year, first_month;




SELECT 
    YEAR(order_purchase_timestamp) AS year_,
    MONTH(order_purchase_timestamp) AS month_,
    COUNT(distinct customer_id)
FROM
    orders
GROUP BY year_ , month_
ORDER BY year_ , month_ ;

select * from orders;

select timestampdiff(order_purchase_timestamp,order_approved_at)
from orders;
select avg(timestampdiff(hour, order_purchase_timestamp,order_estimated_delivery_date)) / 24 as days
from orders;
SELECT 
    FLOOR(AVG(TIMESTAMPDIFF(hour, order_purchase_timestamp,order_estimated_delivery_date)) / 24) AS avg_days,
    MOD(AVG(TIMESTAMPDIFF(hour, order_purchase_timestamp,order_estimated_delivery_date)), 24) AS remainder_hours
FROM orders;



SELECT
    SUM(CASE WHEN category = '<750' THEN total_count ELSE 0 END)  /
    SUM(total_count) * 100 as percent
    from
(SELECT 
count(payment_value) as total_count, case when payment_value <20 then '<20'
					when payment_value <50 then '<50'
					when payment_value <150 then '<150'
                    when payment_value <350 then '<350'
                    when payment_value <750 then '<750'
                    when payment_value <1000 then '<1000' 
                    else 'over 1000' 
                    end as category
FROM
	order_payments
    group by category) as tiers;   
    
    sum(total_count) 
from
(SELECT 
count(payment_value) as total_count, case when payment_value <20 then '<20'
					when payment_value <50 then '<50'
					when payment_value <150 then '<150'
                    when payment_value <350 then '<350'
                    when payment_value <750 then '<750'
                    when payment_value <1000 then '<1000' 
                    else 'over 1000' 
                    end as category
FROM
	order_payments
    group by category) as tiers;,   sum(total_count) 
from
(SELECT 
count(payment_value) as total_count, case when payment_value <20 then '<20'
					when payment_value <50 then '<50'
					when payment_value <150 then '<150'
                    when payment_value <350 then '<350'
                    when payment_value <750 then '<750'
                    when payment_value <1000 then '<1000' 
                    else 'over 1000' 
                    end as category
FROM
	order_payments
    group by category) as tiers;,   sum(total_count) 
from
(SELECT 
count(payment_value) as total_count, case when payment_value <20 then '<20'
					when payment_value <50 then '<50'
					when payment_value <150 then '<150'
                    when payment_value <350 then '<350'
                    when payment_value <750 then '<750'
                    when payment_value <1000 then '<1000' 
                    else 'over 1000' 
                    end as category
FROM
	order_payments
    group by category) as tiers;
    
    SELECT 
count(payment_value) as total_count, case when payment_value <20 then '<20'
					when payment_value <50 then '<50'
					when payment_value <150 then '<150'
                    when payment_value <350 then '<350'
                    when payment_value <750 then '<750'
                    when payment_value <1000 then '<1000' 
                    else 'over 1000' 
                    end as category
FROM
	order_payments
    group by category;
    
    
    select * from order_items;
    select count(order_id) from order_items;
    -- 112650
    
    select * from order_payments;
    select count(order_id) from order_payments;
    -- 103886
    select sum(payment_value)
    from order_payments;
    -- 16008872
    
    select * from product_category_name_translation;
    -- 'audio', 'cine_photo','electronics', 'computers_accesories','music','pc_gamer','computers','watches_gifts','tablets','printing_images','telephony'
    
    
    from orders
    left join order_items using (order_id)
    left join products using (product_id)
    left join product_category_name_translation using (product_category_name)
    
    
    SELECT
    SUM(CASE WHEN category = '<750' THEN total_count ELSE 0 END)  /
    SUM(total_count) * 100 as percent
    from
(SELECT 
count(payment_value) as total_count, case when payment_value <20 then '<20'
					when payment_value <50 then '<50'
					when payment_value <150 then '<150'
                    when payment_value <350 then '<350'
                    when payment_value <750 then '<750'
                    when payment_value <1000 then '<1000' 
                    else 'over 1000' 
                    end as category
	from order_payments
    left join orders using (order_id)
    left join order_items using (order_id)
    left join products using (product_id)
    left join product_category_name_translation using (product_category_name)
    group by category) as tiers; 
    
  -- Percentage of sold all products in tageted price range   
    
    SELECT
    SUM(CASE WHEN category = '350-750' THEN total_count ELSE 0 END) /
    SUM(total_count) * 100 AS percent
FROM (
    SELECT
        COUNT(op.payment_value) AS total_count,
        CASE
            WHEN op.payment_value < 20  THEN '0-20'
            WHEN op.payment_value < 50  THEN '20-50'
            WHEN op.payment_value < 150 THEN '50-150'
            WHEN op.payment_value < 350 THEN '150-350'
            WHEN op.payment_value < 750 THEN '350-750'
            WHEN op.payment_value < 1000 THEN '<1000'
            ELSE 'over 1000'
        END AS category
    FROM order_payments AS op
    LEFT JOIN `orders` AS o USING (order_id)
    LEFT JOIN order_items AS oi USING (order_id)
    LEFT JOIN products AS p USING (product_id)
    LEFT JOIN product_category_name_translation AS t USING (product_category_name)
    
    GROUP BY category
    
) AS tiers;
    
    
    
     SELECT
    SUM(CASE WHEN category = '350-750' THEN total_count ELSE 0 END) /
    SUM(total_count) * 100 AS percent
FROM (
    SELECT
        COUNT(op.payment_value) AS total_count,
        CASE
            WHEN op.payment_value < 20  THEN '0-20'
            WHEN op.payment_value < 50  THEN '20-50'
            WHEN op.payment_value < 150 THEN '50-150'
            WHEN op.payment_value < 350 THEN '150-350'
            WHEN op.payment_value < 750 THEN '350-750'
            WHEN op.payment_value < 1000 THEN '<1000'
            ELSE 'over 1000'
        END AS category
    FROM order_payments AS op
    LEFT JOIN `orders` AS o USING (order_id)
    LEFT JOIN order_items AS oi USING (order_id)
    LEFT JOIN products AS p USING (product_id)
    LEFT JOIN product_category_name_translation AS t USING (product_category_name)
    where product_category_name_english in ('audio', 'cine_photo','electronics', 'computers_accesories','music','pc_gamer','computers','watches_gifts','tablets','printing_images','telephony')
    GROUP BY category 
    
) AS tiers;
    
  select avg(timestampdiff(hour, o.order_purchase_timestamp,o.order_delivered_customer_date)) / 24
from orders  o
 LEFT JOIN order_items AS oi USING (order_id)
    LEFT JOIN products AS p USING (product_id)
    LEFT JOIN product_category_name_translation AS t USING (product_category_name)
    where product_category_name_english in ('audio', 'cine_photo','electronics', 'computers_accesories','music','pc_gamer','computers','watches_gifts','tablets_printing_image','telephony');
  
  
  SELECT ROUND(AVG(oi.price), 2)
FROM  order_items oi
LEFT JOIN products p
	ON p.product_id = oi.product_id
LEFT JOIN product_category_name_translation pt
	USING (product_category_name)
WHERE pt.product_category_name_english IN ("audio", "electronics", "computers_accessories", "pc_gamer",
                                            "computers", "tablets_printing_image", "telephony")
; 



SELECT count(oi.price),  oi.product_id, oi.price
FROM  order_items oi
LEFT JOIN products p
	ON p.product_id = oi.product_id
LEFT JOIN product_category_name_translation pt
	USING (product_category_name)
WHERE pt.product_category_name_english IN ("audio", "electronics", "computers_accessories", "pc_gamer",
                                            "computers", "tablets_printing_image", "telephony")
and oi.price between 350 and 750
group by oi.product_id, oi.price
 ;
 
 SELECT count(order_id)
FROM  order_items oi
LEFT JOIN products p
	ON p.product_id = oi.product_id
LEFT JOIN product_category_name_translation pt
	USING (product_category_name)
WHERE pt.product_category_name_english IN ("audio", "electronics", "computers_accessories", "pc_gamer",
                                            "computers", "tablets_printing_image", "telephony")
;

SELECT  avg(price)
FROM  order_items oi
LEFT JOIN products p
	ON p.product_id = oi.product_id
LEFT JOIN product_category_name_translation pt
	USING (product_category_name)
WHERE pt.product_category_name_english IN ("audio", "electronics", "computers_accessories", "pc_gamer",
                                            "computers", "tablets_printing_image", "telephony")

;


select distinct order_status from orders;

SELECT order_status, COUNT(order_status) sum_of_order_status FROM orders GROUP BY order_status;






select count(review_score), review_score
from order_reviews 
right join orders o using (order_id)
right join  order_items oi using (order_id)
right JOIN products p
	ON p.product_id = oi.product_id
right JOIN product_category_name_translation pt
	USING (product_category_name)
WHERE pt.product_category_name_english IN ("audio", "electronics", "computers_accessories", "pc_gamer",
                                            "computers", "tablets_printing_image", "telephony")
and oi.price between 350 and 750
group by review_score;


 
SELECT 
COUNT(DISTINCT DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m')), 2) AS avg_monthly_income_tech
FROM sellers s
JOIN order_items oi ON oi.seller_id = s.seller_id
JOIN orders o ON o.order_id = oi.order_id
JOIN products p ON p.product_id = oi.product_id
JOIN product_category_name_translation pt
ON pt.product_category_name = p.product_category_name
WHERE o.order_status NOT IN ('unavailable','canceled')
AND pt.product_category_name_english IN ('audio','electronics','computers_accessories',
'pc_gamer','computers','tablets_printing_image',
'telephony');

SELECT max(oi.price)
FROM order_items oi
JOIN orders o ON o.order_id = oi.order_id
JOIN products p ON p.product_id = oi.product_id
JOIN product_category_name_translation pt
ON pt.product_category_name = p.product_category_name
WHERE o.order_status NOT IN ('unavailable','canceled')
AND pt.product_category_name_english IN ('audio','electronics','computers_accessories',
'pc_gamer','computers','tablets_printing_image',
'telephony');

 select COUNT(DISTINCT order_id) AS orders_count
FROM orders ;

SELECT
    CASE 
        WHEN DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) >= 100 THEN "> 100 day Delay"
        WHEN DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) >= 7 AND DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) < 100 THEN "1 week to 100 day delay"
        WHEN DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) > 3 AND DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) < 7 THEN "4-7 day delay"
        WHEN DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) >= 1  AND DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) <= 3 THEN "1-3 day delay"
        WHEN DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) > 0  AND DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) < 1 THEN "less than 1 day delay"
        WHEN DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) <= 0 THEN 'On time' 
    END AS delay_range, 
    AVG(product_weight_g) AS weight_avg,
    MAX(product_weight_g) AS max_weight,
    MIN(product_weight_g) AS min_weight,
    SUM(product_weight_g) AS sum_weight,
    COUNT(DISTINCT a.order_id) AS orders_count
FROM orders a
LEFT JOIN order_items b
    USING (order_id)
LEFT JOIN products c
    USING (product_id)
WHERE order_estimated_delivery_date IS NOT NULL
AND order_delivered_customer_date IS NOT NULL
AND order_status = 'delivered'
GROUP BY delay_range;


SELECT
    CASE 
        WHEN DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) >= 100 THEN "> 100 day Delay"
        WHEN DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) >= 7 AND DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) < 100 THEN "1 week to 100 day delay"
        WHEN DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) > 3 AND DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) < 7 THEN "4-7 day delay"
        WHEN DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) >= 1  AND DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) <= 3 THEN "1-3 day delay"
        WHEN DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) > 0  AND DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) < 1 THEN "less than 1 day delay"
        WHEN DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date) <= 0 THEN 'On time' 
    END AS delay_range, 
    AVG(product_weight_g) AS weight_avg,
    MAX(product_weight_g) AS max_weight,
    MIN(product_weight_g) AS min_weight,
    SUM(product_weight_g) AS sum_weight,
    COUNT(DISTINCT a.order_id) AS orders_count
FROM orders a
LEFT JOIN order_items b
    USING (order_id)
LEFT JOIN products c
    USING (product_id)
WHERE order_estimated_delivery_date IS NOT NULL
AND order_delivered_customer_date IS NOT NULL
AND order_status = 'delivered'
GROUP BY delay_range;



SELECT
avg( TIMESTAMPDIFF(hour, order_purchase_timestamp,order_delivered_customer_date)) / 24 AS avg_days,
CASE
WHEN p.product_weight_g >= 2000 THEN 'Heavy (>= 2kg)'
WHEN p.product_weight_g >= 500 THEN 'Medium (0.5–2kg)'
ELSE 'Light (< 0.5kg)'
END AS weight_category,
CASE
WHEN order_delivered_customer_date <= order_estimated_delivery_date THEN 'On time'
ELSE 'Delayed'
END AS delivery_status,
COUNT(DISTINCT o.order_id) AS orders_count
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
join product_category_name_translation pt using (product_category_name)
WHERE o.order_status = 'delivered' 
AND o.order_estimated_delivery_date IS NOT NULL
AND o.order_delivered_customer_date IS NOT NULL
and pt.product_category_name_english IN ('audio','electronics','computers_accessories',
'pc_gamer','computers','tablets_printing_image',
'telephony')
GROUP BY weight_category, delivery_status
ORDER BY weight_category, delivery_status;



select avg(timestampdiff(hour, order_purchase_timestamp,order_delivered_customer_date)) / 24 as avd_days_delivery
from geo g
join customers c on g.zip_code_prefix = c.customer_zip_code_prefix
join orders o using (customer_id)
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
join product_category_name_translation pt using (product_category_name)
WHERE o.order_status = 'delivered' 
AND o.order_estimated_delivery_date IS NOT NULL
AND o.order_delivered_customer_date IS NOT NULL
and pt.product_category_name_english IN ('audio','electronics','computers_accessories',
'pc_gamer','computers','tablets_printing_image',
'telephony')
and state in ('SP','RJ','DF');

select distinct city from geo;
select * from geo;

select avg(timestampdiff(hour, order_purchase_timestamp,order_delivered_customer_date)) / 24 as avg_days_delivery, min(timestampdiff(hour, order_purchase_timestamp,order_delivered_customer_date)) / 24 as avg_days_delivery, max(timestampdiff(hour, order_purchase_timestamp,order_delivered_customer_date)) / 24 as avg_days_delivery
from geo g
join customers c on g.zip_code_prefix = c.customer_zip_code_prefix
join orders o using (customer_id)
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
join product_category_name_translation pt using (product_category_name)
WHERE o.order_status = 'delivered'
and order_delivered_customer_date <= order_estimated_delivery_date
AND o.order_estimated_delivery_date IS NOT NULL
AND o.order_delivered_customer_date IS NOT NULL
and pt.product_category_name_english IN ('audio','electronics','computers_accessories',
'pc_gamer','computers','tablets_printing_image',
'telephony')
and state in ('SP','RJ','DF');

select distinct order_status from orders;


select review_score,avg(timestampdiff(hour, order_purchase_timestamp,order_delivered_customer_date)) / 24 as avg_days_delivery, count(review_score) 
from order_reviews 
join orders o using (order_id)
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
join product_category_name_translation pt using (product_category_name)
WHERE o.order_status = 'delivered'
and order_delivered_customer_date >= order_estimated_delivery_date
AND o.order_estimated_delivery_date IS NOT NULL
AND o.order_delivered_customer_date IS NOT NULL
and pt.product_category_name_english IN ('audio','electronics','computers_accessories',
'pc_gamer','computers','tablets_printing_image',
'telephony')
group by review_score;


SELECT 
    review_score,
    AVG(TIMESTAMPDIFF(HOUR, order_purchase_timestamp, order_delivered_customer_date)) / 24 AS avg_days_delivery,
    COUNT(review_score) AS cnt,
    ROUND(COUNT(review_score) * 100.0 / max(t.total_reviews), 2) AS pct_reviews
FROM order_reviews 
JOIN orders o USING (order_id)
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
JOIN product_category_name_translation pt USING (product_category_name)
CROSS JOIN (
    SELECT COUNT(review_score) AS total_reviews
    FROM order_reviews 
    JOIN orders o USING (order_id)
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    JOIN product_category_name_translation pt USING (product_category_name)
    WHERE o.order_status = 'delivered'
      AND order_delivered_customer_date <= order_estimated_delivery_date
      AND o.order_estimated_delivery_date IS NOT NULL
      AND o.order_delivered_customer_date IS NOT NULL
      AND pt.product_category_name_english IN ('audio','electronics','computers_accessories',
                                               'pc_gamer','computers','tablets_printing_image',
                                               'telephony')
) t
WHERE o.order_status = 'delivered'
  AND order_delivered_customer_date <= order_estimated_delivery_date
  AND o.order_estimated_delivery_date IS NOT NULL
  AND o.order_delivered_customer_date IS NOT NULL
  AND pt.product_category_name_english IN ('audio','electronics','computers_accessories',
                                           'pc_gamer','computers','tablets_printing_image',
                                           'telephony')
GROUP BY review_score;

SELECT 
count(payment_value) as total_count, case when payment_value <20 then '<20'
					when payment_value <50 then '<50'
					when payment_value <150 then '<150'
                    when payment_value <350 then '<350'
                    when payment_value <750 then '<750'
                    when payment_value <1000 then '<1000' 
                    else 'over 1000' 
                    end as category
FROM
	order_payments
    group by category;
    
    select * from order_items;
    
    select timestampdiff(hour, order_purchase_timestamp,order_delivered_customer_date) / 24 as days_delivery,shipping_limit_date,order_delivered_carrier_date,
    order_estimated_delivery_date,
    order_delivered_customer_date
from geo g
join customers c on g.zip_code_prefix = c.customer_zip_code_prefix
join orders o using (customer_id)
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
join product_category_name_translation pt using (product_category_name)
WHERE o.order_status = 'delivered'
AND o.order_estimated_delivery_date IS NOT NULL
AND o.order_delivered_customer_date IS NOT NULL
and pt.product_category_name_english IN ('audio','electronics','computers_accessories',
'pc_gamer','computers','tablets_printing_image',
'telephony')
;








 SELECT count(price) , case when payment_value <350 then '0 - 350'
					when payment_value <750 then '350 - 750'
					else 'over 750' 
					end as category
from order_payments op                     
join orders o using  (order_id)                 
join order_items oi using (order_id)
 JOIN products p
	ON p.product_id = oi.product_id
JOIN product_category_name_translation pt
	USING (product_category_name)
WHERE pt.product_category_name_english IN ("audio", "electronics", "computers_accessories", "pc_gamer",
                                            "computers", "tablets_printing_image", "telephony")
group by category;


SELECT sum(price), order_id ,case when sum(price) <550 then '0 - 550'
					when sum(price) <950 then '550 - 950'
					else 'over 950' 
					end as category_order_value
from order_payments op                     
join orders o using  (order_id)                 
join order_items oi using (order_id)
 JOIN products p
	ON p.product_id = oi.product_id
JOIN product_category_name_translation pt
	USING (product_category_name)
WHERE pt.product_category_name_english IN ("audio", "electronics", "computers_accessories", "pc_gamer",
                                            "computers", "tablets_printing_image", "telephony")
                                            group by category_order_value, order_id
;


select count(category_order_value), category_order_value 
from (SELECT 
  o.order_id,
  round(SUM(oi.price)) ,
  CASE 
    WHEN SUM(oi.price) < 550 THEN '0 - 550'
    WHEN SUM(oi.price) < 950 THEN '550 - 950'
    ELSE 'over 950'
  END AS category_order_value
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON p.product_id = oi.product_id
JOIN product_category_name_translation pt USING (product_category_name)
WHERE pt.product_category_name_english IN (
  'audio','electronics','computers_accessories','pc_gamer',
  'computers','tablets_printing_image','telephony'
)
GROUP BY o.order_id) as tab
group by category_order_value;


SELECT sum(price) / 25 as Ravenue_Majest_Tech_per_m7
from order_payments op                     
join orders o using  (order_id)                 
join order_items oi using (order_id)
 JOIN products p
	ON p.product_id = oi.product_id
JOIN product_category_name_translation pt
	USING (product_category_name)
WHERE pt.product_category_name_english IN ("audio", "electronics", "computers_accessories", "pc_gamer",
                                            "computers", "tablets_printing_image", "telephony")
                                         

 