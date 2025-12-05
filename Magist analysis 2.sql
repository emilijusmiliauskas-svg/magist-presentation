
-- 10 hours beween order and approval
select avg(timestampdiff(hour, order_purchase_timestamp,order_approved_at))
from orders;

-- 2 days 18 hours between approval and carrier pick up
select
 FLOOR(AVG(TIMESTAMPDIFF(HOUR, order_approved_at, order_delivered_carrier_date)) / 24) AS avg_days,
    MOD(AVG(TIMESTAMPDIFF(HOUR, order_approved_at, order_delivered_carrier_date)), 24) AS remainder_hours
FROM orders;

-- 9days and 7 hours
SELECT 
    FLOOR(AVG(TIMESTAMPDIFF(hour, order_delivered_carrier_date,order_delivered_customer_date)) / 24) AS avg_days,
    MOD(AVG(TIMESTAMPDIFF(hour, order_delivered_carrier_date,order_delivered_customer_date)), 24) AS remainder_hours
FROM orders; 

from orders;

-- 23 days and 17 hours


-- 12 days 12 hours actual average delivery time
SELECT 
    FLOOR(AVG(TIMESTAMPDIFF(hour, order_purchase_timestamp,order_delivered_customer_date)) / 24) AS avg_days,
    MOD(AVG(TIMESTAMPDIFF(hour, order_purchase_timestamp,order_delivered_customer_date)), 24) AS remainder_hours
FROM orders;

select avg(timestampdiff(hour, order_purchase_timestamp,order_delivered_customer_date)) / 24
from orders;

select avg(timestampdiff(hour, order_purchase_timestamp,order_estimated_delivery_date)) / 24
from orders;  -- 23.74 hours

-- Percentage of sold all products in targeted price range    - 5.4897

SELECT
    SUM(CASE WHEN category = '350-750' THEN total_count ELSE 0 END)  /
    SUM(total_count) * 100 as percent
    from
(SELECT 
count(payment_value) as total_count, case when payment_value <20 then '0-20'
					when payment_value <50 then '<20-50'
					when payment_value <150 then '50-150'
                    when payment_value <350 then '150-350'
                    when payment_value <750 then '350-750'
                    when payment_value <1000 then '<1000' 
                    else 'over 1000' 
                    end as category
FROM
	order_payments
    group by category) as tiers; 





  
    
  -- -- Percentage of sold targeted products in targeted price range  6.8047

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
    where product_category_name_english in ('audio','electronics', 'computers_accesories','music','pc_gamer','computers','watches_gifts','tablets_printing_images','telephony')
    GROUP BY category
) AS tiers;


-- average price of tech products - 105.99

SELECT ROUND(AVG(oi.price), 2)
FROM order_items oi
JOIN orders o ON o.order_id = oi.order_id
JOIN products p ON p.product_id = oi.product_id
JOIN product_category_name_translation pt
ON pt.product_category_name = p.product_category_name
WHERE o.order_status NOT IN ('unavailable','canceled')
AND pt.product_category_name_english IN ('audio','electronics','computers_accessories',
'pc_gamer','computers','tablets_printing_image',
'telephony');


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




-- categories by price range

 SELECT count(order_id), case when payment_value <350 then '0 - 350'
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
