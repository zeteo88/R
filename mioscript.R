library(RMySQL)
library(RMariaDB)
connection <- dbConnect(MySQL(), user = 'root', password = '', host = 'localhost', dbname = 'dopmw_orders_at')
ordersat_frame <- dbGetQuery(connection, 
"SELECT
o.merchant AS 'Merchant',
op.id AS 'OP_id',
'Article' AS 'OP_orderpositiontype',
op.order_position_id AS 'Order Position UID',
og.id AS 'Order Group ID', -- Used Order_Group_ID
o.orders_id AS 'Order UID', -- not available in Backend
op.duin AS 'DUIN',
op.product_name AS 'OP_name',
ops.`name` AS 'OP_orderpositionstatus',
os.`name` AS 'O_orderstatus',
(CASE op.condition_id
       WHEN 1 THEN 'New'
       WHEN 2 THEN 'Fake New'
       WHEN 3 THEN 'Used Like New'
       WHEN 4 THEN 'Used Very Good'
       WHEN 5 THEN 'Used Good'
       WHEN 6 THEN 'Used Acceptable'
       WHEN 9 THEN 'Refurbished'
       ELSE '' END) AS 'OP_sellingCondition', 

og.user_id AS 'shopuserId',
a.country_code AS 'Country Code',
a.zip AS 'Zip',
a.city AS 'City',

o.payment_method AS 'OP_paymentType',
o.payment_psp AS 'Payment Service provider',
og.voucher_code AS 'Voucher Code',
og.payment_method_name AS 'Payment_Method',

opsc.carrier_name AS 'Carrier',
opsc.shipping_method AS 'OP_shippingmethod',
og.shipping_method_name,


op.currency AS 'OP_currency',
op.tax_type_value AS 'Tax_Value',
op.final_price_net AS 'Final_Price_Net',
op.final_price_gross AS 'Final_Price_Gross',
op.shipping_cost_gross AS 'Shipping_Cost_Gross',
op.shipping_cost_net AS 'Shipping_Cost_Net',
-- REFUND INFO
ref.refund_amount AS 'Refund Amount',
refr.`name`  AS 'Refund Reason',


oret.return_date AS 'Return Date',
oret.return_reason AS 'Return Reason',  


opc.cancellation_reason AS 'Cancel Reason',


'3rd party vendor' AS 'Vendor',
'3rd party vendor group' AS 'Vendor Group',

opsc.shipping_date,
og.orders_group_date AS 'Creation_Date',
og.orders_group_date AS 'INITDATE',
     GROUP_CONCAT((CASE oph.new_status_id WHEN '2' THEN oph.`date` ELSE NULL END)) AS 'PREQUESTDATE',
     GROUP_CONCAT((CASE oph.new_status_id WHEN '3' THEN oph.`date` ELSE NULL END)) AS 'PENDINGDATE',
     GROUP_CONCAT((CASE oph.new_status_id WHEN '4' THEN oph.`date` ELSE NULL END)) AS 'INPORGRESSDATE',
     GROUP_CONCAT((CASE oph.new_status_id WHEN '5' THEN oph.`date` ELSE NULL END)) AS 'COMPLETEDDATE',
     GROUP_CONCAT((CASE oph.new_status_id WHEN '6' THEN oph.`date` ELSE NULL END)) AS 'CANCELEDDATE',
     GROUP_CONCAT((CASE oph.new_status_id WHEN '7' THEN oph.`date` ELSE NULL END)) AS 'PFAILDATE',
     GROUP_CONCAT((CASE oph.new_status_id WHEN '8' THEN oph.`date` ELSE NULL END)) AS 'PDONEDATE',
op.notification_sent_date AS 'Shipping_Notification_Sent_Date'


FROM
orders_group og
LEFT JOIN orders o ON o.orders_group_id = og.id
LEFT JOIN order_position op ON op.orders_id = o.id
LEFT JOIN order_position_history oph ON oph.order_position_id = op.id
LEFT JOIN order_position_status ops ON ops.id = op.order_position_status_id
LEFT JOIN orders_status os ON os.id = o.orders_status_id
LEFT JOIN orders_has_address oha ON oha.orders_id = o.id
LEFT JOIN address a ON a.id = oha.address_id
LEFT JOIN order_position_shipping_confirmation opsc ON opsc.order_position_id = op.id
LEFT JOIN order_return oret ON oret.order_position_id = op.id
LEFT JOIN refund ref ON ref.order_position_id = op.id
LEFT JOIN refund_reason refr ON refr.id = ref.refund_reason_id
LEFT JOIN order_position_cancellation opc ON opc.order_position_id = op.id

WHERE DATE(og.orders_group_date) >= DATE('2020-01-01')

GROUP BY op.id                             
")


# dbWriteTable(connection, "subset_frame", subset_frame, append = TRUE)
