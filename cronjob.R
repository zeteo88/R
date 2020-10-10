library(RMySQL)
library(RMariaDB)
connection <- dbConnect(MySQL(), user = 'root', password = '', host = 'localhost', dbname = 'dopmw_orders_at')
frame <- dbGetQuery(connection, "SELECT * FROM dopmw_orders_at.orders LIMIT 5")


orders_group_frame <- dbGetQuery(connection, "SELECT * FROM dopmw_orders_at.orders_group LIMIT 5")


                       
merged_frame <- merge(frame, orders_group_frame, by.x="orders_group_id", by.y="id")

subset_frame <- merged_frame[c(1,2:4)]

dbWriteTable(connection, "subset_frame", subset_frame, append = TRUE)
