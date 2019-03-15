val view1  = spark.sql("create or replace view v1 as select InternetService, count(Churn) as count from telco where InternetService not like 'No' and Churn like 'Yes' group by InternetService")

val view3  = spark.sql("create or replace view v3 as select InternetService, count(Churn) as totalcount from telco where InternetService not like 'No' group by InternetService")

val ot_7= spark.sql ("select v1.InternetService as InternetService, v1.count as count, v3.totalcount as totalcount, round(v1.count*100/v3.totalcount) as PrcntChurn from v1,v3 where v1.InternetService=v3.InternetService")

ot_7.show()