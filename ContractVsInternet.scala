val view1  = spark.sql("create or replace view v1 as select Contract, InternetService, count(Churn) as count from telco where Churn like 'Yes' and Contract like 'Month-to-month' and InternetService not like 'No' group by Contract, InternetService")

val view3  = spark.sql("create or replace view v3 as select Contract, InternetService, count(Churn) as totalcount from telco where InternetService not like 'No' group by Contract, InternetService")

val ot_9= spark.sql ("select v1.Contract as Contract, v1.InternetService, v1.count as count, v3.totalcount as totalcount, round(v1.count*100/v3.totalcount) as PrcntChurn from v1,v3 where v1.Contract=v3.Contract and v1.InternetService = v3.InternetService")

ot_9.show()