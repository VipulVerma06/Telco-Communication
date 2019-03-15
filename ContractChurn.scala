val view1  = spark.sql("create or replace view v1 as select Contract,count(Churn) as count from telco where Churn like 'Yes' group by Contract")

val view3  = spark.sql("create or replace view v3 as select Contract, count(Churn) as totalcount from telco group by Contract")

val ot_8= spark.sql ("select v1.Contract as Contract, v1.count as count, v3.totalcount as totalcount, round(v1.count*100/v3.totalcount) as PrcntChurn from v1,v3 where v1.Contract=v3.Contract")

ot_8.show()