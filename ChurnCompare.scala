val view1  = spark.sql("create or replace view v1 as select PhoneService, count(Churn) as count from telco where  Churn like 'Yes' group by PhoneService")

val view2  = spark.sql("create or replace view v2 as select PhoneService, count(Churn) as count from telco where Churn like 'Yes' and InternetService like 'No' group by PhoneService")

val view3  = spark.sql("create or replace view v3 as select PhoneService, count(Churn) as totalcount from telco group by PhoneService")

val ot_1 = spark.sql ("select v1.PhoneService as PhoneService, 'DSL/FiberOptic' as InternetService, v1.count as count, v3.totalcount as totalcount, round(v1.count*100/v3.totalcount) as PrcntChurn from v1,v3 where v1.PhoneService=v3.PhoneService")

val ot_2 = spark.sql ("select v2.PhoneService as PhoneService, 'No' as InternetService,v2.count as count, v3.totalcount as totalcount, round(v2.count*100/v3.totalcount) as PrcntChurn from v2,v3 where v2.PhoneService=v3.PhoneService")

ot_1.show()
ot_2.show()