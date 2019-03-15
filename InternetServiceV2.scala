val view1  = spark.sql("create or replace view v1 as select PhoneService, 'Yes' as IS,count(Churn) as count from telco where InternetService not like 'No' and  PhoneService like 'No' and Churn like 'Yes' group by PhoneService, 'Yes'")

val view3  = spark.sql("create or replace view v3 as select PhoneService, 'Yes' as IS, count(Churn) as totalcount from telco where InternetService not like 'No' and PhoneService like 'No' group by PhoneService, 'Yes'")

val ot_6= spark.sql ("select v1.PhoneService as PhoneService, v1.IS as InternetService, v1.count as count, v3.totalcount as totalcount, round(v1.count*100/v3.totalcount) as PrcntChurn from v1,v3 where v1.PhoneService=v3.PhoneService")

ot_6.show()