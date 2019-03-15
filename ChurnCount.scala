spark.sql("create or replace view p1 as select Churn, count(Churn) as CustomerChurn from telco where Churn like 'Yes' group by Churn")

spark.sql("create or replace view p2 as select 'Yes' as Churn, count(Churn) as TotalCustomer from telco")

val output_1 = spark.sql("select p2.TotalCustomer as TotalCustomer, p1.CustomerChurn from p1, p2 where p1.Churn = p2.Churn")

output_1.show()	