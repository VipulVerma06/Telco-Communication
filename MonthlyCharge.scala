val ot_7 = spark.sql("select 'No/Yes' as Churning, InternetService, round(avg(MonthlyCharges)) as Average from telco where InternetService not like 'No' group by InternetService") 

val ot_8 = spark.sql("select  'Yes' as Churning, InternetService, round(avg(MonthlyCharges)) as Average  from telco where Churn like 'Yes' and InternetService not like 'No' group by InternetService")

ot_7.show()

ot_8.show()