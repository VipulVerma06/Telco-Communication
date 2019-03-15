spark.sql("create or replace view p1 as select Churn, count(OnlineSecurity) as OnlineSecurity from  telco where OnlineSecurity like 'Yes' group by Churn")

spark.sql("create or replace view p2 as select Churn, count(OnlineBackup) as OnlineBackup from  telco where OnlineBackup like 'Yes' group by  Churn")

spark.sql("create or replace view p3 as select Churn, count(DeviceProtection) as DeviceProtection from  telco where DeviceProtection like 'Yes' group by  Churn")

spark.sql("create or replace view p4 as select Churn, count(TechSupport) as TechSupport from  telco where TechSupport like 'Yes' group by  Churn")

spark.sql("create or replace view p5 as select Churn, count(StreamingTV) as StreamingTV from  telco where StreamingTV like 'Yes' group by  Churn")

spark.sql("create or replace view p6 as select Churn, count(StreamingMovies) as StreamingMovies  from  telco where StreamingMovies like 'Yes' group by Churn")


spark.sql("create or replace view c as select p1.Churn as Churn, p1.OnlineSecurity as OnlineSecurity, p2.OnlineBackup as OnlineBackup, p3.DeviceProtection as DeviceProtection, p4.TechSupport as TechSupport, p5.StreamingTV as StreamingTV, p6.StreamingMovies as StreamingMovies from p1, p2, p3, p4, p5, p6  where p1.Churn = p2.Churn and p1.Churn = p3.Churn and p1.Churn = p4.Churn and p1.Churn = p5.Churn and p1.Churn = p6.Churn and p1.Churn like 'Yes'")



spark.sql("create or replace view s as select 'Yes' as Churn, sum(p1.OnlineSecurity) as OnlineSecurity, sum(p2.OnlineBackup) as OnlineBackup, sum(p3.DeviceProtection) as DeviceProtection, sum(p4.TechSupport) as TechSupport, sum(p5.StreamingTV) as StreamingTV, sum(p6.StreamingMovies) as StreamingMovies from p1, p2, p3, p4, p5, p6  where p1.Churn = p2.Churn and p1.Churn = p3.Churn and p1.Churn = p4.Churn and p1.Churn = p5.Churn and p1.Churn = p6.Churn")


val try3 = spark.sql("select 'Percent' as Percent_Churn, round(c.OnlineSecurity*100/s.OnlineSecurity) as OnlineSecurity, round(c.OnlineBackup*100/s.OnlineBackup) as OnlineBackup, round(c.DeviceProtection*100/s.DeviceProtection) as DeviceProtection, round(c.TechSupport*100/s.TechSupport) as TechSupport, round(c.StreamingTV*100/s.StreamingTV) as StreamingTV, round(c.StreamingMovies*100/s.StreamingMovies) as StreamingMovies from c,s where c.Churn = s.Churn")


spark.sql("select OnlineSecurity, OnlineBackup, DeviceProtection, TechSupport, StreamingTV, StreamingMovies from c").show()

spark.sql("select 'Total' as Total_Churn, OnlineSecurity, OnlineBackup, DeviceProtection, TechSupport, StreamingTV, StreamingMovies from s").show()

try3.show()