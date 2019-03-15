val ot_16 = spark.sql("Select Churn, count(StreamingTV) from telco where OnlineSecurity like 'No' and OnlineBackup like 'No' and DeviceProtection like 'No' and TechSupport like 'No' and StreamingTV like 'Yes' and StreamingMovies like 'No' and Churn like 'Yes' group by Churn")

val ot_17 = spark.sql("Select count(StreamingTV) as CountStreamingTV from telco where OnlineSecurity like 'No' and OnlineBackup like 'No' and DeviceProtection like 'No' and TechSupport like 'No' and StreamingTV like 'Yes' and StreamingMovies like 'No'")

val ot_18 = spark.sql("Select Churn, count(StreamingMovies) from telco where OnlineSecurity like 'No' and OnlineBackup like 'No' and DeviceProtection like 'No' and TechSupport like 'No' and StreamingTV like 'No' and StreamingMovies like 'Yes' and Churn like 'Yes' group by Churn")

val ot_19 = spark.sql("Select count(StreamingMovies) as CountStreamingMovies from telco where OnlineSecurity like 'No' and OnlineBackup like 'No' and DeviceProtection like 'No' and TechSupport like 'No' and StreamingTV like 'No' and StreamingMovies like 'Yes'")

ot_16.show()
ot_17.show()
ot_18.show()
ot_19.show()