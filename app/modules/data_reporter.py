# data_reporter.py
from pyspark.sql.functions import col, count, max, avg,row_number
from pyspark.sql.window import Window
from app.modules.data_processor import DataWriter 


class DataReporter:
    def __init__(self, spark):
        self.spark = spark
        self.data_writer = DataWriter(spark)  

#1 Total equipment failures that happened?

    def total_failures(self, equipment_failure_sensors, db_schema):
        total_failures_df = equipment_failure_sensors.filter(col("severity") == "ERROR") \
                                    .agg(count("*").alias("total_failures"))
        self.data_writer.write_to_database({"total_failures": total_failures_df}, db_schema) #Escreve no BD
        print("") # Print vazio para melhorar a identação do relatorio em tela
        print("#1 Total equipment failures that happened?")#Printa no terminal
        total_failures_df.show() 

#2 Which equipment name had most failures?

    def equipment_most_failures(self, equipment_failure_sensors, equipment_sensors_df, equipment_df, db_schema):
        failures_df = equipment_failure_sensors.alias("efs").join(equipment_sensors_df.alias("es"), col("efs.sensor_id") == col("es.sensor_id")) \
            .join(equipment_df.alias("e"), col("e.equipment_id") == col("es.equipment_id")) \
            .filter(col("efs.severity") == "ERROR") \
            .groupBy("efs.sensor_id") \
            .agg(max(col("e.name")).alias("equipment_name"), count("*").alias("count")) \
            .orderBy(col("count").desc()).limit(1)
        self.data_writer.write_to_database({"failures_df": failures_df}, db_schema)
        print("#2 Which equipment name had most failures?")
        failures_df.show() 

#3 Average amount of failures across equipment group, ordered by the number of failures in ascending order?

    def average_failures_per_group(self, equipment_failure_sensors, equipment_sensors_df, equipment_df, db_schema):
        failures_cte_df = equipment_failure_sensors.alias("efs").join(equipment_sensors_df.alias("es"), col("efs.sensor_id") == col("es.sensor_id")) \
            .join(equipment_df.alias("e"), col("e.equipment_id") == col("es.equipment_id")) \
            .filter(col("efs.severity") == "ERROR") \
            .groupBy("e.group_name","e.equipment_id") \
            .agg(count("*").alias("total_failures"))
        
        average_failures_df = failures_cte_df.groupBy(failures_cte_df["group_name"]) \
            .agg(avg(col("total_failures")).alias("average_failures")) \
            .orderBy(col("average_failures").asc())
        self.data_writer.write_to_database({"average_failures_df": average_failures_df}, db_schema)
        print("#3 Average amount of failures across equipment group, ordered by the number of failures in ascending order?")
        average_failures_df.show()

#4 Rank the sensors which present the most number of errors by equipment name in an equipment group.

    def sensor_errors_ranking(self, equipment_failure_sensors, equipment_sensors_df, equipment_df, db_schema):
        windowSpec = Window.partitionBy("e.name").orderBy(col("error_count").desc())

        sensor_errors_ranking_df = equipment_failure_sensors.alias("efs") \
            .join(equipment_sensors_df.alias("es"), col("efs.sensor_id") == col("es.sensor_id")) \
            .join(equipment_df.alias("e"), col("e.equipment_id") == col("es.equipment_id")) \
            .filter(col("severity") == "ERROR") \
            .groupBy("e.name", "es.sensor_id") \
            .agg(count("*").alias("error_count")) \
            .withColumn("ranking", row_number().over(windowSpec)) \
            .filter(col("ranking") == 1) \
            .select(col("e.name").alias("equipment_name"), col("es.sensor_id"), col("error_count"))

        failure_ranking_df = sensor_errors_ranking_df.alias("ser") \
            .join(equipment_df.alias("eqp"), col("ser.equipment_name") == col("eqp.name")) \
            .select(col("ser.equipment_name"), col("eqp.group_name"), col("ser.error_count"))

        self.data_writer.write_to_database({"final_ranking_df": failure_ranking_df}, db_schema)
        print("#4 Rank the sensors which present the most number of errors by equipment name in an equipment group.")
        failure_ranking_df.show()