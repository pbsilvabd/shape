# functions.py
from app.modules.data_processor import DataReader, DataWriter
from app.config.credentials import get_spark_session, DatabaseConfig
from app.modules.data_processor import DataReader
from app.modules.data_reporter import DataReporter


def DataAudition(db_schema):
    spark = get_spark_session()
    try:
        files_directory = DatabaseConfig.get_files_directory()
        # Leitura
        data_reader = DataReader(spark)
        # Coleta os dataframes
        dataframes = data_reader.read_files(files_directory)
        # Inicializa a classe de escrita
        data_writer = DataWriter(spark)
        # Salva no bd
        data_writer.write_to_database(dataframes, db_schema)
    finally:
        spark.stop()


def Reports(db_schema):
    spark = get_spark_session()
    data_reader = DataReader(spark)
    dataframes = data_reader.read_files(DatabaseConfig.get_files_directory())
    # DF referente aos arquivos
    equipment_df = dataframes["equipment"]
    equipment_sensors_df = dataframes["equipment_sensors"]
    equipment_failure_sensors_df = dataframes["equipment_failure_sensors"]
    # Istancia a classe de relatorios
    data_reporter = DataReporter(spark)
    # Chama os Relatórios
    data_reporter.total_failures(equipment_failure_sensors_df, db_schema)
    data_reporter.equipment_most_failures(
        equipment_failure_sensors_df, equipment_sensors_df, equipment_df, db_schema
    )
    data_reporter.average_failures_per_group(
        equipment_failure_sensors_df, equipment_sensors_df, equipment_df, db_schema
    )
    data_reporter.sensor_errors_ranking(
        equipment_failure_sensors_df, equipment_sensors_df, equipment_df, db_schema
    )
    spark.stop()
