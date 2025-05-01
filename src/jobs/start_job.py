from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# Create the source
def create_events_source_kafka(t_name, t_env):
    source_ddl = f"""
        CREATE TABLE {t_name} (
            event_number INTEGER,
            event_unix_time BIGINT,
            event_watermark AS TO_TIMESTAMP_LTZ(event_unix_time, 3),
            WATERMARK FOR event_watermark AS event_watermark - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'dummy_topic',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)

# Create the sink
def create_processed_events_sink_postgres(t_name, t_env):
    sink_ddl = f"""
        CREATE TABLE {t_name} (
            event_number INTEGER,
            event_timestamp TIMESTAMP
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{t_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)

# Create process
def log_processing():
    # Define the apache flink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Define the apache flink table environment
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)
    
    # Execute job
    try:
        kafka_source_table_name = 'events'
        create_events_source_kafka(kafka_source_table_name, t_env)
        
        postgres_sink_table_name = 'processed_events'
        create_processed_events_sink_postgres(postgres_sink_table_name, t_env)
        
        t_env.execute_sql(
            f"""
                INSERT INTO {postgres_sink_table_name}
                SELECT event_number, TO_TIMESTAMP_LTZ(event_unix_time, 3) AS event_timestamp
                FROM {kafka_source_table_name};
            """
        ).wait()
    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))

if __name__ == "__main__":
    log_processing()

