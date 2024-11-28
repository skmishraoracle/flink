import json
from typing import Iterable, Tuple

from pyflink.common import Types, Row, WatermarkStrategy
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.table import StreamTableEnvironment


def process_message(message) -> Tuple[int, str, float] | None:
  try:
    value_json = json.loads(message.value)
    timestamp = int(value_json.get("timestamp"))  # Ensure timestamp is an integer
    symbol = value_json.get("symbol")
    price = float(value_json.get("price"))
    return timestamp, symbol, price
  except (json.JSONDecodeError, ValueError, TypeError) as e:
    # Log the error or skip the invalid message
    print(f"Error processing message: {e}, Message: {message}")  # Include message in error log
    return None

class CustomTimestampAssigner(TimestampAssigner):
  def extract_timestamp(self, element, record_timestamp):
    return element[0]

class MovingAverageFunction(ProcessWindowFunction):
  def process(self,
              key: str,
              context: 'ProcessWindowFunction.Context',
              elements: Iterable[Row]) -> Iterable[Row]:
    prices = [row[2] for row in elements]
    avg_price = sum(prices) / len(prices) if prices else 0
    yield Row(key, avg_price)

class StreamingProcessor:
  def __init__(self, kafka_topic, kafka_brokers, consumer_group):
    self.kafka_topic = kafka_topic
    self.kafka_brokers = kafka_brokers
    self.consumer_group = consumer_group
    self.env = StreamExecutionEnvironment.get_execution_environment()
    self.env.set_parallelism(1)
    self.table_env = StreamTableEnvironment.create(self.env)
    self.ds = self.create_datastream()

  def create_kafka_source(self):
    # Helper function to create the Kafka source DDL
    return f"""
        CREATE TABLE kafka_source (
            `key` STRING,
            `value` STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{self.kafka_topic}',
            'properties.bootstrap.servers' = '{self.kafka_brokers}',
            'properties.group.id' = '{self.consumer_group}',
            'format' = 'raw',
            "value.format" = 'json',
            'scan.startup.mode' = 'earliest-offset'
        )
        """

  def create_datastream(self):
    kafka_source_ddl = self.create_kafka_source()
    self.table_env.execute_sql(kafka_source_ddl)
    table = self.table_env.from_path("kafka_source")
    return self.table_env.to_data_stream(table)

  def compute_moving_average(self):
    parsed_stream = self.ds.flat_map(
      lambda message: process_message(message) or [],
      Types.ROW([Types.LONG(), Types.STRING(), Types.FLOAT()])
    )

    # allows events to arrive upto 5 secs late
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Time.seconds(5)).with_timestamp_assigner(
      CustomTimestampAssigner()
    )

    with_timestamps = parsed_stream.assign_timestamps_and_watermarks(watermark_strategy)
    keyed_stream = with_timestamps.key_by(lambda row: row[1])

    moving_avg_stream = keyed_stream.window(
      SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10))
    ).process(
      MovingAverageFunction(),
      Types.ROW([Types.STRING(), Types.FLOAT()])
    )

    moving_avg_stream.print()

  def append_and_process(self):
    # Implement your appending and processing logic here
    processed_stream = self.ds.flat_map(
      lambda message: process_message(message) or [],
      Types.ROW([Types.LONG(), Types.STRING(), Types.FLOAT()])  # Use LONG for timestamp
    )

    # Example: Print the processed messages with timestamps
    processed_stream.print("Processed Messages:")

    self.env.execute("Kafka Streaming Job")


if __name__ == "__main__":
  # Kafka configuration
  _kafka_topic = "your_kafka_topic"
  _kafka_brokers = "your_kafka_brokers"
  _consumer_group = "your_consumer_group"
  processor = StreamingProcessor(
    _kafka_topic,
    _kafka_brokers,
    _consumer_group)
  processor.append_and_process()