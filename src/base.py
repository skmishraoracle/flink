from __future__ import annotations

import math
import traceback
from datetime import datetime
from typing import Any, Optional, List

import pandas as pd
import pyflink
import yfinance as yf
from lxml.xsltext import self_node
from pyflink.common import Types, Time
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, WindowFunction
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

from utils import logger, write_xls


# Define a function to fetch quotes
def fetch_ticks_history(
  tickers: List[str],
  flag: str,
  **kwargs: Any
) -> pd.DataFrame:
  hist_with_date: Optional[pd.DataFrame] = None
  try:
    if flag == "hist":
      start: Optional[str] = kwargs.get("start", None)
      if start is None:
        raise ValueError("start parameter is required...")
      end: Optional[str] = kwargs.get("end", None)
      if end is None:
        raise ValueError("end parameter is required...")
      hist = yf.download(tickers, start, end)
    elif flag == "recent":
      period = kwargs.get("period", None)
      if period is None:
        raise ValueError("recent parameter is required...")
      hist = yf.download(tickers, period=period)
    elif flag == "adj-data":
      start: Optional[str] = kwargs.get("start", None)
      if start is None:
        raise ValueError("start parameter is required...")
      end: Optional[str] = kwargs.get("end", None)
      if end is None:
        raise ValueError("end parameter is required...")
      hist = yf.download(tickers, start, end, auto_adjust=True)
    elif flag == "interval":
      start: Optional[str] = kwargs.get("start", None)
      if start is None:
        raise ValueError("start parameter is required...")
      end: Optional[str] = kwargs.get("end", None)
      if end is None:
        raise ValueError("end parameter is required...")
      interval: Optional[str] = kwargs.get("interval", None)
      if interval is None:
        raise ValueError("interval parameter is required...")
      hist = yf.download(tickers, start, end, interval=interval)
    elif flag == "real-time":
      period: Optional[str] = kwargs.get("period", None)
      if period is None:
        raise ValueError("period parameter is required...")
      hist = yf.download(tickers, period=period)
    else:
      valid_flags = "hist, recent, adj-data, interval, real-time..."
      raise ValueError(f"invalid flag - valid flags are: {valid_flags}")

    hist_with_date = hist.reset_index()
    # Remove timezone information
    hist_with_date['Date'] = hist_with_date['Date'].dt.tz_localize(None)
    return hist_with_date

  except Exception as ex:
    logger.warning(f"Failed to fetch 'hist': {ex}")
    traceback.print_exc()

def ds2pdf_moving_avg(ds: pyflink.datastream) -> pd.DataFrame:
  try:
    results = [
      (
        datetime.fromtimestamp(row[0]/1000),
        datetime.fromtimestamp(row[1]/1000),
        row[2],
        row[3],
        row[4],
        row[5]
      ) for row in ds.execute_and_collect()
    ]
    return pd.DataFrame(
      results,
      columns=[
        'window_start',
        'window_end',
        'moving_avg_open',
        'moving_avg_high',
        'moving_avg_low',
        'moving_avg_close'
      ]
    )

  except Exception as ex:
    logger.warning(f"Error converting DataStream to pandas DataFrame: {ex}")
    traceback.print_exc()

def moving_average(
  ds: pyflink.datastream,
  window_size: int,
  slide_interval: int
) -> pyflink.datastream:
  try:
    # Define a custom WindowFunction for calculating the moving average
    class MovingAverageFunction(WindowFunction):
      def apply(self, key, window, inputs):
        open_prices = [x[2] for x in inputs]
        high_prices = [x[3] for x in inputs]
        low_prices = [x[4] for x in inputs]
        close_prices = [x[5] for x in inputs]
        return [
          (
            window.start,
            window.end,
            sum(open_prices) / len(open_prices),
            sum(high_prices) / len(high_prices),
            sum(low_prices) / len(low_prices),
            sum(close_prices) / len(close_prices)
          )
        ]

    # Apply the sliding window and calculate the moving average
    result_ds = (
      ds.key_by(lambda x: 1).window(SlidingEventTimeWindows.of(
          size=Time.milliseconds(window_size * 1000),
          slide=Time.milliseconds(slide_interval * 1000)
        )
      ).apply(MovingAverageFunction())
    )

    return result_ds

  except Exception as ex:
    logger.warning(f"Error computing moving average: {ex}")
    traceback.print_exc()

def ds2pdf_conf_interval(ds: pyflink.datastream) -> pd.DataFrame:
  try:
    # Collect the results from the DataStream
    results = [
      (
        datetime.fromtimestamp(row[0]/1000),
        datetime.fromtimestamp(row[1]/1000),
        row[2],
        row[3],
        row[4],
      ) for row in ds.execute_and_collect()
    ]

    return pd.DataFrame(
      results,
      columns=[
        'window_start',
        'window_end',
        'conf_interval_open',
        'conf_interval_high',
        'conf_interval_low',
      ]
    )

  except Exception as ex:
    logger.warning(f"Error converting DataStream to pandas DataFrame: {ex}")
    traceback.print_exc()

def confidence_interval(
  ds: pyflink.datastream,
  window_size: int,
  slide_interval: int,
  confidence_level=0.95
) -> pyflink.datastream:
  try:
    # Define a custom WindowFunction for calculating the confidence interval
    class ConfidenceIntervalFunction(WindowFunction):
      def apply(self, key, window, inputs):
        close_prices = [x[5] for x in inputs]
        n = len(close_prices)

        if n > 1:
          # Calculate mean
          mean = sum(close_prices) / n

          # Calculate standard deviation
          variance = sum((x - mean) ** 2 for x in close_prices) / (n - 1)
          stddev = math.sqrt(variance)

          # Calculate confidence interval (using z-score for 95% confidence level)
          z_score = 1.96  # 1.96 for 95% confidence level
          confidence_margin = z_score * (stddev / math.sqrt(n))
          lower_bound = mean - confidence_margin
          upper_bound = mean + confidence_margin

          return [(window.start, window.end, mean, lower_bound, upper_bound)]
        else:
          # Handle case with 1 or no values (cannot compute variance)
          return [(window.start, window.end, close_prices[0], close_prices[0], close_prices[0])]

    # Apply the sliding window and calculate the confidence interval
    result_ds = (
      ds.key_by(lambda x: 1).window(SlidingEventTimeWindows.of(
        size=Time.milliseconds(window_size * 1000),  # Use Time.milliseconds()
        slide=Time.milliseconds(slide_interval * 1000)  # Use Time.milliseconds()
      )).apply(ConfidenceIntervalFunction())
    )

    return result_ds

  except Exception as ex:
    logger.warning(f"Error computing confidence interval: {ex}")
    traceback.print_exc()


class TickHistory:
  def __init__(self):
    try:
      # Create Flink environment
      self.env = StreamExecutionEnvironment.get_execution_environment()
      self.env.set_parallelism(1)

      # Use streaming mode in a local environment
      env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
      self.t_env = StreamTableEnvironment.create(
        self.env,
        environment_settings=env_settings
      )
    except Exception as ex:
      logger.warning(f"Error in init: {ex}")
      traceback.print_exc()

  def append2ds(
    self,
    ds: pyflink.datastream,
    df: pd.DataFrame
  ) -> pyflink.datastream:
    try:
      new_ds = self.pdf2ds(df)
      return ds.union(new_ds)
    except Exception as ex:
      logger.warning(f"Error in init: {ex}")
      traceback.print_exc()

  def pdf2ds(self, df: pd.DataFrame) -> pyflink.datastream:
    try:
      # Mapping of Pandas data types to PyFlink Types
      type_mapping = {
        'datetime64[ns]': Types.SQL_TIMESTAMP(),
        'int64': Types.LONG(),
        'float64': Types.DOUBLE(),
        'object': Types.STRING()
      }

      # Get PyFlink types for each column
      field_names = list(df.columns)
      flink_types = [type_mapping.get(
        str(df[col].dtype),
        Types.STRING()
      ) for col in df.columns]

      # Create a DataStream from the Pandas DataFrame
      ds = self.env.from_collection(
        df.values.tolist(),
        type_info=Types.ROW_NAMED(
          field_names,
          flink_types
        )
      )

      # Assign timestamps and watermarks to the DataStream
      class MyTimestampAssigner(TimestampAssigner):
        def extract_timestamp(self, value, record_timestamp):
          # Assuming value[0] is in seconds or milliseconds
          if isinstance(value[1], float) or isinstance(value[1], int):
            return int(value[1] * 1000)  # Convert to milliseconds
          elif isinstance(value[1], datetime):
            return int(value[1].timestamp() * 1000)
          else:
            raise ValueError(f"Unexpected value type for timestamp: {type(value[1])}")

      # Assign timestamps and watermarks to the DataStream
      ds = ds.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(MyTimestampAssigner())
      )

      return ds
    except Exception as ex:
      logger.warning(f"Error converting pandas DataFrame to Flink DataStream: {ex}")
      traceback.print_exc()


if __name__ == "__main__":
  # _tickers = ["AMD"]
  _tickers = ["AMD", "BA", "BABA", "GOOG", "META", "PYPL", "SHOP", "SPY"]
  _tick_history = TickHistory()
  _hist_with_date: Optional[pd.DataFrame] = None
  # Fetch historical data from Yahoo Finance
  #_flags = ["hist", "recent", "adj-data", "interval", "real-time"]
  _flags = ["hist"]
  _today = datetime.today().strftime("%Y-%m-%d")

  for _flag in _flags:
    if _flag in ["hist", "adj-data"]:
      _hist_with_date = fetch_ticks_history(
        tickers=_tickers,
        flag=_flag,
        start="2024-01-01",
        end=_today
      )
    elif _flag in ["recent"]:
      _hist_with_date = fetch_ticks_history(
        tickers=_tickers,
        flag="recent",
        period="1mo"
      )
    elif _flag in ["interval"]:
      _hist_with_date = fetch_ticks_history(
        tickers=_tickers,
        flag="interval",
        start="2024-01-01",
        end="2024-11-21",
        interval="1wk"
      )
    elif _flag in ["real-time"]:
      _hist_with_date = fetch_ticks_history(
        tickers=_tickers,
        flag="real-time",
        period="1d"
      )

    _flink_ds: Optional[pyflink.datastream] = None

    if _hist_with_date is not None:
      for _ticker in _tickers:
        level_0_cols = _hist_with_date.columns.get_level_values(0).unique().tolist()
        cols_to_select = [('Date', '')] + [(col, _ticker) for col in level_0_cols if col != 'Date' ]
        _tick_hist_with_date = _hist_with_date[cols_to_select]
        _tick_hist_with_date = _tick_hist_with_date.reset_index()
        # Flatten multi-level column names
        _tick_hist_with_date.columns = _tick_hist_with_date.columns.get_level_values(0)

        # Convert to Flink datastream
        try:
          _flink_ds = _tick_history.pdf2ds(_tick_hist_with_date)
        except Exception as ex:
          logger.warning(f"Failed to fetch 'hist': {ex}")
          traceback.print_exc()

        # Compute moving average over a 7-day window sliding by 1 day
        _window_size = 7 * 24 * 60 * 60
        _slide_interval = 24 * 60 * 60
        _moving_average_ds = moving_average(
          _flink_ds,
          window_size=_window_size,
          slide_interval=_slide_interval
        )

        # Convert the result back to a Pandas DataFrame and print it
        _moving_avg_df = ds2pdf_moving_avg(_moving_average_ds)
        write_xls(
          _moving_avg_df,
          f"moving_avg"
        )

        _conf_interval_ds = confidence_interval(
          _flink_ds,
          _window_size,
          _slide_interval,
        )
        _conf_interval_df = ds2pdf_conf_interval(_conf_interval_ds)
        write_xls(
          _conf_interval_df,
          f"conf_interval"
        )