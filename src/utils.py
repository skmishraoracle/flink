import logging
import os
import traceback

import pandas as pd
from pyflink import datastream
from datetime import datetime

logger = logging.getLogger(__name__)
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
  level=getattr(logging, log_level),
  format="%(asctime)s - %(levelname)s - %(message)s",
)

def print_ds(ds: datastream):
  results = [
    (
      datetime.fromtimestamp(row[0]),
      datetime.fromtimestamp(row[1]),
      row[2],
      row[3],
      row[4],
      row[5]
    ) for row in ds.execute_and_collect()
  ]
  print(results)

def print_ds0(ds: datastream):
  try:
    for element in ds.execute_and_collect():
      if isinstance(element, bytearray):
        element = element.decode('utf-8')  # Decode as string
      elif isinstance(element, (tuple, list)):
        element = tuple(element)  # Handle tuple/list elements
      elif isinstance(element, dict):
        # Process dict as necessary
        pass
      print(element)
  except Exception as e:
    print(f"Error processing DataStream: {e}")

def write_xls(df_in: pd.DataFrame, filename: str) -> None:
  try:
    if not filename.endswith(('.xls', '.xlsx')):  # Add extension if missing
      filename += '.xlsx'  # Default to .xlsx

    if os.path.exists(filename):
      existing_df = pd.read_excel(filename)
      df = pd.concat(
        [existing_df, df_in],
        ignore_index=True
      )
    else:
      df = df_in

    # Write the updated dataframe to the file
    df.to_excel(filename, index=False)

  except Exception as ex:
    logger.error(f"Error in _prompt_stats: {ex}")
    traceback.print_exc()