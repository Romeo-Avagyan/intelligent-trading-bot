import logging
from datetime import datetime
import os

import click
import pandas as pd
from binance.client import Client
from binance.enums import *

from common.utils import binance_freq_from_pandas, klines_to_df
from service.App import *

"""
The script is intended for retrieving data from binance server: klines, server info etc.

Links:
https://sammchardy.github.io/binance/2018/01/08/historical-data-download-binance.html
https://sammchardy.github.io/kucoin/2018/01/14/historical-data-download-kucoin.html
"""

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('download_binance')

#
# Historic data
#

@click.command()
@click.option('--config_file', '-c', type=click.Path(), default='', help='Configuration file name')
def main(config_file):
    """
    This script retrieves historical kline (candlestick) data from the Binance server,
    processes it, and saves it to CSV files.
    It uses the binance.client.Client to interact with the Binance API. (Client.get_historical_klines)
    It utilizes the following configurations:
        - time_column
        - data_folder
        - freq
        - api_key
        - api_secret
        - data_source
    The script appends new data to file if file already exists.
    """
    load_config(config_file)

    # Configure Logger
    logs_folder = App.config.get('logging', {}).get('path', './logs')
    os.makedirs(logs_folder, exist_ok = True)

    logs_path = os.path.join(logs_folder, "download_binance.log")
    logger.addHandler(logging.FileHandler(logs_path))
    
    # Load necessary configurations
    logger.info("Loading configurations")

    time_column = App.config["time_column"]
    data_path = Path(App.config["data_folder"])
    now = datetime.now()
    pandas_freq = App.config["freq"]
    binance_freq = binance_freq_from_pandas(pandas_freq)

    App.client = Client(
        api_key=App.config["api_key"],
        api_secret=App.config["api_secret"]
    )

    futures = False

    data_sources = App.config["data_sources"]

    logger.info(f"""Starting Download with the following configurations:
        Pandas frequency: {pandas_freq}
        Binance frequency: {binance_freq}
        Data path: {data_path}
        Futures: {futures}""")

    # Download data
    for ds in data_sources:
        # Assumption: folder name is equal to the symbol name we want to download
        quote = ds.get("folder")
        if not quote:
            logger.error(f"ERROR. Folder is not specified., skipping Data Source")
            continue

        data_source_type = HistoricalKlinesType.SPOT
        config_data_source_type = ds.get("type", "spot")
        if config_data_source_type == 'futures':
            data_source_type = HistoricalKlinesType.FUTURES

        logger.info(f"Start downloading '{quote}' ({config_data_source_type}) ...")

        file_path = data_path / quote
        file_path.mkdir(parents=True, exist_ok=True)  # Ensure that folder exists

        file_name = ds.get("file").with_suffix(".csv")

        # Get a few latest klines to determine the latest available timestamp
        latest_klines = App.client.get_klines(symbol=quote, interval=binance_freq, limit=5)
        latest_ts = pd.to_datetime(latest_klines[-1][0], unit='ms')

        if not file_name.is_file():
            # No existing data so we will download all available data and store as a new file
            df = pd.DataFrame()

            oldest_point = datetime(2017, 1, 1)

            logger.info(f"File not found. All data will be downloaded and stored in newly created file for {quote} and {binance_freq}.")
        else:            
            # Load the existing data in order to append newly downloaded data
            df = pd.read_csv(file_name)
            df[time_column] = pd.to_datetime(df[time_column], format='ISO8601')

            # oldest_point = parser.parse(data["timestamp"].iloc[-1])
            oldest_point = df["timestamp"].iloc[-5]  # Use an older point so that new data will overwrite old data

            logger.info(f"File found. Downloaded data for {quote} and {binance_freq} since {str(latest_ts)} will be appended to the existing file {file_name}")

        # === Download from the remote server using binance client
        get_binance_historical_klines(
            symbol=quote,
            interval=binance_freq,
            start_str=oldest_point.isoformat(),
            klines_type= data_source_type,
            df=df,
            file_name=file_name
        )

    elapsed = datetime.now() - now
    logger.info(f"Finished downloading data in {str(elapsed).split('.')[0]}")

    return df


def get_binance_historical_klines(symbol, interval, start_str, klines_type, df, file_name):
    """
    This functions iteratevly fetches historic data and adds it to the file.
    The reason is Binance limitations.
    """

    now = datetime.now()
    start_point_dt = datetime.fromisoformat(start_str)

    # Define a timedelta of 1 year, to download year by year
    one_year = timedelta(days=365)

    more_data_to_fetch = True

    while (more_data_to_fetch):
        difference = now - start_point_dt

        end_dt = None
        if difference > one_year:
            end_dt = start_point_dt + one_year
        else:
            more_data_to_fetch = False

        logger.info(
            f"Fetching data since '{start_point_dt.isoformat()}' until '{end_dt.isoformat() if end_dt else 'now'}'"
        )

        klines = App.client.get_historical_klines(
            symbol=symbol,
            interval=interval,
            start_str=start_str,
            end_str = end_dt.isoformat() if end_dt else None,
            klines_type=klines_type
        )
        
        df = klines_to_df(klines, df)
        logger.info(f'Overall {df.size} rows in the Data Frame')

        if (more_data_to_fetch):
            start_point_dt = end_dt - timedelta(days=1)

    # Remove last row because it represents a non-complete kline (the interval not finished yet)
    df = df.iloc[:-1]

    df.to_csv(file_name)

    logger.info(f"Finished downloading '{symbol}'. Stored in '{file_name}'")


if __name__ == '__main__':
    main()
