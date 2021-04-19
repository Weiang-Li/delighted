# pip install delighted
import delighted
import os
import pandas as pd
from datetime import datetime
from sqlalchemy import *
from dotenv import load_dotenv
from pathlib import Path

env_path = Path("..") / "credentials.env"
load_dotenv(dotenv_path=env_path)
user = os.environ.get('user')
host = os.environ.get('host')
password = os.environ.get('password')
database = os.getenv("database")
delighted.api_key = os.environ.get("delighted_api_key")


def fix_df(df):
    df = df.rename(str.lower, axis="columns")
    df.columns = df.columns.str.replace("/", "_")
    df.columns = df.columns.str.strip().str.replace("\s+", "_")
    df.columns = df.columns.str.replace("[()]", "_")
    df.columns = df.columns.str.replace(".", "")
    df["last_run"] = str(datetime.now().strftime("%Y-%m-%d %I:%M:%S %p"))
    return df


# generate dates
dates = pd.date_range("01-01-2020", "01-10-2020")
df = pd.DataFrame()
for date in dates:
    fromdate = datetime(date.year, date.month, date.day, 0, 0, 0)
    todate = datetime(date.year, date.month, date.day, 23, 59, 59)
    # get sms data for each date
    survey_metric = delighted.Metrics.retrieve(since=fromdate,
                                               until=todate, groups=['sms'])
    survey_metric['date'] = date
    # concat each date's dataframe into one
    df = pd.concat([pd.DataFrame(data={k: [v] for k, v in survey_metric.items()}), df])

engine = create_engine(
    f'{database}://{user}:{password}@{host}/postgres')  # databasename://username:password@host/databasename
con = engine.connect()
# insert into pg admin
fix_df(df).to_sql('delighted_metrics', con, if_exists='replace', index=bool)
