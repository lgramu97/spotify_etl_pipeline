import sys
import os

sys.path.insert(0,os.path.abspath(os.getcwd()))

from dags.commons.cfg import CLIENT_ID, CLIENT_SECRET, SPOTIPY_REDIRECT_URI,DB_CONNSTR, SCOPE, AUTHORIZE, URL_TOKEN, CURRENT_UESR_RECENTLY_PLAYED
from datetime import datetime, timedelta
from dags.commons.models import TABLENAME
import pandas as pd
from sqlalchemy import create_engine
import requests.api
import json
import base64
import webbrowser
from urllib.parse import urlencode

#Complete here with your callback code.
CODE = ''

def request_acces_token():
    auth_headers = {
        "client_id": CLIENT_ID,
        "response_type": "code",
        "redirect_uri": SPOTIPY_REDIRECT_URI,
        "scope": SCOPE,
        "show_dialog":True
    }
    webbrowser.open("https://accounts.spotify.com/authorize?" + urlencode(auth_headers))
    
def request_token_spotify():
    headers,data = {}, {}
   # Encode as Base64
    message = "{clientId}:{clientSecret}".format(clientId= CLIENT_ID,clientSecret=CLIENT_SECRET)
    messageBytes = message.encode('ascii')
    base64Bytes = base64.b64encode(messageBytes)
    base64Message = base64Bytes.decode('ascii')

    headers['Authorization'] = f"Basic {base64Message}"
    headers['Content-Type'] = "application/x-www-form-urlencoded"
    
    data['grant_type'] = "client_credentials"
    data['code'] = CODE
    data['redirect_uri'] = SPOTIPY_REDIRECT_URI
    
    r = requests.post(URL_TOKEN, headers=headers, data=data)
    return r.json()['access_token']


def extract(date: datetime, limit=50):
    """Get limit elements from last listen tracks

    Args:
        date (datetime): Date to query
        limit (int, optional): Limit of element to query. Defaults to 50.
    """
    ds = int(date.timestamp()) * 1000 #UNIX FORMAT.
    data = {}
    token = request_token_spotify()

    headers = {
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=token)
    }
    
    data['after'] = str(ds)
    data['limit'] = str(limit)
    
    res = requests.get(CURRENT_UESR_RECENTLY_PLAYED, headers = headers, params=data)
    return res.json()


def transform(raw_data, date):
    """Transform raw_data to pandas dataframe with songs information.

    Args:
        raw_data (dictionary): spotify data
        date (datetime): date listening to music

    Raises:
        Exception: Corrupted information (2 song at the same time)
        Exception: Corrupted information (missed information)

    Returns:
        dataframe with songs information.
    """
    data = []
    #Iterate items = songs
    for r in raw_data["items"]:
        data.append(
            {
                "played_at": r["played_at"],
                "artist": r["track"]["artists"][0]["name"],
                "track": r["track"]["name"]
            }
        )
    #r['track] = {album,artist,avaible_markets,.....}
    #Create dataframe pandas with dict.
    df = pd.DataFrame(data)
    # Remove dates different from what we want
    clean_df = df[pd.to_datetime(df["played_at"]).dt.date == date.date()]

    # Data validation FTW
    if not df["played_at"].is_unique:
        raise Exception("A value from played_at is not unique")

    if df.isnull().values.any():
        raise Exception("A value in df is null")

    return clean_df
    
def load(df: pd.DataFrame):
    print(f'Uploading {df.shape[0]} to postgres')
    engine = create_engine(DB_CONNSTR)
    df.to_sql(TABLENAME,con=engine,index=False, if_exists='append')
    
def run_etl(execution_date):
    """Execute pipeline ETL.

    Args:
        execution_date (DateTime?): Airflow MACRO date.
    """
    print(type(execution_date))
    # Extract
    data_raw = extract(date)
    print(f"Extracted {len(data_raw['items'])} registers")

    ## Transform
    clean_df = transform(data_raw, date)
    print(f"{clean_df.shape[0]} registers after transform")

    ## Load
    load(clean_df)


if __name__ == '__main__':
    #datetime.today() 2022-07-06 12:37:00.796589 1 day
    date = datetime.today() - timedelta(days=5)
    #date = 5 days ago. (Year-Month-Day Hour:Minutes:Sec.Mils)
    
    #uncoment line below: Execute request_acces_token and copy the url token. Replace it in TOKEN=''.
    #request_acces_token()
    
    #Now you cant execute etl process.
    run_etl(date)