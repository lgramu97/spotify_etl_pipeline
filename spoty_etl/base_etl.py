
from argon2 import extract_parameters
from cfg import CLIENT_ID, CLIENT_SECRET, SPOTIPY_REDIRECT_URI, DB_CONNSTR
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime, timedelta
from models import TABLENAME
import pandas as pd
from sqlalchemy import create_engine

scope = "user-read-recently-played" #Los datos de la api que voy a traer.

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    redirect_uri=SPOTIPY_REDIRECT_URI,
    scope=scope
))

def extract(date: datetime, limit=50):
    """Get limit elements from last listen tracks

    Args:
        date (datetime): Date to query
        limit (int, optional): Limit of element to query. Defaults to 50.
    """
    ds = int(date.timestamp()) * 1000 #UNIX FORMAT.
    return sp.current_user_recently_played(limit=limit, after=ds)


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
    
    
if __name__ == '__main__':
    #datetime.today() 2022-07-06 12:37:00.796589 1 day
    date = datetime.today() - timedelta(days=5)
    #date = 5 days ago. (Year-Month-Day Hour:Minutes:Sec.Mils)

    # Extract
    data_raw = extract(date)
    print(f"Extracted {len(data_raw['items'])} registers")

    ## Transform
    clean_df = transform(data_raw, date)
    print(f"{clean_df.shape[0]} registers after transform")

    ## Load
    load(clean_df)
    print("Done")