from decouple import RepositoryIni, Config


config = Config(RepositoryIni("settings.ini"))

#Spotify API
CLIENT_ID = config("CLIENT_ID")
CLIENT_SECRET = config("CLIENT_SECRET")
SPOTIPY_REDIRECT_URI = config("SPOTIPY_REDIRECT_URI")
#Database PostgreSQL
DB_CONNSTR = config("DB_CONNSTR")