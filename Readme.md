## Run DOCKER postgres.

Create container and volume.

*docker run -d --name spoty_etl_pg -v my_dbdata:/var/lib/postgresql/data -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DB=spotipy postgres*

Execute command on container

*docker exec -it spoty_etl_pg psql -h localhost -U postgres -W spotipy*
