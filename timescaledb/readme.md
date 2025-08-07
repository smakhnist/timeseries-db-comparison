```
docker run -d \
--name timescaledb \
-p 5442:5432 \
-e POSTGRES_PASSWORD=secretpassword \
-e POSTGRES_USER=tsdbuser \
-e POSTGRES_DB=tsdb \
timescale/timescaledb:latest-pg15
```