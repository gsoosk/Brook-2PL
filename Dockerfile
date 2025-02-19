FROM postgres:16-alpine

# Set environment variables
ENV POSTGRES_USER username
ENV POSTGRES_PASSWORD password
ENV POSTGRES_DB postgres
ENV PGDATA /data/postgres

COPY init.sql /docker-entrypoint-initdb.d

# Start PostgreSQL in the background, execute init.sql, then dump the database to a file
RUN set -eux; \
    timeout 180s bash docker-entrypoint.sh postgres & \
    sleep 220; \
