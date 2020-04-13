CREATE DATABASE sparkify;
CREATE USER sparkify WITH  PASSWORD 'sparkify' SUPERUSER CREATEDB CREATEROLE INHERIT LOGIN REPLICATION;
CREATE USER user WITH PASSWORD 'user';
GRANT sparkify TO user;