docker exec -it psql -U postgres


CREATE USER debezium;
ALTER USER debezium WITH PASSWORD '12345678';
ALTER ROLE debezium REPLICATION LOGIN;

CREATE SCHEMA IF NOT EXISTS cdc_schema;



CREATE ROLE replication_group;
GRANT replication_group TO postgres;
GRANT replication_group TO debezium;


CREATE TABLE cdc_schema.clients (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    data_criacao TIMESTAMP DEFAULT NOW()
);

ALTER TABLE cdc_schema.clients OWNER TO replication_group;
GRANT USAGE ON SCHEMA cdc_schema TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA cdc_schema TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA cdc_schema GRANT SELECT ON TABLES TO debezium;
CREATE PUBLICATION dbz_publication FOR TABLE cdc_schema.clients;

//

INSERT INTO cdc_schema.clients (nome, email) VALUES ('Pochita', 'pochita@exemplo.com');