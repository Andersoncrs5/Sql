docker exec -it postgres psql -U postgres


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
SELECT COUNT(c.id) FROM cdc_schema.clients AS c;

INSERT INTO cdc_schema.clients (id, nome, email) VALUES (9999999, 'Pochita', 'pochita@exemplo.com');
INSERT INTO cdc_schema.clients (nome, email) VALUES ('reze', 'reze@exemplo.com');
INSERT INTO cdc_schema.clients (nome, email) VALUES 
('Denji', 'denji_chainsaw@devilhunter.com'),
('Makima', 'makima_control@publicsafety.gov'),
('Power', 'power_blood_fiend@devilhunter.com'),
('Aki Hayakawa', 'aki_fox_devil@publicsafety.gov'),
('Kobeni Higashiyama', 'kobeni_scared@devilhunter.com'),
('Himeno', 'himeno_ghost@devilhunter.com'),
('Kishibe', 'kishibe_strongest@master.com');

UPDATE cdc_schema.clients
SET nome = 'Pochicta the chainsaw demon'
WHERE id = 9999999;


delete from cdc_schema.clients WHERE id = 9999999;