-- Schema do Cassandra: leituras dos sensores como séries temporais.
--
-- No Cassandra a modelagem parte das CONSULTAS, não das entidades. Cada tabela
-- atende a uma pergunta, e o mesmo dado é gravado em mais de uma tabela
-- (desnormalização) para que cada consulta leia uma única partição.
--
-- O "dia" entra na chave de partição para que nenhuma partição cresça sem
-- limite: um sensor a 1 leitura/s gera 86.400 linhas por dia por partição.

CREATE KEYSPACE IF NOT EXISTS iot
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Pergunta: "quais as leituras do sensor X no dia Y, das mais recentes para as mais antigas?"
CREATE TABLE IF NOT EXISTS iot.leituras_por_sensor (
    sensor_id  text,
    dia        date,
    ts         timestamp,
    valor      double,
    unidade    text,
    status     text,
    tipo       text,
    maquina_id text,
    linha_id   text,
    evento_id  text,
    PRIMARY KEY ((sensor_id, dia), ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND default_time_to_live = 2592000
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'HOURS',
                    'compaction_window_size': 6};

-- Pergunta: "o que todos os sensores da máquina X mediram no dia Y?"
CREATE TABLE IF NOT EXISTS iot.leituras_por_maquina (
    maquina_id text,
    dia        date,
    ts         timestamp,
    sensor_id  text,
    tipo       text,
    valor      double,
    unidade    text,
    status     text,
    PRIMARY KEY ((maquina_id, dia), ts, sensor_id)
) WITH CLUSTERING ORDER BY (ts DESC, sensor_id ASC)
  AND default_time_to_live = 2592000
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'HOURS',
                    'compaction_window_size': 6};

-- Pergunta: "quais leituras saíram do normal (warning/error) na linha X no dia Y?"
CREATE TABLE IF NOT EXISTS iot.leituras_fora_do_normal_por_linha (
    linha_id   text,
    dia        date,
    ts         timestamp,
    sensor_id  text,
    maquina_id text,
    tipo       text,
    valor      double,
    status     text,
    PRIMARY KEY ((linha_id, dia), ts, sensor_id)
) WITH CLUSTERING ORDER BY (ts DESC, sensor_id ASC)
  AND default_time_to_live = 7776000;
