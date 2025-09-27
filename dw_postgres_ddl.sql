-- ==============================================
--  Data Warehouse para Atención Médica Domiciliaria
--  Esquema DW (PostgreSQL)
--  Autor: ChatGPT
-- ==============================================

CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.cargas_log (
  id            BIGSERIAL PRIMARY KEY,
  proceso       TEXT NOT NULL,
  detalle       TEXT,
  filas         BIGINT,
  estado        TEXT NOT NULL DEFAULT 'OK',
  started_at    TIMESTAMP NOT NULL DEFAULT now(),
  finished_at   TIMESTAMP
);

-- Dimensión de fechas (Conforme)
CREATE TABLE IF NOT EXISTS dw.dim_fecha (
  fecha_solicitud_id DATE PRIMARY KEY,
  ano INT NOT NULL,
  mes INT NOT NULL,
  dia INT NOT NULL,
  trimestre INT GENERATED ALWAYS AS ((EXTRACT(QUARTER FROM fecha_solicitud_id))::INT) STORED,
  dia_semana INT GENERATED ALWAYS AS (((EXTRACT(DOW FROM fecha_solicitud_id)+1))::INT) STORED,
  nombre_mes TEXT GENERATED ALWAYS AS (TO_CHAR(fecha_solicitud_id, 'TMMonth')) STORED,
  nombre_dia TEXT GENERATED ALWAYS AS (TO_CHAR(fecha_solicitud_id, 'TMDay')) STORED
);

-- Dim Aseguradora (Conforme)
CREATE TABLE IF NOT EXISTS dw.dim_aseguradora (
  aseguradora_id BIGSERIAL PRIMARY KEY,
  aseguradora_nk TEXT UNIQUE NOT NULL,  -- nombre normalizado
  aseguradora    TEXT NOT NULL
);

-- Dim Paciente (SCD2)
CREATE TABLE IF NOT EXISTS dw.dim_paciente (
  paciente_id    BIGSERIAL PRIMARY KEY,
  paciente_nk    TEXT NOT NULL,     -- documento o id operativo
  nombre         TEXT,
  municipio      TEXT,
  estado         TEXT,              -- departamento
  aseguradora    TEXT,              -- valor actual declarado
  zona           TEXT,
  fecha_ingreso  DATE,
  vigente_desde  TIMESTAMP NOT NULL DEFAULT now(),
  vigente_hasta  TIMESTAMP,
  es_actual      BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS ix_dim_paciente_nk ON dw.dim_paciente(paciente_nk);
CREATE INDEX IF NOT EXISTS ix_dim_paciente_actual ON dw.dim_paciente(paciente_nk, es_actual);

-- Dim Equipo (SCD2)
CREATE TABLE IF NOT EXISTS dw.dim_equipo (
  equipo_id      BIGSERIAL PRIMARY KEY,
  equipo_nk      TEXT UNIQUE NOT NULL,  -- serial/código equipo
  equipo         TEXT,
  estado_equipo  TEXT,
  vigente_desde  TIMESTAMP NOT NULL DEFAULT now(),
  vigente_hasta  TIMESTAMP,
  es_actual      BOOLEAN NOT NULL DEFAULT TRUE
);

-- Dim Medicamento (Conforme)
CREATE TABLE IF NOT EXISTS dw.dim_medicamento (
  codigo_medicamento BIGSERIAL PRIMARY KEY,
  medicamento_nk     TEXT UNIQUE NOT NULL, -- código del maestro
  nombre             TEXT,
  forma_farmaceutica TEXT,
  via_administracion TEXT
);

-- Dim Pedido (según diseño actual)
CREATE TABLE IF NOT EXISTS dw.dim_pedido (
  numero_pedido_id BIGSERIAL PRIMARY KEY,
  numero_pedido_nk TEXT UNIQUE NOT NULL,
  insumo_solicitado TEXT,
  cantidad          NUMERIC
);

-- Hecho Equipos
CREATE TABLE IF NOT EXISTS dw.hecho_equipos (
  solicitud_equipos_id BIGSERIAL PRIMARY KEY,
  equipo_id            BIGINT NOT NULL REFERENCES dw.dim_equipo(equipo_id),
  paciente_id          BIGINT NOT NULL REFERENCES dw.dim_paciente(paciente_id),
  aseguradora_id       BIGINT NOT NULL REFERENCES dw.dim_aseguradora(aseguradora_id),
  fecha_solicitud_id   DATE   NOT NULL REFERENCES dw.dim_fecha(fecha_solicitud_id)
);
CREATE INDEX IF NOT EXISTS ix_heq_fks ON dw.hecho_equipos(equipo_id, paciente_id, aseguradora_id, fecha_solicitud_id);

-- Hecho Solicitud Servicios
CREATE TABLE IF NOT EXISTS dw.hecho_solicitud_servicios (
  servicio_id        BIGSERIAL PRIMARY KEY,
  numero_pedido_id   BIGINT NOT NULL REFERENCES dw.dim_pedido(numero_pedido_id),
  paciente_id        BIGINT NOT NULL REFERENCES dw.dim_paciente(paciente_id),
  aseguradora_id     BIGINT NOT NULL REFERENCES dw.dim_aseguradora(aseguradora_id),
  fecha_solicitud_id DATE   NOT NULL REFERENCES dw.dim_fecha(fecha_solicitud_id),
  codigo_medicamento BIGINT NOT NULL REFERENCES dw.dim_medicamento(codigo_medicamento)
);
CREATE INDEX IF NOT EXISTS ix_hss_fks ON dw.hecho_solicitud_servicios(numero_pedido_id, paciente_id, aseguradora_id, fecha_solicitud_id, codigo_medicamento);

-- Vista útil
CREATE OR REPLACE VIEW dw.v_dim_paciente_actual AS
SELECT * FROM dw.dim_paciente WHERE es_actual = TRUE;

-- Función de log
CREATE OR REPLACE FUNCTION dw.log_fin(proceso TEXT, detalle TEXT, filas BIGINT, estado TEXT)
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO dw.cargas_log(proceso, detalle, filas, estado, finished_at)
  VALUES (proceso, detalle, filas, COALESCE(estado,'OK'), now());
END;
$$;
