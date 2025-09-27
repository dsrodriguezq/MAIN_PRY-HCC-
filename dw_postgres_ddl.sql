-- Esquemas
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dw;

-- Log simple de cargas
CREATE TABLE IF NOT EXISTS dw.cargas_log (
  id            BIGSERIAL PRIMARY KEY,
  proceso       TEXT NOT NULL,
  detalle       TEXT,
  filas         BIGINT,
  estado        TEXT NOT NULL DEFAULT 'OK',
  started_at    TIMESTAMP NOT NULL DEFAULT now(),
  finished_at   TIMESTAMP
);

-- ========== dim_fecha SIN columnas generadas ==========
DROP TABLE IF EXISTS dw.dim_fecha CASCADE;
CREATE TABLE dw.dim_fecha (
  fecha_solicitud_id DATE PRIMARY KEY,
  ano INT NOT NULL,
  mes INT NOT NULL,
  dia INT NOT NULL,
  trimestre INT,
  dia_semana INT,    -- 1=Lunes … 7=Domingo
  nombre_mes TEXT,
  nombre_dia TEXT
);

-- Dimensiones conformes
CREATE TABLE IF NOT EXISTS dw.dim_aseguradora (
  aseguradora_id BIGSERIAL PRIMARY KEY,
  aseguradora_nk TEXT UNIQUE NOT NULL,
  aseguradora    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dw.dim_paciente (
  paciente_id    BIGSERIAL PRIMARY KEY,
  paciente_nk    TEXT NOT NULL,
  nombre         TEXT,
  municipio      TEXT,
  estado         TEXT,
  aseguradora    TEXT,
  zona           TEXT,
  fecha_ingreso  DATE,
  vigente_desde  TIMESTAMP NOT NULL DEFAULT now(),
  vigente_hasta  TIMESTAMP,
  es_actual      BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS ix_dim_paciente_nk ON dw.dim_paciente(paciente_nk);
CREATE INDEX IF NOT EXISTS ix_dim_paciente_actual ON dw.dim_paciente(paciente_nk, es_actual);

CREATE TABLE IF NOT EXISTS dw.dim_equipo (
  equipo_id      BIGSERIAL PRIMARY KEY,
  equipo_nk      TEXT UNIQUE NOT NULL,
  equipo         TEXT,
  estado_equipo  TEXT,
  vigente_desde  TIMESTAMP NOT NULL DEFAULT now(),
  vigente_hasta  TIMESTAMP,
  es_actual      BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dw.dim_medicamento (
  codigo_medicamento BIGSERIAL PRIMARY KEY,
  medicamento_nk     TEXT UNIQUE NOT NULL,
  nombre             TEXT,
  forma_farmaceutica TEXT,
  via_administracion TEXT
);

CREATE TABLE IF NOT EXISTS dw.dim_pedido (
  numero_pedido_id BIGSERIAL PRIMARY KEY,
  numero_pedido_nk TEXT UNIQUE NOT NULL,
  insumo_solicitado TEXT,
  cantidad          NUMERIC
);

-- Hechos
CREATE TABLE IF NOT EXISTS dw.hecho_equipos (
  solicitud_equipos_id BIGSERIAL PRIMARY KEY,
  equipo_id            BIGINT NOT NULL REFERENCES dw.dim_equipo(equipo_id),
  paciente_id          BIGINT NOT NULL REFERENCES dw.dim_paciente(paciente_id),
  aseguradora_id       BIGINT NOT NULL REFERENCES dw.dim_aseguradora(aseguradora_id),
  fecha_solicitud_id   DATE   NOT NULL REFERENCES dw.dim_fecha(fecha_solicitud_id)
);
CREATE INDEX IF NOT EXISTS ix_heq_fks ON dw.hecho_equipos(equipo_id, paciente_id, aseguradora_id, fecha_solicitud_id);

CREATE TABLE IF NOT EXISTS dw.hecho_solicitud_servicios (
  servicio_id        BIGSERIAL PRIMARY KEY,
  numero_pedido_id   BIGINT NOT NULL REFERENCES dw.dim_pedido(numero_pedido_id),
  paciente_id        BIGINT NOT NULL REFERENCES dw.dim_paciente(paciente_id),
  aseguradora_id     BIGINT NOT NULL REFERENCES dw.dim_aseguradora(aseguradora_id),
  fecha_solicitud_id DATE   NOT NULL REFERENCES dw.dim_fecha(fecha_solicitud_id),
  codigo_medicamento BIGINT NOT NULL REFERENCES dw.dim_medicamento(codigo_medicamento)
);
CREATE INDEX IF NOT EXISTS ix_hss_fks ON dw.hecho_solicitud_servicios(numero_pedido_id, paciente_id, aseguradora_id, fecha_solicitud_id, codigo_medicamento);

-- Vista de pacientes actuales
CREATE OR REPLACE VIEW dw.v_dim_paciente_actual AS
SELECT * FROM dw.dim_paciente WHERE es_actual = TRUE;
