-- ============================================================================
-- PASO 2: Crear Tabla de Metadatos de Instalación
-- ============================================================================
-- Propósito: Tabla C del Paper_NLM - Información estática de cada sucursal
-- Esta tabla se usa para normalizar datos y contextualizar KPIs
-- ============================================================================

CREATE TABLE IF NOT EXISTS `chillers-478716.IoT_monitoring.installation_metadata`
(
  -- Identificador único
  installation_id STRING NOT NULL OPTIONS(description="Identificador único de la sucursal/supermercado"),
  
  -- Información del gabinete de refrigeración
  cabinet_type STRING NOT NULL OPTIONS(description="Tipo de gabinete: fruit, meat, frozen"),
  target_operation_temp FLOAT64 OPTIONS(description="Temperatura objetivo de operación (°C)"),
  
  -- Dimensiones y ubicación
  square_meters FLOAT64 NOT NULL OPTIONS(description="Área del supermercado en metros cuadrados (para cálculo de KPI)"),
  geographic_location STRING OPTIONS(description="País y ubicación específica"),
  city STRING OPTIONS(description="Ciudad de la instalación"),
  country STRING DEFAULT 'Argentina' OPTIONS(description="País de la instalación"),
  
  -- Información del hardware
  gateway_model STRING DEFAULT 'SIMATIC IOT2040' OPTIONS(description="Modelo del gateway IoT"),
  controller_model STRING DEFAULT 'EWCM9100' OPTIONS(description="Modelo del controlador de refrigeración"),
  energy_meter_model STRING DEFAULT 'CVM-MINI' OPTIONS(description="Modelo del medidor de energía"),
  
  -- Metadatos de gestión
  installation_date DATE OPTIONS(description="Fecha de instalación del sistema de monitoreo"),
  is_active BOOL DEFAULT TRUE OPTIONS(description="Si la instalación está actualmente activa"),
  notes STRING OPTIONS(description="Notas adicionales sobre la instalación"),
  
  -- Auditoría
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp de creación del registro"),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp de última actualización")
)
OPTIONS(
  description="Metadatos estáticos de cada instalación de refrigeración monitoreada. Basado en Tabla C del Paper_NLM.",
  labels=[("layer", "silver"), ("source", "manual_entry"), ("data_type", "metadata")]
);

-- ============================================================================
-- Crear índices para mejorar performance
-- ============================================================================

-- BigQuery no requiere índices explícitos, pero podemos usar clustering
-- Recrear la tabla con clustering si es necesario en el futuro:
-- ALTER TABLE `chillers-478716.IoT_monitoring.installation_metadata`
-- CLUSTER BY installation_id;

-- ============================================================================
-- Insertar datos de ejemplo de las 3 sucursales mencionadas en el paper
-- ============================================================================

INSERT INTO `chillers-478716.IoT_monitoring.installation_metadata`
(installation_id, cabinet_type, target_operation_temp, square_meters, geographic_location, city, country, installation_date, is_active)
VALUES
  ('STORE_ARG_001', 'fruit', 4.0, 850.0, 'Buenos Aires, Argentina', 'Buenos Aires', 'Argentina', '2023-01-15', TRUE),
  ('STORE_ARG_002', 'meat', 2.0, 720.0, 'Córdoba, Argentina', 'Córdoba', 'Argentina', '2023-02-20', TRUE),
  ('STORE_ARG_003', 'frozen', -18.0, 950.0, 'Rosario, Argentina', 'Rosario', 'Argentina', '2023-03-10', TRUE);

-- ============================================================================
-- Queries de validación
-- ============================================================================

-- Ver todas las instalaciones
SELECT * FROM `chillers-478716.IoT_monitoring.installation_metadata`;

-- Resumen por tipo de gabinete
SELECT 
  cabinet_type,
  COUNT(*) as num_installations,
  AVG(square_meters) as avg_square_meters,
  AVG(target_operation_temp) as avg_target_temp
FROM `chillers-478716.IoT_monitoring.installation_metadata`
WHERE is_active = TRUE
GROUP BY cabinet_type
ORDER BY cabinet_type;
