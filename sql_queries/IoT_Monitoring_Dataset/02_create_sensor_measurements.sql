-- ============================================================================
-- PASO 2: Crear Tabla de Mediciones de Sensores (Bronze Layer)
-- ============================================================================
-- Propósito: Tabla A del Paper_NLM - Datos crudos de sensores cada minuto
-- Esta es la tabla principal que recibe datos del sistema IoT
-- ============================================================================

CREATE TABLE IF NOT EXISTS `chillers-478716.IoT_monitoring.sensor_measurements`
(
  -- Identificadores
  measurement_id STRING NOT NULL OPTIONS(description="ID único de la medición (UUID)"),
  installation_id STRING NOT NULL OPTIONS(description="FK a installation_metadata"),
  
  -- Timestamp
  timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp de la medición (cada minuto)"),
  
  -- Variables de Temperatura (°C) - Según Tabla A del paper
  temp_evaporator FLOAT64 OPTIONS(description="Temperatura del evaporador (°C)"),
  temp_suction FLOAT64 OPTIONS(description="Temperatura de succión (°C)"),
  temp_discharge FLOAT64 OPTIONS(description="Temperatura de descarga (°C)"),
  temp_ambient FLOAT64 OPTIONS(description="Temperatura ambiente (°C)"),
  temp_cabinet FLOAT64 OPTIONS(description="Temperatura interior del gabinete (°C)"),
  
  -- Variables de Presión (bar)
  pressure_suction FLOAT64 OPTIONS(description="Presión de succión (bar)"),
  pressure_discharge FLOAT64 OPTIONS(description="Presión de descarga (bar)"),
  
  -- Variables de Consumo Energético
  power_consumption_kw FLOAT64 OPTIONS(description="Consumo de potencia instantáneo (kW)"),
  energy_consumption_kwh FLOAT64 OPTIONS(description="Energía acumulada (kWh)"),
  current_phase_a FLOAT64 OPTIONS(description="Corriente fase A (A)"),
  current_phase_b FLOAT64 OPTIONS(description="Corriente fase B (A)"),
  current_phase_c FLOAT64 OPTIONS(description="Corriente fase C (A)"),
  voltage FLOAT64 OPTIONS(description="Voltaje (V)"),
  power_factor FLOAT64 OPTIONS(description="Factor de potencia"),
  
  -- Estados del Sistema (Boolean)
  compressor_status BOOL OPTIONS(description="Estado del compresor: TRUE=ON, FALSE=OFF"),
  defrost_cycle_active BOOL OPTIONS(description="Ciclo de descongelamiento activo"),
  door_open BOOL OPTIONS(description="Puerta del gabinete abierta"),
  
  -- Alarmas y Eventos
  alarm_high_temp BOOL OPTIONS(description="Alarma de temperatura alta"),
  alarm_low_pressure BOOL OPTIONS(description="Alarma de presión baja"),
  alarm_high_pressure BOOL OPTIONS(description="Alarma de presión alta"),
  
  -- Metadatos de Calidad de Datos
  data_quality STRING OPTIONS(description="good, suspect, bad - según Paper_NLM"),
  gateway_connection_status STRING OPTIONS(description="online, offline, intermittent"),
  
  -- Auditoría
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp de ingesta a BigQuery"),
  source_system STRING DEFAULT 'IoT_Gateway' OPTIONS(description="Sistema fuente de los datos")
)
PARTITION BY DATE(timestamp)
CLUSTER BY installation_id, timestamp
OPTIONS(
  description="Mediciones brutas de sensores cada minuto. Bronze Layer - Tabla A del Paper_NLM.",
  labels=[("layer", "bronze"), ("source", "iot_gateway"), ("frequency", "1min")],
  partition_expiration_days=730  -- Retener 2 años de datos
);

-- ============================================================================
-- Crear tabla de staging para validación (opcional)
-- ============================================================================

CREATE TABLE IF NOT EXISTS `chillers-478716.IoT_monitoring.sensor_measurements_staging`
LIKE `chillers-478716.IoT_monitoring.sensor_measurements`
OPTIONS(
  description="Tabla temporal para validar datos antes de insertar en Bronze",
  labels=[("layer", "staging"), ("temporary", "true")]
);

-- ============================================================================
-- Queries de validación
-- ============================================================================

-- Ver estructura de la tabla
SELECT 
  column_name,
  data_type,
  description
FROM `chillers-478716.IoT_monitoring.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
WHERE table_name = 'sensor_measurements'
ORDER BY column_name;

-- Verificar particiones (después de insertar datos)
SELECT 
  partition_id,
  total_rows,
  total_logical_bytes / POW(10, 9) as size_gb
FROM `chillers-478716.IoT_monitoring.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'sensor_measurements'
ORDER BY partition_id DESC
LIMIT 10;
