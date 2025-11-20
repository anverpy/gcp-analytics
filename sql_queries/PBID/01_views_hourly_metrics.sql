-- ============================================================================
-- PASO 1: Vista de Métricas Horarias (Agregación)
-- ============================================================================
-- Propósito: Reducir granularidad de minuto a hora para Power BI
-- Optimiza el rendimiento del dashboard reduciendo filas (1/60 del tamaño)
-- ============================================================================

CREATE OR REPLACE VIEW `chillers-478716.IoT_monitoring.vw_hourly_metrics` AS
SELECT
  installation_id,
  TIMESTAMP_TRUNC(timestamp, HOUR) as hour,
  
  -- Promedios de Temperatura
  AVG(temp_cabinet) as avg_temp_cabinet,
  AVG(temp_evaporator) as avg_temp_evaporator,
  AVG(temp_suction) as avg_temp_suction,
  AVG(temp_discharge) as avg_temp_discharge,
  AVG(temp_ambient) as avg_temp_ambient,
  
  -- Promedios de Presión
  AVG(pressure_suction) as avg_pressure_suction,
  AVG(pressure_discharge) as avg_pressure_discharge,
  
  -- Energía
  AVG(power_consumption_kw) as avg_power_kw,
  SUM(energy_consumption_kwh) as total_energy_kwh,
  
  -- Métricas Operativas
  -- % de tiempo que el compresor estuvo encendido en esa hora
  COUNTIF(compressor_status) / COUNT(*) as compressor_utilization,
  
  -- Minutos totales en descongelamiento durante esa hora
  COUNTIF(defrost_cycle_active) as defrost_minutes,
  
  -- Conteo de Alarmas
  COUNTIF(alarm_high_temp OR alarm_low_pressure OR alarm_high_pressure) as alarm_count

FROM `chillers-478716.IoT_monitoring.sensor_measurements`
WHERE MOD(EXTRACT(HOUR FROM timestamp), 6) = 0  -- ✅ Filtrar cada 6 horas
GROUP BY installation_id, hour;