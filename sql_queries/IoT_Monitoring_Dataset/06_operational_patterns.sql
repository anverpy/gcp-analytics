-- ============================================================================
-- PASO 6: Análisis de Patrones Operativos y Anomalías
-- ============================================================================
-- Propósito: Detectar patrones inusuales en la operación diaria
-- KPI: Análisis de comportamiento horario y detección de anomalías
-- ============================================================================

-- Análisis de consumo por hora del día
WITH hourly_patterns AS (
  SELECT
    sm.installation_id,
    EXTRACT(HOUR FROM sm.timestamp) as hour_of_day,
    AVG(sm.power_consumption_kw) as avg_power,
    AVG(sm.temp_cabinet) as avg_temp,
    AVG(sm.temp_ambient) as avg_ambient_temp,
    COUNT(*) as measurements,
    
    -- % de tiempo con alarmas
    COUNTIF(sm.alarm_high_temp OR sm.alarm_low_pressure OR sm.alarm_high_pressure) * 100.0 / COUNT(*) as alarm_rate_pct

  FROM `chillers-478716.IoT_monitoring.sensor_measurements` sm
  GROUP BY sm.installation_id, hour_of_day
)

SELECT
  im.installation_id,
  im.cabinet_type,
  hp.hour_of_day,
  ROUND(hp.avg_power, 2) as avg_power_kw,
  ROUND(hp.avg_temp, 2) as avg_temp_cabinet,
  ROUND(hp.avg_ambient_temp, 2) as avg_temp_ambient,
  ROUND(hp.alarm_rate_pct, 2) as alarm_rate_pct,
  
  -- Detectar horas pico
  CASE 
    WHEN hp.avg_power > AVG(hp.avg_power) OVER (PARTITION BY hp.installation_id) * 1.2 
    THEN '⚡ Hora pico'
    ELSE 'Normal'
  END as consumption_pattern,
  
  -- Detectar anomalías de temperatura
  CASE
    WHEN ABS(hp.avg_temp - im.target_operation_temp) > 3.0 
    THEN '🌡️ Desviación térmica'
    ELSE 'OK'
  END as temp_status

FROM hourly_patterns hp
INNER JOIN `chillers-478716.IoT_monitoring.installation_metadata` im
  ON hp.installation_id = im.installation_id

ORDER BY hp.installation_id, hp.hour_of_day;

-- ============================================================================
-- Análisis de eventos de descongelamiento
-- ============================================================================

WITH defrost_events AS (
  SELECT
    sm.installation_id,
    DATE(sm.timestamp) as date,
    COUNTIF(sm.defrost_cycle_active) / 20 as defrost_count,
    
    -- Consumo durante descongelamiento vs operación normal
    AVG(CASE WHEN sm.defrost_cycle_active THEN sm.power_consumption_kw END) as avg_power_defrost,
    AVG(CASE WHEN NOT sm.defrost_cycle_active THEN sm.power_consumption_kw END) as avg_power_normal,
    
    -- Impacto térmico del descongelamiento
    AVG(CASE WHEN sm.defrost_cycle_active THEN sm.temp_cabinet END) as avg_temp_during_defrost

  FROM `chillers-478716.IoT_monitoring.sensor_measurements` sm
  GROUP BY sm.installation_id, date
)

SELECT
  im.installation_id,
  im.cabinet_type,
  de.date,
  de.defrost_count,
  ROUND(de.avg_power_defrost, 2) as power_during_defrost_kw,
  ROUND(de.avg_power_normal, 2) as power_normal_kw,
  ROUND(de.avg_power_defrost - de.avg_power_normal, 2) as power_increase_kw,
  ROUND(de.avg_temp_during_defrost, 2) as temp_during_defrost,
  
  -- Evaluación del ciclo de descongelamiento
  CASE
    WHEN de.defrost_count < 3 THEN '⚠️ Pocos ciclos - Verificar'
    WHEN de.defrost_count > 5 THEN '⚠️ Muchos ciclos - Revisar programación'
    ELSE '✅ Normal (3-5 ciclos/día)'
  END as defrost_evaluation

FROM defrost_events de
INNER JOIN `chillers-478716.IoT_monitoring.installation_metadata` im
  ON de.installation_id = im.installation_id

ORDER BY de.installation_id, de.date;

-- ============================================================================
-- Detección de anomalías en datos de sensores
-- ============================================================================

WITH sensor_stats AS (
  SELECT
    installation_id,
    AVG(power_consumption_kw) as avg_power,
    STDDEV(power_consumption_kw) as stddev_power,
    AVG(temp_cabinet) as avg_temp,
    STDDEV(temp_cabinet) as stddev_temp
  FROM `chillers-478716.IoT_monitoring.sensor_measurements`
  GROUP BY installation_id
),

anomalies AS (
  SELECT
    sm.installation_id,
    sm.timestamp,
    sm.power_consumption_kw,
    sm.temp_cabinet,
    
    -- Z-score para detectar outliers
    (sm.power_consumption_kw - ss.avg_power) / NULLIF(ss.stddev_power, 0) as power_zscore,
    (sm.temp_cabinet - ss.avg_temp) / NULLIF(ss.stddev_temp, 0) as temp_zscore
    
  FROM `chillers-478716.IoT_monitoring.sensor_measurements` sm
  INNER JOIN sensor_stats ss
    ON sm.installation_id = ss.installation_id
  
  WHERE 
    ABS((sm.power_consumption_kw - ss.avg_power) / NULLIF(ss.stddev_power, 0)) > 3
    OR ABS((sm.temp_cabinet - ss.avg_temp) / NULLIF(ss.stddev_temp, 0)) > 3
)

SELECT
  installation_id,
  timestamp,
  ROUND(power_consumption_kw, 2) as power_kw,
  ROUND(temp_cabinet, 2) as temp,
  ROUND(power_zscore, 2) as power_zscore,
  ROUND(temp_zscore, 2) as temp_zscore,
  
  CASE
    WHEN ABS(power_zscore) > 3 THEN '⚠️ Anomalía de potencia'
    WHEN ABS(temp_zscore) > 3 THEN '⚠️ Anomalía de temperatura'
    ELSE 'OK'
  END as anomaly_type

FROM anomalies

ORDER BY installation_id, timestamp
LIMIT 100;
