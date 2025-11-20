-- ============================================================================
-- PASO 4: Vista de Análisis de Alarmas y Eventos
-- ============================================================================
-- Propósito: Detectar patrones de alarmas y problemas operativos
-- Uso: Dashboards de mantenimiento, alertas preventivas, análisis de confiabilidad
-- ============================================================================

CREATE OR REPLACE VIEW `chillers-478716.IoT_monitoring.vw_alarm_analysis` AS
WITH hourly_alarms AS (
  SELECT
    installation_id,
    TIMESTAMP_TRUNC(timestamp, HOUR) as hour,
    DATE(timestamp) as date,
    EXTRACT(HOUR FROM timestamp) as hour_of_day,
    EXTRACT(DAYOFWEEK FROM timestamp) as day_of_week,
    
    -- Conteo de cada tipo de alarma
    COUNTIF(alarm_high_temp) as alarm_high_temp_count,
    COUNTIF(alarm_low_pressure) as alarm_low_pressure_count,
    COUNTIF(alarm_high_pressure) as alarm_high_pressure_count,
    
    -- Total de alarmas
    COUNTIF(alarm_high_temp OR alarm_low_pressure OR alarm_high_pressure) as total_alarms,
    
    -- Minutos con alarma activa
    COUNTIF(alarm_high_temp OR alarm_low_pressure OR alarm_high_pressure) as alarm_minutes,
    
    -- Contexto operativo durante las alarmas
    AVG(CASE WHEN alarm_high_temp THEN temp_cabinet ELSE NULL END) as avg_temp_during_alarm,
    AVG(CASE WHEN alarm_low_pressure THEN pressure_suction ELSE NULL END) as avg_pressure_during_alarm,
    AVG(power_consumption_kw) as avg_power_kw,
    
    -- Estados del sistema
    COUNTIF(compressor_status) / COUNT(*) as compressor_uptime_pct,
    COUNTIF(defrost_cycle_active) as defrost_minutes
    
  FROM `chillers-478716.IoT_monitoring.sensor_measurements`
  GROUP BY installation_id, hour, date, hour_of_day, day_of_week
)
SELECT
  ha.installation_id,
  ha.hour,
  ha.date,
  ha.hour_of_day,
  
  -- Día de la semana legible
  CASE ha.day_of_week
    WHEN 1 THEN 'Domingo'
    WHEN 2 THEN 'Lunes'
    WHEN 3 THEN 'Martes'
    WHEN 4 THEN 'Miércoles'
    WHEN 5 THEN 'Jueves'
    WHEN 6 THEN 'Viernes'
    WHEN 7 THEN 'Sábado'
  END as day_name,
  
  -- Metadata
  m.city,
  m.cabinet_type,
  
  -- Métricas de alarmas
  ha.alarm_high_temp_count,
  ha.alarm_low_pressure_count,
  ha.alarm_high_pressure_count,
  ha.total_alarms,
  ROUND(ha.alarm_minutes / 60.0, 2) as alarm_hours,
  
  -- Severidad de las alarmas
  CASE
    WHEN ha.total_alarms = 0 THEN '✅ Sin alarmas'
    WHEN ha.total_alarms <= 5 THEN '⚠️ Alarmas menores'
    WHEN ha.total_alarms <= 15 THEN '🟠 Alarmas moderadas'
    ELSE '🔴 Alarmas críticas'
  END as alarm_severity,
  
  -- Tipo de alarma predominante
  CASE
    WHEN ha.alarm_high_temp_count > ha.alarm_low_pressure_count 
         AND ha.alarm_high_temp_count > ha.alarm_high_pressure_count THEN 'Temperatura'
    WHEN ha.alarm_low_pressure_count > ha.alarm_high_pressure_count THEN 'Presión Baja'
    WHEN ha.alarm_high_pressure_count > 0 THEN 'Presión Alta'
    ELSE 'Normal'
  END as primary_alarm_type,
  
  -- Contexto operativo
  ROUND(ha.avg_temp_during_alarm, 2) as temp_during_alarm,
  ROUND(ha.avg_pressure_during_alarm, 2) as pressure_during_alarm,
  ROUND(ha.avg_power_kw, 2) as avg_power_kw,
  ROUND(ha.compressor_uptime_pct * 100, 1) as compressor_uptime_pct,
  ha.defrost_minutes,
  
  -- Clasificación de turno
  CASE
    WHEN ha.hour_of_day BETWEEN 6 AND 14 THEN 'Mañana'
    WHEN ha.hour_of_day BETWEEN 15 AND 22 THEN 'Tarde'
    ELSE 'Noche'
  END as shift

FROM hourly_alarms ha
LEFT JOIN `chillers-478716.IoT_monitoring.installation_metadata` m
  ON ha.installation_id = m.installation_id
WHERE ha.total_alarms > 0;
