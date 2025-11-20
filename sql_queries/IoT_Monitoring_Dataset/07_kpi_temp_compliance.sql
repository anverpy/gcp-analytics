-- ============================================================================
-- PASO 7: KPI - Tiempo Fuera de Rango de Temperatura
-- ============================================================================
-- Propósito: Medir calidad del control de temperatura por instalación
-- KPI: % de tiempo que la temperatura estuvo fuera del rango objetivo (±2°C)
-- ============================================================================

WITH temp_compliance AS (
  SELECT
    sm.installation_id,
    DATE_TRUNC(DATE(sm.timestamp), MONTH) AS report_month,
    
    -- Total de mediciones
    COUNT(*) as total_measurements,
    
    -- Mediciones fuera de rango (±2°C del objetivo)
    COUNTIF(ABS(sm.temp_cabinet - im.target_operation_temp) > 2.0) as out_of_range_count,
    
    -- % fuera de rango
    COUNTIF(ABS(sm.temp_cabinet - im.target_operation_temp) > 2.0) * 100.0 / COUNT(*) as out_of_range_pct,
    
    -- Horas fuera de rango
    COUNTIF(ABS(sm.temp_cabinet - im.target_operation_temp) > 2.0) / 60.0 as hours_out_of_range,
    
    -- Temperatura promedio y desviación
    AVG(sm.temp_cabinet) as avg_temp_cabinet,
    STDDEV(sm.temp_cabinet) as stddev_temp_cabinet,
    
    -- Desviación promedio del objetivo
    AVG(ABS(sm.temp_cabinet - im.target_operation_temp)) as avg_deviation,
    
    -- Metadatos
    im.target_operation_temp,
    im.cabinet_type,
    im.city

  FROM `chillers-478716.IoT_monitoring.sensor_measurements` sm
  INNER JOIN `chillers-478716.IoT_monitoring.installation_metadata` im
    ON sm.installation_id = im.installation_id
  WHERE im.is_active = TRUE
  GROUP BY 
    sm.installation_id, 
    report_month,
    im.target_operation_temp,
    im.cabinet_type,
    im.city
)

SELECT
  installation_id,
  cabinet_type,
  city,
  report_month,
  
  -- KPI Principal
  ROUND(out_of_range_pct, 2) as temp_out_of_range_pct,
  
  -- Clasificación de performance
  CASE 
    WHEN out_of_range_pct < 5 THEN '🟢 Excelente (<5%)'
    WHEN out_of_range_pct < 10 THEN '🟡 Aceptable (5-10%)'
    ELSE '🔴 Problema (>10%)'
  END as compliance_status,
  
  -- Métricas de soporte
  ROUND(hours_out_of_range, 1) as hours_out_of_range,
  ROUND(avg_temp_cabinet, 2) as avg_temp_actual,
  target_operation_temp,
  ROUND(avg_deviation, 2) as avg_deviation_celsius,
  ROUND(stddev_temp_cabinet, 2) as temp_variability,
  
  -- Impacto estimado
  CASE
    WHEN out_of_range_pct > 10 THEN 
      CONCAT('⚠️ Riesgo de pérdida de producto (', CAST(ROUND(hours_out_of_range) AS STRING), ' horas)')
    WHEN out_of_range_pct > 5 THEN 
      '⚠️ Monitorear de cerca'
    ELSE 
      '✅ Control de temperatura adecuado'
  END as impact_assessment

FROM temp_compliance

ORDER BY out_of_range_pct DESC, installation_id;

-- ============================================================================
-- Análisis de tendencias de temperatura por hora del día
-- ============================================================================

WITH hourly_temp_analysis AS (
  SELECT
    sm.installation_id,
    EXTRACT(HOUR FROM sm.timestamp) as hour_of_day,
    im.target_operation_temp,
    
    AVG(sm.temp_cabinet) as avg_temp,
    COUNTIF(ABS(sm.temp_cabinet - im.target_operation_temp) > 2.0) * 100.0 / COUNT(*) as out_of_range_pct
    
  FROM `chillers-478716.IoT_monitoring.sensor_measurements` sm
  INNER JOIN `chillers-478716.IoT_monitoring.installation_metadata` im
    ON sm.installation_id = im.installation_id
  GROUP BY sm.installation_id, hour_of_day, im.target_operation_temp
)

SELECT
  installation_id,
  hour_of_day,
  ROUND(avg_temp, 2) as avg_temp_cabinet,
  ROUND(target_operation_temp, 2) as target_temp,
  ROUND(avg_temp - target_operation_temp, 2) as deviation,
  ROUND(out_of_range_pct, 2) as out_of_range_pct,
  
  -- Identificar horas problemáticas
  CASE
    WHEN out_of_range_pct > 15 THEN '🔴 Hora crítica'
    WHEN out_of_range_pct > 8 THEN '🟡 Hora de atención'
    ELSE '🟢 Normal'
  END as hour_status

FROM hourly_temp_analysis

ORDER BY installation_id, hour_of_day;

-- ============================================================================
-- Resumen ejecutivo
-- ============================================================================

WITH temp_compliance_summary AS (
  SELECT
    sm.installation_id,
    COUNTIF(ABS(sm.temp_cabinet - im.target_operation_temp) > 2.0) * 100.0 / COUNT(*) as out_of_range_pct,
    COUNTIF(ABS(sm.temp_cabinet - im.target_operation_temp) > 2.0) / 60.0 as hours_out_of_range
  FROM `chillers-478716.IoT_monitoring.sensor_measurements` sm
  INNER JOIN `chillers-478716.IoT_monitoring.installation_metadata` im
    ON sm.installation_id = im.installation_id
  WHERE im.is_active = TRUE
  GROUP BY sm.installation_id
)

SELECT
  'Total instalaciones analizadas' as metric,
  CAST(COUNT(DISTINCT installation_id) AS STRING) as value
FROM temp_compliance_summary

UNION ALL

SELECT
  'Instalaciones con compliance <5%' as metric,
  CAST(COUNT(DISTINCT installation_id) AS STRING) as value
FROM temp_compliance_summary
WHERE out_of_range_pct < 5

UNION ALL

SELECT
  'Instalaciones que requieren atención (>10%)' as metric,
  CAST(COUNT(DISTINCT installation_id) AS STRING) as value
FROM temp_compliance_summary
WHERE out_of_range_pct > 10

UNION ALL

SELECT
  'Promedio de horas fuera de rango (mensual)' as metric,
  CAST(ROUND(AVG(hours_out_of_range), 1) AS STRING) as value
FROM temp_compliance_summary;
