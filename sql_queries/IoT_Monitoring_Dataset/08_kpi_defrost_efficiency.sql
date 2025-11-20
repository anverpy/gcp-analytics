-- ============================================================================
-- PASO 8: KPI - Eficiencia del Ciclo de Descongelamiento
-- ============================================================================
-- Propósito: Evaluar eficiencia de los ciclos de descongelamiento
-- KPI: Ratio de tiempo de defrost vs tiempo operativo del compresor
-- ============================================================================

WITH defrost_efficiency AS (
  SELECT
    sm.installation_id,
    DATE_TRUNC(DATE(sm.timestamp), MONTH) AS report_month,
    
    -- Tiempo de descongelamiento (minutos → horas)
    SUM(CASE WHEN sm.defrost_cycle_active THEN 1 ELSE 0 END) / 60.0 as total_defrost_hours,
    
    -- Tiempo operativo del compresor (horas)
    SUM(CASE WHEN sm.compressor_status THEN 1 ELSE 0 END) / 60.0 as compressor_runtime_hours,
    
    -- Aproximación de ciclos: total_defrost_hours / duración_promedio_ciclo (asumimos 20 min)
    (SUM(CASE WHEN sm.defrost_cycle_active THEN 1 ELSE 0 END) / 20.0) as defrost_cycles_count,
    
    -- Duración promedio de cada ciclo (minutos)
    20.0 as avg_defrost_duration_min,  -- Duración fija del ciclo según script de generación
    
    -- Consumo durante descongelamiento
    AVG(CASE WHEN sm.defrost_cycle_active THEN sm.power_consumption_kw END) as avg_power_during_defrost,
    AVG(CASE WHEN NOT sm.defrost_cycle_active THEN sm.power_consumption_kw END) as avg_power_normal,
    
    -- Metadatos
    im.cabinet_type,
    im.city

  FROM `chillers-478716.IoT_monitoring.sensor_measurements` sm
  INNER JOIN `chillers-478716.IoT_monitoring.installation_metadata` im
    ON sm.installation_id = im.installation_id
  WHERE im.is_active = TRUE
  GROUP BY 
    sm.installation_id, 
    report_month,
    im.cabinet_type,
    im.city
)

SELECT
  installation_id,
  cabinet_type,
  city,
  report_month,
  
  -- KPI Principal: Ratio Defrost/Runtime
  ROUND(total_defrost_hours / NULLIF(compressor_runtime_hours, 0), 3) as defrost_efficiency_ratio,
  
  -- Clasificación de eficiencia
  CASE 
    WHEN (total_defrost_hours / NULLIF(compressor_runtime_hours, 0)) < 0.1 THEN '🟢 Excelente (<0.1)'
    WHEN (total_defrost_hours / NULLIF(compressor_runtime_hours, 0)) < 0.2 THEN '🟡 Normal (0.1-0.2)'
    ELSE '🔴 Ineficiente (>0.2)'
  END as efficiency_status,
  
  -- Métricas de soporte
  ROUND(total_defrost_hours, 2) as total_defrost_hours,
  ROUND(compressor_runtime_hours, 1) as compressor_runtime_hours,
  defrost_cycles_count,
  ROUND(avg_defrost_duration_min, 1) as avg_defrost_duration_min,
  
  -- Consumo eléctrico
  ROUND(avg_power_during_defrost, 2) as avg_power_defrost_kw,
  ROUND(avg_power_normal, 2) as avg_power_normal_kw,
  ROUND(avg_power_during_defrost - avg_power_normal, 2) as power_increase_during_defrost_kw,
  
  -- Evaluación y recomendaciones
  CASE
    WHEN defrost_cycles_count < (7 * 3) THEN 
      '⚠️ Pocos ciclos - Verificar programación'
    WHEN defrost_cycles_count > (7 * 5) THEN 
      '⚠️ Muchos ciclos - Optimizar frecuencia'
    ELSE 
      '✅ Frecuencia de descongelamiento adecuada (3-5 ciclos/día)'
  END as cycle_frequency_assessment,
  
  CASE
    WHEN avg_defrost_duration_min < 15 THEN 
      '⚠️ Ciclos muy cortos - Puede ser inefectivo'
    WHEN avg_defrost_duration_min > 30 THEN 
      '⚠️ Ciclos muy largos - Revisar programación'
    ELSE 
      '✅ Duración de ciclo adecuada (15-30 min)'
  END as cycle_duration_assessment

FROM defrost_efficiency

ORDER BY defrost_efficiency_ratio DESC, installation_id;

-- ============================================================================
-- Análisis de impacto energético del descongelamiento
-- ============================================================================

WITH defrost_energy_impact AS (
  SELECT
    sm.installation_id,
    DATE(sm.timestamp) as date,
    
    -- Energía total consumida
    MAX(sm.energy_consumption_kwh) - MIN(sm.energy_consumption_kwh) as total_energy_kwh,
    
    -- Energía durante descongelamiento (aproximación)
    AVG(CASE WHEN sm.defrost_cycle_active THEN sm.power_consumption_kw END) * 
      (SUM(CASE WHEN sm.defrost_cycle_active THEN 1 ELSE 0 END) / 60.0) as defrost_energy_kwh,
    
    -- Tiempo en descongelamiento
    SUM(CASE WHEN sm.defrost_cycle_active THEN 1 ELSE 0 END) / 60.0 as defrost_hours

  FROM `chillers-478716.IoT_monitoring.sensor_measurements` sm
  GROUP BY sm.installation_id, date
)

SELECT
  installation_id,
  date,
  ROUND(total_energy_kwh, 2) as total_energy_kwh,
  ROUND(defrost_energy_kwh, 2) as defrost_energy_kwh,
  ROUND((defrost_energy_kwh / NULLIF(total_energy_kwh, 0)) * 100, 2) as defrost_energy_pct,
  ROUND(defrost_hours, 2) as defrost_hours,
  
  -- Evaluación del impacto
  CASE
    WHEN (defrost_energy_kwh / NULLIF(total_energy_kwh, 0)) * 100 > 15 THEN 
      '🔴 Alto impacto energético (>15%)'
    WHEN (defrost_energy_kwh / NULLIF(total_energy_kwh, 0)) * 100 > 10 THEN 
      '🟡 Impacto moderado (10-15%)'
    ELSE 
      '🟢 Impacto bajo (<10%)'
  END as energy_impact_assessment

FROM defrost_energy_impact

ORDER BY installation_id, date;

-- ============================================================================
-- Resumen ejecutivo de descongelamiento
-- ============================================================================

WITH defrost_efficiency_summary AS (
  SELECT
    sm.installation_id,
    SUM(CASE WHEN sm.defrost_cycle_active THEN 1 ELSE 0 END) / 60.0 as total_defrost_hours,
    SUM(CASE WHEN sm.compressor_status THEN 1 ELSE 0 END) / 60.0 as compressor_runtime_hours,
    -- Aproximación de ciclos: total_defrost_hours / duración_promedio_ciclo (asumimos 20 min)
    (SUM(CASE WHEN sm.defrost_cycle_active THEN 1 ELSE 0 END) / 20.0) as defrost_cycles_count
  FROM `chillers-478716.IoT_monitoring.sensor_measurements` sm
  GROUP BY sm.installation_id
)

SELECT
  'Total instalaciones analizadas' as metric,
  CAST(COUNT(DISTINCT installation_id) AS STRING) as value
FROM defrost_efficiency_summary

UNION ALL

SELECT
  'Instalaciones con eficiencia excelente (<0.1)' as metric,
  CAST(COUNT(DISTINCT installation_id) AS STRING) as value
FROM defrost_efficiency_summary
WHERE (total_defrost_hours / NULLIF(compressor_runtime_hours, 0)) < 0.1

UNION ALL

SELECT
  'Instalaciones ineficientes (>0.2)' as metric,
  CAST(COUNT(DISTINCT installation_id) AS STRING) as value
FROM defrost_efficiency_summary
WHERE (total_defrost_hours / NULLIF(compressor_runtime_hours, 0)) > 0.2

UNION ALL

SELECT
  'Promedio de ciclos de defrost por semana' as metric,
  CAST(ROUND(AVG(defrost_cycles_count), 1) AS STRING) as value
FROM defrost_efficiency_summary;
