-- ============================================================================
-- PASO 5: Calcular KPIs Mensuales desde Sensor Measurements
-- ============================================================================
-- Propósito: Agregar datos de sensor_measurements para poblar kpi_energy_monthly
-- Calcula el KPI principal: kWh/m²/mes para benchmarking
-- ============================================================================

INSERT INTO `chillers-478716.IoT_monitoring.kpi_energy_monthly`
(
  kpi_id,
  installation_id,
  report_month,
  energy_per_sqm,
  total_energy_kwh,
  avg_power_kw,
  peak_power_kw,
  min_power_kw,
  compressor_runtime_hours,
  compressor_cycles,
  defrost_cycles_count,
  avg_temp_cabinet,
  temp_deviation_hours,
  cop_estimated,
  energy_efficiency_ratio,
  data_completeness_pct,
  alarm_count
)
WITH monthly_aggregations AS (
  SELECT
    sm.installation_id,
    DATE_TRUNC(DATE(sm.timestamp), MONTH) AS report_month,
    
    -- Métricas de energía
    MAX(sm.energy_consumption_kwh) - MIN(sm.energy_consumption_kwh) AS total_energy_kwh,
    AVG(sm.power_consumption_kw) AS avg_power_kw,
    MAX(sm.power_consumption_kw) AS peak_power_kw,
    MIN(sm.power_consumption_kw) AS min_power_kw,
    
    -- Métricas operativas
    SUM(CASE WHEN sm.compressor_status THEN 1 ELSE 0 END) / 60.0 AS compressor_runtime_hours,
    
    -- Estimar ciclos del compresor (cambios de OFF a ON)
    -- Aproximación: runtime_hours / avg_cycle_duration (asumimos 2h por ciclo)
    CAST((SUM(CASE WHEN sm.compressor_status THEN 1 ELSE 0 END) / 60.0) / 2.0 AS INT64) AS compressor_cycles,
    
    CAST(SUM(CASE WHEN sm.defrost_cycle_active THEN 1 ELSE 0 END) / 20 AS INT64) AS defrost_cycles_count,
    
    -- Métricas de temperatura
    AVG(sm.temp_cabinet) AS avg_temp_cabinet,
    
    -- Horas fuera de rango (temperatura desviada)
    COUNTIF(
      ABS(sm.temp_cabinet - im.target_operation_temp) > 2.0
    ) / 60.0 AS temp_deviation_hours,
    
    -- Calidad de datos
    COUNTIF(sm.data_quality = 'good') * 100.0 / COUNT(*) AS data_completeness_pct,
    
    -- Alarmas totales
    COUNTIF(sm.alarm_high_temp OR sm.alarm_low_pressure OR sm.alarm_high_pressure) AS alarm_count,
    
    -- Metadata para cálculos
    im.square_meters,
    im.target_operation_temp
    
  FROM `chillers-478716.IoT_monitoring.sensor_measurements` sm
  INNER JOIN `chillers-478716.IoT_monitoring.installation_metadata` im
    ON sm.installation_id = im.installation_id
  WHERE im.is_active = TRUE
  GROUP BY 
    sm.installation_id, 
    report_month,
    im.square_meters,
    im.target_operation_temp
),

baseline_energy AS (
  -- Calcular energía baseline (promedio de todas las instalaciones) para ratio de eficiencia
  SELECT
    report_month,
    AVG(total_energy_kwh / square_meters) AS baseline_energy_per_sqm
  FROM monthly_aggregations
  GROUP BY report_month
)

SELECT
  GENERATE_UUID() AS kpi_id,
  ma.installation_id,
  ma.report_month,
  
  -- KPI PRINCIPAL del Paper: Energía por m² por mes
  ma.total_energy_kwh / ma.square_meters AS energy_per_sqm,
  
  -- Métricas de energía
  ma.total_energy_kwh,
  ma.avg_power_kw,
  ma.peak_power_kw,
  ma.min_power_kw,
  
  -- Métricas operativas
  ma.compressor_runtime_hours,
  ma.compressor_cycles,
  ma.defrost_cycles_count,
  
  -- Métricas de temperatura
  ma.avg_temp_cabinet,
  ma.temp_deviation_hours,
  
  -- COP estimado (Coefficient of Performance)
  -- Simplified: cooling_capacity / power_input
  -- Asumimos cooling capacity proporcional a (temp_ambient - temp_cabinet)
  CASE 
    WHEN ma.avg_power_kw > 0 THEN 
      (20.0 - ma.avg_temp_cabinet) / ma.avg_power_kw  -- Aproximación simple
    ELSE NULL 
  END AS cop_estimated,
  
  -- Ratio de eficiencia vs baseline (1.0 = promedio, <1.0 = más eficiente)
  (ma.total_energy_kwh / ma.square_meters) / be.baseline_energy_per_sqm AS energy_efficiency_ratio,
  
  -- Calidad de datos
  ma.data_completeness_pct,
  ma.alarm_count

FROM monthly_aggregations ma
LEFT JOIN baseline_energy be
  ON ma.report_month = be.report_month;

-- ============================================================================
-- Validación de KPIs calculados
-- ============================================================================

-- Ver KPIs por instalación
SELECT 
  installation_id,
  report_month,
  ROUND(energy_per_sqm, 2) as kWh_per_sqm,
  ROUND(total_energy_kwh, 2) as total_kwh,
  ROUND(compressor_runtime_hours, 1) as runtime_hours,
  alarm_count
FROM `chillers-478716.IoT_monitoring.kpi_energy_monthly`
ORDER BY report_month, installation_id;

-- Benchmarking: Comparar instalaciones
SELECT 
  installation_id,
  ROUND(energy_per_sqm, 2) as kWh_per_sqm,
  ROUND(energy_efficiency_ratio, 3) as efficiency_ratio,
  CASE 
    WHEN energy_efficiency_ratio < 0.95 THEN '🟢 Excelente'
    WHEN energy_efficiency_ratio < 1.05 THEN '🟡 Normal'
    ELSE '🔴 Ineficiente'
  END as performance
FROM `chillers-478716.IoT_monitoring.kpi_energy_monthly`
ORDER BY energy_per_sqm;
