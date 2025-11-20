-- ============================================================================
-- PASO 5: Análisis de Eficiencia Energética por Instalación
-- ============================================================================
-- Propósito: Comparar eficiencia energética entre instalaciones
-- KPI: Consumo normalizado por condiciones ambientales
-- ============================================================================

SELECT
  im.installation_id,
  im.cabinet_type,
  im.city,
  
  -- KPI: Energía por m² por mes
  ROUND(km.energy_per_sqm, 2) as kWh_per_sqm_month,
  
  -- Ranking de eficiencia (1 = más eficiente)
  RANK() OVER (ORDER BY km.energy_per_sqm ASC) as efficiency_rank,
  
  -- Ratio vs mejor instalación
  ROUND(km.energy_per_sqm / MIN(km.energy_per_sqm) OVER (), 2) as ratio_vs_best,
  
  -- Performance del compresor
  ROUND(km.compressor_runtime_hours, 1) as runtime_hours,
  km.compressor_cycles,
  ROUND(km.compressor_runtime_hours * 100 / (24 * 7), 1) as uptime_percentage,
  
  -- Calidad operativa
  km.alarm_count,
  ROUND(km.temp_deviation_hours, 1) as hours_out_of_range,
  ROUND(km.data_completeness_pct, 1) as data_quality_pct,
  
  -- Clasificación de performance
  CASE 
    WHEN km.energy_efficiency_ratio < 0.90 THEN '🟢 Excelente (<10% del promedio)'
    WHEN km.energy_efficiency_ratio < 1.10 THEN '🟡 Normal (±10% del promedio)'
    ELSE '🔴 Ineficiente (>10% del promedio)'
  END as performance_category,
  
  -- Métricas de referencia
  ROUND(km.avg_power_kw, 2) as avg_power_kw,
  ROUND(km.cop_estimated, 2) as cop_estimated

FROM `chillers-478716.IoT_monitoring.kpi_energy_monthly` km
INNER JOIN `chillers-478716.IoT_monitoring.installation_metadata` im
  ON km.installation_id = im.installation_id

ORDER BY km.energy_per_sqm ASC;

-- ============================================================================
-- Análisis de oportunidades de mejora
-- ============================================================================

WITH best_practice AS (
  SELECT
    cabinet_type,
    MIN(energy_per_sqm) as best_energy_per_sqm
  FROM `chillers-478716.IoT_monitoring.kpi_energy_monthly` km
  INNER JOIN `chillers-478716.IoT_monitoring.installation_metadata` im
    ON km.installation_id = im.installation_id
  GROUP BY cabinet_type
)

SELECT
  im.installation_id,
  im.cabinet_type,
  ROUND(km.energy_per_sqm, 2) as current_kWh_per_sqm,
  ROUND(bp.best_energy_per_sqm, 2) as best_practice_kWh_per_sqm,
  ROUND(km.energy_per_sqm - bp.best_energy_per_sqm, 2) as improvement_potential,
  ROUND((km.energy_per_sqm - bp.best_energy_per_sqm) * im.square_meters, 2) as monthly_savings_kwh,
  
  -- Recomendaciones
  CASE
    WHEN km.alarm_count > 5 THEN '⚠️ Revisar sistema - Muchas alarmas'
    WHEN km.temp_deviation_hours > 10 THEN '⚠️ Ajustar setpoint de temperatura'
    WHEN km.compressor_cycles > 100 THEN '⚠️ Demasiados ciclos - Revisar control'
    WHEN km.cop_estimated < 2.0 THEN '⚠️ COP bajo - Mantenimiento requerido'
    ELSE '✅ Operación normal'
  END as recommendation

FROM `chillers-478716.IoT_monitoring.kpi_energy_monthly` km
INNER JOIN `chillers-478716.IoT_monitoring.installation_metadata` im
  ON km.installation_id = im.installation_id
INNER JOIN best_practice bp
  ON im.cabinet_type = bp.cabinet_type

WHERE km.energy_per_sqm > bp.best_energy_per_sqm

ORDER BY improvement_potential DESC;
