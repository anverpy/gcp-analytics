-- ============================================================================
-- PASO 3: Vista de Benchmarking Entre Instalaciones
-- ============================================================================
-- Propósito: Comparar eficiencia entre instalaciones y detectar mejores prácticas
-- Uso: Rankings, análisis de varianza, oportunidades de ahorro
-- ============================================================================

CREATE OR REPLACE VIEW `chillers-478716.IoT_monitoring.vw_benchmarking` AS
WITH monthly_stats AS (
  SELECT
    installation_id,
    report_month as month,
    energy_per_sqm,
    total_energy_kwh,
    compressor_runtime_hours,
    cop_estimated,
    alarm_count
  FROM `chillers-478716.IoT_monitoring.kpi_energy_monthly`
),
rankings AS (
  SELECT
    ms.*,
    -- Ranking por eficiencia energética (1 = mejor)
    RANK() OVER (PARTITION BY ms.month ORDER BY energy_per_sqm ASC) as efficiency_rank,
    
    -- Promedios del mes para comparación
    AVG(energy_per_sqm) OVER (PARTITION BY ms.month) as avg_energy_per_sqm,
    MIN(energy_per_sqm) OVER (PARTITION BY ms.month) as best_energy_per_sqm,
    MAX(energy_per_sqm) OVER (PARTITION BY ms.month) as worst_energy_per_sqm,
    
    -- Estadísticas para detección de outliers
    STDDEV(energy_per_sqm) OVER (PARTITION BY ms.month) as stddev_energy_per_sqm
  FROM monthly_stats ms
)
SELECT
  r.installation_id,
  r.month,
  r.energy_per_sqm,
  r.total_energy_kwh,
  r.compressor_runtime_hours,
  r.cop_estimated,
  r.alarm_count,
  
  -- Metadata de instalación
  m.city,
  m.cabinet_type,
  m.square_meters as floor_area_sqm,
  
  -- Rankings y comparaciones
  r.efficiency_rank,
  ROUND(r.avg_energy_per_sqm, 2) as market_avg_energy_per_sqm,
  ROUND(r.best_energy_per_sqm, 2) as best_performer_energy_per_sqm,
  
  -- Varianza vs promedio (%)
  ROUND((r.energy_per_sqm - r.avg_energy_per_sqm) / r.avg_energy_per_sqm * 100, 1) as variance_from_avg_pct,
  
  -- Potencial de ahorro comparado con el mejor (usar m.square_meters directamente)
  ROUND((r.energy_per_sqm - r.best_energy_per_sqm) * m.square_meters, 2) as potential_savings_kwh,
  ROUND((r.energy_per_sqm - r.best_energy_per_sqm) * m.square_meters * 0.15, 2) as potential_savings_usd,
  
  -- Clasificación de performance
  CASE
    WHEN r.efficiency_rank = 1 THEN '🏆 Líder'
    WHEN r.energy_per_sqm <= r.avg_energy_per_sqm THEN '✅ Sobre el promedio'
    WHEN r.energy_per_sqm <= r.avg_energy_per_sqm + r.stddev_energy_per_sqm THEN '⚠️ Bajo el promedio'
    ELSE '🔴 Requiere atención'
  END as performance_status,
  
  -- Z-score para detección de anomalías
  ROUND((r.energy_per_sqm - r.avg_energy_per_sqm) / NULLIF(r.stddev_energy_per_sqm, 0), 2) as z_score

FROM rankings r
LEFT JOIN `chillers-478716.IoT_monitoring.installation_metadata` m
  ON r.installation_id = m.installation_id;
