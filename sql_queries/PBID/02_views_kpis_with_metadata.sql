-- ============================================================================
-- PASO 2: Vista de KPIs con Metadata de Instalación
-- ============================================================================
-- Propósito: Combinar KPIs mensuales con información de instalación
-- Uso: Filtros por ciudad, tipo de gabinete, análisis de eficiencia
-- ============================================================================

CREATE OR REPLACE VIEW `chillers-478716.IoT_monitoring.vw_kpis_with_metadata` AS
SELECT
  -- KPIs principales
  k.installation_id,
  k.report_month as month,
  k.total_energy_kwh,
  k.energy_per_sqm,
  k.avg_power_kw,
  k.peak_power_kw,
  k.compressor_runtime_hours,
  k.compressor_cycles,
  k.defrost_cycles_count,
  k.alarm_count,
  k.temp_deviation_hours,
  k.cop_estimated,
  k.energy_efficiency_ratio,
  
  -- Metadata de instalación (para filtros y slicers)
  m.city,
  'Argentina' as country,  -- ✅ NUEVO: País explícito
  CONCAT(m.city, ', Argentina') as location_full,  -- ✅ NUEVO: Para mapas
  m.cabinet_type,
  m.square_meters as floor_area_sqm,
  m.target_operation_temp,
  m.controller_model as refrigerant_type,
  m.controller_model as compressor_model,
  m.installation_date,
  
  -- Campos calculados adicionales
  ROUND(k.energy_per_sqm, 2) as energy_intensity,
  CASE
    WHEN k.energy_per_sqm < 30 THEN 'Excelente'
    WHEN k.energy_per_sqm < 40 THEN 'Bueno'
    WHEN k.energy_per_sqm < 50 THEN 'Regular'
    ELSE 'Mejorable'
  END as efficiency_category,
  
  -- Costo estimado (asumiendo $0.15/kWh promedio Argentina)
  ROUND(k.total_energy_kwh * 0.15, 2) as estimated_cost_usd

FROM `chillers-478716.IoT_monitoring.kpi_energy_monthly` k
LEFT JOIN `chillers-478716.IoT_monitoring.installation_metadata` m
  ON k.installation_id = m.installation_id;