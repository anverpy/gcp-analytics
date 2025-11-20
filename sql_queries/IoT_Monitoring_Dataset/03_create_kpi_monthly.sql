-- ============================================================================
-- PASO 3: Crear Tabla de KPIs Agregados Mensuales
-- ============================================================================
-- Propósito: Tabla B del Paper_NLM - KPIs calculados mensualmente
-- KPI principal: Energía consumida por metro cuadrado por mes (kWh/m²/mes)
-- ============================================================================

CREATE TABLE IF NOT EXISTS `chillers-478716.IoT_monitoring.kpi_energy_monthly`
(
  -- Identificadores
  kpi_id STRING NOT NULL OPTIONS(description="ID único del registro KPI"),
  installation_id STRING NOT NULL OPTIONS(description="FK a installation_metadata"),
  
  -- Periodo de reporte
  report_month DATE NOT NULL OPTIONS(description="Primer día del mes reportado (ej: 2024-01-01)"),
  
  -- KPI Principal (del Paper)
  energy_per_sqm FLOAT64 OPTIONS(description="kWh/m²/mes - KPI principal para benchmarking entre sucursales"),
  
  -- Métricas de Energía
  total_energy_kwh FLOAT64 OPTIONS(description="Energía total consumida en el mes (kWh)"),
  avg_power_kw FLOAT64 OPTIONS(description="Potencia promedio del mes (kW)"),
  peak_power_kw FLOAT64 OPTIONS(description="Pico de potencia máxima registrado (kW)"),
  min_power_kw FLOAT64 OPTIONS(description="Potencia mínima registrada (kW)"),
  
  -- Métricas Operativas
  compressor_runtime_hours FLOAT64 OPTIONS(description="Horas totales de operación del compresor"),
  compressor_cycles INT64 OPTIONS(description="Número de arranques del compresor"),
  defrost_cycles_count INT64 OPTIONS(description="Número de ciclos de descongelamiento"),
  
  -- Métricas de Temperatura
  avg_temp_cabinet FLOAT64 OPTIONS(description="Temperatura promedio del gabinete (°C)"),
  temp_deviation_hours FLOAT64 OPTIONS(description="Horas fuera del rango objetivo de temperatura"),
  
  -- Eficiencia Energética
  cop_estimated FLOAT64 OPTIONS(description="Coeficiente de performance estimado (Cooling/Power)"),
  energy_efficiency_ratio FLOAT64 OPTIONS(description="Ratio de eficiencia vs. baseline"),
  
  -- Calidad de Datos
  data_completeness_pct FLOAT64 OPTIONS(description="Porcentaje de datos válidos en el mes (0-100)"),
  alarm_count INT64 OPTIONS(description="Número total de alarmas en el mes"),
  
  -- Auditoría
  calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp de cálculo del KPI"),
  calculation_source STRING DEFAULT 'BigQuery_Scheduled_Query' OPTIONS(description="Sistema que calculó el KPI")
)
PARTITION BY report_month
CLUSTER BY installation_id
OPTIONS(
  description="KPIs mensuales agregados por instalación. Gold Layer - Tabla B del Paper_NLM.",
  labels=[("layer", "gold"), ("frequency", "monthly"), ("data_type", "kpi")]
);

-- ============================================================================
-- Queries de validación
-- ============================================================================

-- Ver estructura
SELECT 
  column_name,
  data_type,
  description
FROM `chillers-478716.IoT_monitoring.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
WHERE table_name = 'kpi_energy_monthly'
ORDER BY column_name;

-- Verificar tabla vacía
SELECT COUNT(*) as total_kpis 
FROM `chillers-478716.IoT_monitoring.kpi_energy_monthly`;
