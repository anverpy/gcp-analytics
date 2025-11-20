# Design Decisions: Paper to BigQuery Data Warehouse

## 🎯 Context
IoT monitoring system for **3 retail refrigeration installations in Argentina**, measuring energy consumption at 1-minute intervals. Implementation follows medallion architecture (Bronze → Silver → Gold layers).

**Target KPI:** kWh/m²/month

**Hardware Stack:**
- Gateway: Siemens SIMATIC IOT2040 (edge computing)
- Controller: Eliwell EWCM9100 (temps, pressures, states)
- Energy Meter: Circutor CVM-MINI (electrical consumption)

---

## 📊 Paper Tables → BigQuery Mapping

### **Table C (Paper) → `installation_metadata`**

| Paper Column | BigQuery Column | Source | Notes |
|---------------|------------------|--------|-------|
| Installation ID | `installation_id` | Manual | Unique identifier (e.g., STORE_ARG_001) |
| Cabinet Type | `cabinet_type` | Manual | fruit/meat/frozen |
| Area (m²) | `square_meters` | Manual | **CRITICAL** for KPI calculation |
| Target Temperature | `target_operation_temp` | Manual | Depends on cabinet type |
| Location | `city`, `country`, `geographic_location` | Manual | Split into 3 columns |
| *(Added)* | `gateway_model`, `controller_model`, `energy_meter_model` | - | Hardware audit trail |
| *(Added)* | `is_active`, `created_at`, `updated_at` | - | Lifecycle management |

---

### **Table A (Paper) → `sensor_measurements`**

#### Identifiers
| Paper Column | BigQuery Column | Source | Notes |
|---------------|------------------|--------|-------|
| Installation ID | `installation_id` | System | FK to installation_metadata |
| Timestamp UTC | `timestamp` | System | Every minute |
| *(Added)* | `measurement_id` | - | UUID to prevent duplicates |

#### Temperatures (from EWCM9100)
| Paper Column | BigQuery Column | Source | Notes |
|---------------|------------------|--------|-------|
| Temperature (°C) | `temp_evaporator` | EWCM9100 | Evaporator |
| - | `temp_suction` | EWCM9100 | Compressor suction |
| - | `temp_discharge` | EWCM9100 | Compressor discharge |
| - | `temp_ambient` | EWCM9100 | Ambient temperature |
| - | `temp_cabinet` | EWCM9100 | Product temperature (most critical) |

#### Pressures (from EWCM9100)
| Paper Column | BigQuery Column | Source | Notes |
|---------------|------------------|--------|-------|
| Pressure | `pressure_suction` | EWCM9100 | Suction pressure (bar) |
| - | `pressure_discharge` | EWCM9100 | Discharge pressure (bar) |

#### Electrical Variables (from CVM-MINI)
| Paper Column | BigQuery Column | Source | Notes |
|---------------|------------------|--------|-------|
| Energy Consumption (kWh) | `energy_consumption_kwh` | CVM-MINI | Cumulative |
| Power (kW) | `power_consumption_kw` | CVM-MINI | Instantaneous |
| Power Factor | `power_factor` | CVM-MINI | 0-1 |
| *(Added)* | `current_phase_a`, `current_phase_b`, `current_phase_c` | CVM-MINI | Three-phase - detect imbalance |
| *(Added)* | `voltage` | CVM-MINI | Voltage (V) |

#### System States (from EWCM9100)
| Paper Column | BigQuery Column | Source | Notes |
|---------------|------------------|--------|-------|
| On/Off Cycle State | `compressor_status` | EWCM9100 | TRUE=running |
| Defrost Cycle Detection | `defrost_cycle_active` | EWCM9100 | Consumes energy without cooling |
| *(Added)* | `door_open` | - | Efficiency loss indicator |

#### Alarms (Added - not in paper but critical)
| BigQuery Column | Rationale |
|------------------|-----------||
| `alarm_high_temp` | Product damage risk |
| `alarm_low_pressure` | Refrigerant leak indicator |
| `alarm_high_pressure` | Compressor damage risk |

#### Data Quality (Added - essential in production)
| BigQuery Column | Values | Rationale |
|------------------|---------|-----------||
| `data_quality` | good/suspect/bad | ~5% of IoT data is problematic |
| `gateway_connection_status` | online/offline/intermittent | Detect connectivity issues |

#### Audit Trail
| BigQuery Column | Rationale |
|------------------|-----------||
| `ingestion_timestamp` | BigQuery arrival time (≠ measurement timestamp) |
| `source_system` | Future-proof for multiple gateways |

---

### **Table B (Paper) → `kpi_energy_monthly`**

| Paper Column | BigQuery Column | Notes |
|---------------|------------------|-------|
| Installation ID | `installation_id` | FK |
| Report Date | `report_month` | First day of month |
| KPI: Energy/m²/Month | `energy_per_sqm` | **Primary KPI** from paper |

---

## ⚙️ BigQuery Optimizations

| Optimization | Implementation | Rationale |
|--------------|----------------|-----------||
| **Partitioning** | `PARTITION BY DATE(timestamp)` | 1.5M rows/year → ~95% cost reduction on time-range queries |
| **Clustering** | `CLUSTER BY installation_id, timestamp` | Co-locate related data, leverage pruning on typical filters |
| **Expiration** | `partition_expiration_days=730` | Automated GDPR compliance, storage cost control |
| **Staging Tables** | `_staging` suffix | Data validation before production load |
| **Materialized Views** | `vw_hourly_metrics` | Pre-aggregated to reduce Power BI query cost (90% reduction) |
| **Column Pruning** | SELECT explicit columns | Avoid `SELECT *` - columnar storage optimization |

---

## 🔄 ETL Best Practices

### **Idempotency**
```sql
-- MERGE instead of INSERT to handle reprocessing
MERGE `project.dataset.sensor_measurements` T
USING staging_data S
ON T.measurement_id = S.measurement_id
WHEN NOT MATCHED THEN INSERT ...
```

### **Data Quality Checks**
```sql
-- Validation before Silver layer promotion
WHERE 
  timestamp BETWEEN '2024-01-01' AND CURRENT_TIMESTAMP()
  AND temp_cabinet BETWEEN -40 AND 10  -- Sanity checks
  AND power_consumption_kw >= 0
  AND data_quality IN ('good', 'suspect')  -- Exclude 'bad'
```

### **Incremental Loads**
```sql
-- Process only new data since last run
WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  AND ingestion_timestamp > (SELECT MAX(last_processed) FROM metadata.etl_watermark)
```

---

## 📐 SQL Design Patterns

### **Window Functions for Anomaly Detection**
```sql
-- Detect temperature spikes
SELECT 
  *,
  AVG(temp_cabinet) OVER(
    PARTITION BY installation_id 
    ORDER BY timestamp 
    ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
  ) AS temp_30min_avg,
  temp_cabinet - temp_30min_avg AS temp_deviation
FROM sensor_measurements
HAVING ABS(temp_deviation) > 5  -- Alert threshold
```

### **CTE for Readability**
```sql
WITH daily_aggregates AS (
  SELECT 
    installation_id,
    DATE(timestamp) AS day,
    SUM(power_consumption_kw) / 60 AS daily_kwh  -- 1-min intervals
  FROM sensor_measurements
  GROUP BY 1, 2
),
installation_metadata AS (
  SELECT installation_id, square_meters FROM installation_metadata
)
SELECT 
  a.installation_id,
  a.daily_kwh / m.square_meters AS energy_per_sqm
FROM daily_aggregates a
JOIN installation_metadata m USING(installation_id)
```

### **Partitioned DML for Large Updates**
```sql
-- Update in chunks to avoid slot quota issues
UPDATE `dataset.kpi_energy_monthly`
SET efficiency_ratio = actual_cop / baseline_cop
WHERE DATE(report_month) = '2025-11-01'  -- Single partition
```

---

## 📝 Summary

| Aspect | Paper NLM | BigQuery Implementation | Delta |
|--------|-----------|------------------------|-------|
| Columns (Table A) | 10 suggested | 23 implemented | +13 for FDD & data quality |
| Timestamps | 1 (measurement) | 2 (measurement + ingestion) | +1 for ETL traceability |
| Storage | Local SQLite | Partitioned BigQuery | Horizontal scalability |
| Data Model | Single table | Medallion (Bronze/Silver/Gold) | Data maturity levels |
| Query Cost | N/A | ~$0.02/query (with clustering) | Cost-optimized |
| ETL Pattern | Batch inserts | MERGE (upserts) | Idempotent pipeline |


