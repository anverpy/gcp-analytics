# Del Paper NLM a BigQuery - Mapeo de Columnas

## 🎯 Contexto
Sistema IoT en **3 supermercados en Argentina** que mide consumo energético de refrigeración cada minuto.

**KPI objetivo:** kWh/m²/mes

**Hardware:**
- Gateway: Siemens SIMATIC IOT2040
- Controlador: Eliwell EWCM9100 (temperaturas, presiones, estados)
- Medidor: Circutor CVM-MINI (consumo eléctrico)

---

## 📊 Tablas del Paper → BigQuery

## 📊 Tablas del Paper → BigQuery

### **Tabla C (Paper) → `installation_metadata`**

| Columna Paper | Columna BigQuery | Fuente | Notas |
|---------------|------------------|--------|-------|
| ID Instalación | `installation_id` | Manual | Identificador único (ej: STORE_ARG_001) |
| Tipo de Gabinete | `cabinet_type` | Manual | fruit/meat/frozen |
| Área (m²) | `square_meters` | Manual | **CRÍTICO** para KPI |
| Temperatura Objetivo | `target_operation_temp` | Manual | Depende del tipo de gabinete |
| Ubicación | `city`, `country`, `geographic_location` | Manual | Separado en 3 columnas |
| *(Añadido)* | `gateway_model`, `controller_model`, `energy_meter_model` | - | Auditoría de hardware |
| *(Añadido)* | `is_active`, `created_at`, `updated_at` | - | Gestión y trazabilidad |

---

### **Tabla A (Paper) → `sensor_measurements`**

#### Identificadores
| Columna Paper | Columna BigQuery | Fuente | Notas |
|---------------|------------------|--------|-------|
| ID Instalación | `installation_id` | Sistema | FK a installation_metadata |
| Timestamp UTC | `timestamp` | Sistema | Cada minuto |
| *(Añadido)* | `measurement_id` | - | UUID para evitar duplicados |

#### Temperaturas (del EWCM9100)
| Columna Paper | Columna BigQuery | Fuente | Notas |
|---------------|------------------|--------|-------|
| Temperatura (°C) | `temp_evaporator` | EWCM9100 | Evaporador |
| - | `temp_suction` | EWCM9100 | Succión del compresor |
| - | `temp_discharge` | EWCM9100 | Descarga del compresor |
| - | `temp_ambient` | EWCM9100 | Temperatura exterior |
| - | `temp_cabinet` | EWCM9100 | Temperatura del producto (la más importante) |

#### Presiones (del EWCM9100)
| Columna Paper | Columna BigQuery | Fuente | Notas |
|---------------|------------------|--------|-------|
| Presión | `pressure_suction` | EWCM9100 | Presión de succión (bar) |
| - | `pressure_discharge` | EWCM9100 | Presión de descarga (bar) |

#### Variables Eléctricas (del CVM-MINI)
| Columna Paper | Columna BigQuery | Fuente | Notas |
|---------------|------------------|--------|-------|
| Consumo de Energía (kWh) | `energy_consumption_kwh` | CVM-MINI | Acumulado |
| Potencia Eléctrica (kW) | `power_consumption_kw` | CVM-MINI | Instantáneo |
| Factor de Potencia | `power_factor` | CVM-MINI | 0-1 |
| *(Añadido)* | `current_phase_a`, `current_phase_b`, `current_phase_c` | CVM-MINI | Trifásico - detectar desbalance |
| *(Añadido)* | `voltage` | CVM-MINI | Voltaje (V) |

#### Estados del Sistema (del EWCM9100)
| Columna Paper | Columna BigQuery | Fuente | Notas |
|---------------|------------------|--------|-------|
| Estado Ciclo On/Off | `compressor_status` | EWCM9100 | TRUE=encendido |
| Detección Ciclo Descongelación | `defrost_cycle_active` | EWCM9100 | Consume energía sin enfriar |
| *(Añadido)* | `door_open` | - | Pérdida de eficiencia |

#### Alarmas (Añadidas - no en paper pero críticas)
| Columna BigQuery | Por qué |
|------------------|---------|
| `alarm_high_temp` | Producto se puede dañar |
| `alarm_low_pressure` | Indica fuga de refrigerante |
| `alarm_high_pressure` | Riesgo de daño al compresor |

#### Calidad de Datos (Añadidas - esenciales en producción)
| Columna BigQuery | Valores | Por qué |
|------------------|---------|---------|
| `data_quality` | good/suspect/bad | ~5% de datos IoT son problemáticos |
| `gateway_connection_status` | online/offline/intermittent | Detectar problemas de conectividad |

#### Auditoría
| Columna BigQuery | Por qué |
|------------------|---------|
| `ingestion_timestamp` | Timestamp de llegada a BigQuery (≠ timestamp de medición) |
| `source_system` | Si en el futuro hay múltiples gateways |

---

### **Tabla B (Paper) → `kpi_energy_monthly`** (Pendiente)

| Columna Paper | Columna BigQuery | Notas |
|---------------|------------------|-------|
| ID Instalación | `installation_id` | FK |
| Fecha de Reporte | `report_month` | Primer día del mes |
| KPI: Energía/m²/Mes | `energy_per_sqm` | **KPI principal** del paper |

---

## ⚙️ Optimizaciones BigQuery

| Optimización | Implementación | Por qué |
|--------------|----------------|---------|
| **Particionamiento** | `PARTITION BY DATE(timestamp)` | 1.5M registros/año → Queries más baratas |
| **Clustering** | `CLUSTER BY installation_id, timestamp` | Queries típicas filtran por instalación + fecha |
| **Expiración** | `partition_expiration_days=730` | Auto-borrado de datos > 2 años |
| **Staging** | `sensor_measurements_staging` | Validar datos antes de insertar en producción |

---

## 📝 Resumen

| Aspecto | Paper NLM | BigQuery | Cambio |
|---------|-----------|----------|--------|
| Columnas Tabla A | 10 sugeridas | 23 implementadas | +13 para FDD y calidad de datos |
| Timestamps | 1 | 2 | +1 para trazabilidad ETL |
| Almacenamiento | SQLite local | BigQuery particionado | Escalabilidad |

**Dataset:** `chillers-478716.IoT_monitoring`  
**Fecha:** 19 Nov 2025
