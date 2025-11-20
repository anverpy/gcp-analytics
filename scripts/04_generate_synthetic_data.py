#!/usr/bin/env python3
"""
Generador de Datos Sintéticos para IoT Monitoring Dataset
Genera datos realistas de sensores de refrigeración para 3 tiendas
"""

import polars as pl
import numpy as np
from datetime import datetime, timedelta
from google.cloud import bigquery
import uuid

# Configuración
PROJECT_ID = "chillers-478716"
DATASET_ID = "IoT_monitoring"
TABLE_ID = "sensor_measurements"

# Parámetros de simulación
NUM_DAYS = 7
FREQ_MINUTES = 1
RECORDS_PER_STORE = NUM_DAYS * 24 * 60  # 10,080 registros por tienda

# Configuración de instalaciones (desde installation_metadata)
STORES = [
    {"id": "STORE_ARG_001", "type": "fruit", "target_temp": 4.0},
    {"id": "STORE_ARG_002", "type": "meat", "target_temp": 2.0},
    {"id": "STORE_ARG_003", "type": "frozen", "target_temp": -18.0},
]

def generate_timestamps(num_records, start_date="2024-01-01"):
    """Generar timestamps cada minuto"""
    start = datetime.fromisoformat(start_date)
    return [start + timedelta(minutes=i) for i in range(num_records)]

def generate_temperatures(store_type, num_records):
    """Generar temperaturas realistas con variación diurna"""
    base_temps = {
        "fruit": {"evap": 3.0, "suction": 6.0, "cabinet": 5.0},
        "meat": {"evap": 1.0, "suction": 4.0, "cabinet": 2.5},
        "frozen": {"evap": -18.0, "suction": -12.0, "cabinet": -18.0},
    }
    
    base = base_temps[store_type]
    
    # Crear variación diurna (ciclo de 24h)
    hours = np.arange(num_records) / 60
    diurnal_variation = 1.5 * np.sin(2 * np.pi * hours / 24)
    
    # Añadir ruido gaussiano
    noise = np.random.normal(0, 0.5, num_records)
    
    return {
        "temp_evaporator": base["evap"] + diurnal_variation + noise,
        "temp_suction": base["suction"] + diurnal_variation * 0.8 + noise,
        "temp_discharge": 75.0 + diurnal_variation * 5 + noise * 3,
        "temp_ambient": 20.0 + diurnal_variation * 5 + noise * 2,
        "temp_cabinet": base["cabinet"] + diurnal_variation * 0.6 + noise * 0.3,
    }

def generate_pressures(num_records):
    """Generar presiones con ruido"""
    return {
        "pressure_suction": 2.5 + np.random.normal(0, 0.3, num_records),
        "pressure_discharge": 13.5 + np.random.normal(0, 0.8, num_records),
    }

def generate_power(store_type, num_records, compressor_status):
    """Generar consumo eléctrico basado en estado del compresor"""
    base_power = {
        "fruit": 4.0,
        "meat": 5.0,
        "frozen": 10.0,
    }
    
    power = np.ones(num_records) * base_power[store_type]
    power = power * compressor_status  # 0 cuando está OFF
    power = power + np.random.normal(0, 0.5, num_records)  # Ruido
    
    # Energía acumulada (kWh) = potencia × tiempo
    energy = np.cumsum(power / 60)  # kW × (1min / 60min) = kWh
    
    return {
        "power_consumption_kw": np.maximum(power, 0),
        "energy_consumption_kwh": energy,
    }

def generate_currents(power_kw, voltage=220):
    """Generar corrientes trifásicas balanceadas"""
    # I = P / (√3 × V × PF)
    pf = 0.88
    current_total = power_kw * 1000 / (np.sqrt(3) * voltage * pf)
    
    # Distribuir en 3 fases con pequeño desbalance
    phase_a = current_total / 3 + np.random.normal(0, 0.5, len(power_kw))
    phase_b = current_total / 3 + np.random.normal(0, 0.5, len(power_kw))
    phase_c = current_total / 3 + np.random.normal(0, 0.5, len(power_kw))
    
    return {
        "current_phase_a": np.maximum(phase_a, 0),
        "current_phase_b": np.maximum(phase_b, 0),
        "current_phase_c": np.maximum(phase_c, 0),
    }

def generate_compressor_cycles(num_records):
    """Generar ciclos ON/OFF del compresor (95% uptime)"""
    # Compresor ON la mayoría del tiempo
    status = np.random.rand(num_records) > 0.05
    return status

def generate_defrost_cycles(num_records):
    """Ciclo de descongelamiento cada 6 horas (20 minutos de duración)"""
    defrost = np.zeros(num_records, dtype=bool)
    for i in range(0, num_records, 360):  # Cada 6h = 360 min
        defrost[i:min(i+20, num_records)] = True  # 20 min de defrost
    return defrost

def generate_alarms(temp_cabinet, target_temp, pressure_suction):
    """Generar alarmas basadas en condiciones"""
    num_records = len(temp_cabinet)
    
    # Alarma si temperatura > target + 5°C
    alarm_high_temp = np.abs(temp_cabinet - target_temp) > 5.0
    
    # Alarma si presión muy baja
    alarm_low_pressure = pressure_suction < 1.5
    
    # Alarma si presión muy alta
    alarm_high_pressure = pressure_suction > 4.5
    
    return {
        "alarm_high_temp": alarm_high_temp,
        "alarm_low_pressure": alarm_low_pressure,
        "alarm_high_pressure": alarm_high_pressure,
    }

def generate_data_for_store(store_config):
    """Generar dataset completo para una tienda"""
    print(f"  Generando datos para {store_config['id']}...")
    
    num_records = RECORDS_PER_STORE
    
    # Timestamps
    timestamps = generate_timestamps(num_records)
    
    # Estados del sistema
    compressor_status = generate_compressor_cycles(num_records)
    defrost_cycles = generate_defrost_cycles(num_records)
    
    # Temperaturas
    temps = generate_temperatures(store_config["type"], num_records)
    
    # Presiones
    pressures = generate_pressures(num_records)
    
    # Consumo eléctrico
    power = generate_power(store_config["type"], num_records, compressor_status)
    
    # Corrientes
    currents = generate_currents(power["power_consumption_kw"])
    
    # Alarmas
    alarms = generate_alarms(
        temps["temp_cabinet"],
        store_config["target_temp"],
        pressures["pressure_suction"]
    )
    
    # Crear DataFrame
    df = pl.DataFrame({
        "measurement_id": [str(uuid.uuid4()) for _ in range(num_records)],
        "installation_id": [store_config["id"]] * num_records,
        "timestamp": timestamps,
        
        # Temperaturas
        "temp_evaporator": temps["temp_evaporator"],
        "temp_suction": temps["temp_suction"],
        "temp_discharge": temps["temp_discharge"],
        "temp_ambient": temps["temp_ambient"],
        "temp_cabinet": temps["temp_cabinet"],
        
        # Presiones
        "pressure_suction": pressures["pressure_suction"],
        "pressure_discharge": pressures["pressure_discharge"],
        
        # Consumo eléctrico
        "power_consumption_kw": power["power_consumption_kw"],
        "energy_consumption_kwh": power["energy_consumption_kwh"],
        
        # Corrientes
        "current_phase_a": currents["current_phase_a"],
        "current_phase_b": currents["current_phase_b"],
        "current_phase_c": currents["current_phase_c"],
        
        # Voltaje y factor de potencia
        "voltage": 220.0 + np.random.normal(0, 3, num_records),
        "power_factor": 0.88 + np.random.normal(0, 0.03, num_records),
        
        # Estados
        "compressor_status": compressor_status,
        "defrost_cycle_active": defrost_cycles,
        "door_open": np.random.rand(num_records) < 0.02,  # 2% del tiempo
        
        # Alarmas
        "alarm_high_temp": alarms["alarm_high_temp"],
        "alarm_low_pressure": alarms["alarm_low_pressure"],
        "alarm_high_pressure": alarms["alarm_high_pressure"],
        
        # Calidad de datos
        "data_quality": np.random.choice(
            ["good", "suspect", "bad"],
            num_records,
            p=[0.98, 0.015, 0.005]
        ),
        "gateway_connection_status": ["online"] * num_records,
        
        # Auditoría
        "ingestion_timestamp": [datetime.now()] * num_records,
        "source_system": ["Polars_Synthetic_Generator"] * num_records,
    })
    
    print(f"    ✓ {len(df):,} registros generados")
    return df

def upload_to_bigquery(df, table_id):
    """Subir DataFrame a BigQuery"""
    print(f"\n📤 Subiendo {len(df):,} registros a BigQuery...")
    
    client = bigquery.Client(project=PROJECT_ID)
    full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    
    # Configurar job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",  # Añadir a datos existentes
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ],
    )
    
    # Convertir a Pandas (BigQuery Python client lo requiere)
    df_pandas = df.to_pandas()
    
    # Subir
    job = client.load_table_from_dataframe(
        df_pandas,
        full_table_id,
        job_config=job_config
    )
    
    job.result()  # Esperar a que termine
    
    print(f"✅ Datos subidos exitosamente a {full_table_id}")
    
    # Verificar
    table = client.get_table(full_table_id)
    print(f"   Total filas en tabla: {table.num_rows:,}")

def main():
    """Generar datos para todas las tiendas y subir a BigQuery"""
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║  Generador de Datos Sintéticos - IoT Monitoring             ║
    ║  Dataset: chillers-478716.IoT_monitoring                     ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    all_data = []
    
    # Generar datos por tienda
    for store in STORES:
        df = generate_data_for_store(store)
        all_data.append(df)
    
    # Combinar todas las tiendas
    df_combined = pl.concat(all_data)
    
    print(f"\n📊 Resumen de datos generados:")
    print(f"   Total registros: {len(df_combined):,}")
    print(f"   Periodo: {NUM_DAYS} días")
    print(f"   Tiendas: {len(STORES)}")
    
    # Estadísticas rápidas
    print(f"\n📈 Estadísticas:")
    stats = df_combined.select([
        pl.col("power_consumption_kw").mean().alias("avg_power_kw"),
        pl.col("temp_cabinet").mean().alias("avg_temp_cabinet"),
        pl.col("compressor_status").cast(pl.Int8).mean().mul(100).alias("compressor_uptime_pct"),
        pl.col("alarm_high_temp").cast(pl.Int8).sum().alias("total_alarms_temp"),
    ])
    print(stats)
    
    # Subir a BigQuery
    upload_to_bigquery(df_combined, TABLE_ID)
    
    print(f"\n✅ Proceso completado!")

if __name__ == "__main__":
    main()
