#!/bin/bash
# Script rápido para validar queries PBID antes de desplegar a Power BI
# Uso: ./quick_validate.sh

set -e

echo "🔍 Validando queries de Power BI..."
echo "===================================="
echo ""

# Validar todas las queries PBID
python scripts/validate_bigquery_queries.py --query-dir sql_queries/PBID/

# Capturar el exit code
EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ ¡Todas las queries son válidas!"
    echo "   Puedes desplegarlas a Power BI con seguridad."
    echo ""
    echo "📋 Siguiente paso:"
    echo "   cd sql_queries/PBID"
    echo "   bq query --use_legacy_sql=false < 01_views_hourly_metrics.sql"
    echo "   bq query --use_legacy_sql=false < 02_views_kpis_with_metadata.sql"
    echo "   bq query --use_legacy_sql=false < 03_views_benchmarking.sql"
    echo "   bq query --use_legacy_sql=false < 04_views_alarm_analysis.sql"
else
    echo "❌ Hay errores de sintaxis en las queries."
    echo "   Por favor corrige los errores antes de desplegar."
    echo ""
    echo "💡 Ayuda:"
    echo "   - Revisa los mensajes de error arriba"
    echo "   - Verifica nombres de columnas y tablas"
    echo "   - Asegúrate que los JOINs estén completos"
    echo "   - Consulta scripts/README_VALIDATOR.md para más info"
fi

exit $EXIT_CODE
