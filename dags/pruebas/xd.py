import pyodbc

# Configuración de la conexión ODBC
conn_str = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=10.0.4.100;"
    "Database=TABRDRO_RPT;"
    "UID=SoporteITX;"
    "PWD=kSN^3xrW7Mw0;"
)

# Ejecutar la consulta
# Ejecutar el procedimiento almacenado
try:
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        
        # Ejecutar el procedimiento
        cursor.execute("""
            EXEC [TABRDRO_RPT].[dbo].[ETL_BRDRO_ORASRV] 
                @BGN_PRC_DT=?, 
                @END_PRC_DT=?, 
                @BGN_STP_ID=1, 
                @END_STP_ID=1
        """, ['2024-12-12', '2024-12-12'])

        # Obtener resultados
        while True:
            if cursor.description:  # Verificar si hay resultados disponibles
                rows = cursor.fetchall()
                print("Resultados obtenidos:")
                for row in rows:
                    print(row)
            if not cursor.nextset():  # Mover al siguiente conjunto de resultados
                break
except Exception as e:
    print(f"Error: {e}")