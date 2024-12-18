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
        cursor.execute("""
            EXEC [TABRDRO_RPT].[dbo].[ETL_BRDRO_ORASRV]
            @BGN_PRC_DT=?, 
            @END_PRC_DT=?, 
            @BGN_STP_ID=1, 
            @END_STP_ID=2
        """, ['2024-12-12', '2024-12-12'])

        print("Ejecución iniciada...")
        while True:
            if cursor.description:
                print("Resultados:", cursor.fetchall())
            if not cursor.nextset():
                break
        print("Ejecución finalizada correctamente.")
except Exception as e:
    print(f"Error: {e}")