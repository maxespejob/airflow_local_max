import pandas as pd
from sqlalchemy import create_engine
import pyodbc  # Para la conexión a SQL Server
import psycopg2  # Para la conexión a PostgreSQL

# Paso 1: Configuración de la conexión a SQL Server
sql_server = '10.0.4.100'  # Reemplaza con el nombre de tu servidor
sql_database = 'TARBC'  # Reemplaza con el nombre de tu base de datos
sql_username = 'SoporteITX'  # Reemplaza con tu usuario
sql_password = 'kSN^3xrW7Mw0'  # Reemplaza con tu contraseña
sql_table_name = 'REPORT_TRANSACTION_RBC_202401_202403'  # Reemplaza con el nombre de la tabla que deseas exportar

# Cadena de conexión a SQL Server
sql_connection_string = f'mssql+pyodbc://{sql_username}:{sql_password}@{sql_server}/{sql_database}?driver=ODBC+Driver+17+for+SQL+Server'

# Crear una conexión usando SQLAlchemy
sql_engine = create_engine(sql_connection_string)

# Paso 2: Leer los datos desde SQL Server
query = f'SELECT * FROM {sql_table_name}'
df = pd.read_sql(query, sql_engine)

print(f"Datos exportados exitosamente de SQL Server a PostgreSQL en el dataframe")

# Paso 3: Configuración de la conexión a PostgreSQL
pg_host = 'app-interchange-analytics-rds-psql-prd.cf3zxr6zcsiz.us-east-1.rds.amazonaws.com'  # Dirección de tu servidor PostgreSQL
pg_port = '5432'  # Puerto de PostgreSQL
pg_database = 'interchange_analytics'  # Base de datos de PostgreSQL
pg_username = 'root'  # Usuario de PostgreSQL
pg_password = '9?4.WHisG|E<WZmV4_{v-f$vBS)B'  # Contraseña de PostgreSQL
pg_table_name = 'report_transaction_rbc_202401_202403'  # Nombre de la tabla en PostgreSQL

# Cadena de conexión a PostgreSQL
pg_connection_string = f'postgresql://{pg_username}:{pg_password}@{pg_host}:{pg_port}/{pg_database}'

# Crear una conexión usando SQLAlchemy
pg_engine = create_engine(pg_connection_string)

# Paso 4: Migrar los datos a PostgreSQL
df.to_sql(pg_table_name, pg_engine, if_exists='replace', index=False)

print(f"Datos migrados exitosamente de SQL Server a PostgreSQL en la tabla {pg_table_name}")
