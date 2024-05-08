import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

REDSHIFT_HOST =  os.getenv('REDSHIFT_HOST')
REDSHIFT_PORT = os.getenv('REDSHIFT_PORT')
REDSHIFT_DATABASE = os.getenv('REDSHIFT_DATABASE')
REDSHIFT_USER = os.getenv('REDSHIFT_USER')
REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')

def createConnection():
    try:
        conn = psycopg2.connect(
            host=REDSHIFT_HOST,
            dbname=REDSHIFT_DATABASE,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD,
            port=REDSHIFT_PORT
        )

        print("- conectado a redshift")

        return conn
    except Exception as e:
        print(e)
        print("- no se pudo conectar a redshift")
        return None
    
def dropTable(conn: psycopg2.extensions.connection, tableName: str) -> bool:
    try:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {tableName}")
        conn.commit()
        cur.close()
        print(f"- tabla {tableName} eliminada")
        return True
    except Exception as e:
        print(e)
        print(f"- no se pudo eliminar la tabla {tableName}")
        return False
    
def createProjectsTable(conn: psycopg2.extensions.connection) -> bool:
    try:
        cur = conn.cursor()
        query = """
        CREATE TABLE IF NOT EXISTS projects (
            project_id INT NOT NULL,
            acronym VARCHAR(50),
            title VARCHAR(250),
            description VARCHAR(MAX),
            start_year VARCHAR(4),
            start_month VARCHAR(2),
            end_year VARCHAR(4),
            end_month VARCHAR(2),
            status_description VARCHAR(20),
            website VARCHAR(250),
            last_updated DATE,
            release_status_string VARCHAR(20),
            organization_id INT,
            organization_name VARCHAR(250),
            organization_type_pretty VARCHAR(250),
            etl_load DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """

        cur.execute(query)
        conn.commit()
        cur.close()
        print("- tabla projects creada")
        return True
    except Exception as e:
        print(e)
        print("- no se pudo crear la tabla projects")
        return False
    
def insertDataInProjectsTable(conn: psycopg2.extensions.connection, data) -> bool:
    try:
        cur = conn.cursor()
        execute_values(
            cur,
            """
            INSERT INTO projects (
                project_id,
                acronym,
                title,
                description,
                start_year,
                start_month,
                end_year,
                end_month,
                status_description,
                website,
                last_updated,
                release_status_string,
                organization_id,
                organization_name,
                organization_type_pretty
            ) VALUES %s
            """,
            [tuple(row) for row in data.values],
            page_size=len(data)
        )

        conn.commit()
        cur.close()
        print("- datos insertados en la tabla projects")
        return True
    except Exception as e:
        print(e)
        print("- no se pudo insertar los datos en la tabla projects")
        return False
    
def loadDataIntoRedshift(data) -> bool:
    try:
        conn = createConnection()
        if conn is None:
            return False
        
        dropTable(conn, "projects")
        createProjectsTable(conn)
        insertDataInProjectsTable(conn, data)
        conn.close()
        return True
    except Exception as e:
        print(e)
        return False