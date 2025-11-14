import csv
import psycopg2
from openpyxl import Workbook
from datetime import datetime
import logging
from sshtunnel import SSHTunnelForwarder
import os

# --- Configuración de Logs ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIGURACIÓN DEL ARCHIVO CSV ---
CSV_FILE = "roles.csv"  # Columnas esperadas: documento_usuario,nombre_ubicacion,nombre_rol

# --- CONFIGURACIÓN DE CONEXIÓN SSH ---
# Host público del servidor SSH (el que usas para conectarte vía SSH)
SSH_SERVER_HOST = "34.148.129.131"
# Usuario de conexión SSH
SSH_SERVER_USER = "ubuntu"
# Ruta completa a tu clave privada .pem
SSH_PRIVATE_KEY_PATH = "C:\\Users\\Brayan Correa\\Downloads\\key-positiva-prod.pem" 
# Puerto SSH, usualmente 22
SSH_PORT = 22

# --- CONFIGURACIÓN DE LA BASE DE DATOS (ACCESIBLE VÍA EL TÚNEL SSH) ---
# Host interno de PostgreSQL (la IP privada que viste en DBeaver)
DB_HOST = "172.26.0.5"
# Puerto de PostgreSQL
DB_PORT = 5432
DB_NAME = "gcpprolinktic"
DB_USER = "postgres"
DB_PASS = "IMlCXSQOkvbGNG6i"


def get_db_connection(local_port):
    """
    Establece una conexión a la base de datos a través del túnel SSH.
    Se conecta a 'localhost' en el puerto asignado por el túnel.
    """
    return psycopg2.connect(
        host='127.0.0.1',  # Siempre localhost para el túnel
        port=local_port,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def ejecutar_asignacion_y_reporte():
    logger.info("=== Iniciando el proceso de asignación de roles ===")
    
    # 🚨 Se establece el túnel SSH al inicio del proceso 🚨
    try:
        with SSHTunnelForwarder(
            (SSH_SERVER_HOST, SSH_PORT),
            ssh_username=SSH_SERVER_USER,
            ssh_pkey=SSH_PRIVATE_KEY_PATH,
            remote_bind_address=(DB_HOST, DB_PORT)
        ) as tunnel:
            
            local_port = tunnel.local_bind_port
            logger.info(f"✅ Túnel SSH establecido. Conectando a PostgreSQL en 127.0.0.1:{local_port}")

            # Conectarse a la base de datos a través del túnel
            conn = get_db_connection(local_port)
            cursor = conn.cursor()

            # Cargar datos CSV
            try:
                with open(CSV_FILE, newline='', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    filas = [(r['documento_usuario'], r['nombre_ubicacion'], r['nombre_rol']) for r in reader]
                logger.info(f"✅ Se leyeron {len(filas)} filas del archivo CSV.")
            except FileNotFoundError:
                logger.error(f"❌ Error: El archivo CSV '{CSV_FILE}' no se encontró.")
                return # Salir si el archivo no existe
            except KeyError as e:
                logger.error(f"❌ Error en las columnas del CSV. Falta la columna: {e}. Asegúrate de que las columnas sean 'documento_usuario', 'nombre_ubicacion', 'nombre_rol'.")
                return # Salir si las columnas son incorrectas


            # Crear tabla temporal
            cursor.execute("DROP TABLE IF EXISTS tmp_roles_input")
            cursor.execute("""
                CREATE TEMP TABLE tmp_roles_input (
                    documento_usuario TEXT,
                    nombre_ubicacion TEXT,
                    nombre_rol TEXT
                )
            """)
            cursor.executemany("INSERT INTO tmp_roles_input VALUES (%s, %s, %s)", filas)
            logger.info("✅ Tabla temporal 'tmp_roles_input' creada y poblada.")

            # Ejecutar consulta principal para calcular el estado y los IDs
            cursor.execute("""
                WITH datos_crudos AS (
                    SELECT documento_usuario, nombre_ubicacion, nombre_rol 
                    FROM tmp_roles_input
                ),
                datos_calculados AS (
                    SELECT
                        dc.documento_usuario,
                        dc.nombre_ubicacion,
                        dc.nombre_rol,
                        u.id AS usuario_id,
                        r.id AS rol_id,
                        o.id_dependencia AS seccionsubseccion_id_oficina,
                        o.id AS oficina_id,
                        s.idseccionsubseccion AS seccionsubseccion_id_seccion,
                        CASE
                            WHEN o.id IS NOT NULL THEN 'GRUPO_ESPECIAL'
                            ELSE 'SECCION_UNICA'
                        END AS tipo_ubicacion_mapeo
                    FROM datos_crudos dc
                    LEFT JOIN usuarios u ON u.numero_documento = dc.documento_usuario
                    LEFT JOIN roles r ON r.nombre = dc.nombre_rol
                    LEFT JOIN oficina o ON o.nombre = dc.nombre_ubicacion
                    LEFT JOIN seccionsubseccion s ON s.nombre = dc.nombre_ubicacion
                ),
                inserciones_validas AS (
                    SELECT
                        dc.*,
                        -- Definir IDs finales
                        CASE 
                            WHEN dc.oficina_id IS NOT NULL THEN dc.seccionsubseccion_id_oficina
                            ELSE dc.seccionsubseccion_id_seccion
                        END AS seccionsubseccion_id_final,
                        CASE 
                            WHEN dc.tipo_ubicacion_mapeo = 'SECCION_UNICA' THEN NULL
                            ELSE dc.oficina_id
                        END AS oficina_id_final,
                        -- Lógica de estado
                        CASE
                            WHEN dc.usuario_id IS NULL THEN 'ERROR - USUARIO NO ENCONTRADO'
                            WHEN dc.rol_id IS NULL THEN 'ERROR - ROL NO ENCONTRADO'
                            WHEN (dc.tipo_ubicacion_mapeo = 'GRUPO_ESPECIAL' AND (dc.oficina_id IS NULL OR dc.seccionsubseccion_id_oficina IS NULL)) THEN 'ERROR - UBICACIÓN GRUPO INVALIDA'
                            WHEN (dc.tipo_ubicacion_mapeo = 'SECCION_UNICA' AND dc.seccionsubseccion_id_seccion IS NULL) THEN 'ERROR - UBICACIÓN SECCIÓN INVALIDA'
                            WHEN EXISTS (
                                SELECT 1 FROM usuario_relacion ur
                                WHERE ur.usuario_id = dc.usuario_id
                                    AND ur.rol_id = dc.rol_id
                                    AND ur.oficina_id IS NOT DISTINCT FROM (
                                        CASE WHEN dc.tipo_ubicacion_mapeo = 'SECCION_UNICA' THEN NULL ELSE dc.oficina_id END
                                    )
                                    AND ur.seccionsubseccion_id IS NOT DISTINCT FROM (
                                        CASE 
                                            WHEN dc.tipo_ubicacion_mapeo = 'GRUPO_ESPECIAL' THEN dc.seccionsubseccion_id_oficina
                                            ELSE dc.seccionsubseccion_id_seccion
                                        END
                                    )
                            ) THEN 'YA EXISTE'
                            ELSE 'CREADO'
                        END AS estado
                    FROM datos_calculados dc
                )
                SELECT * FROM inserciones_validas
            """)

            resultados = cursor.fetchall()
            columnas = [desc[0] for desc in cursor.description]
            logger.info("✅ Consulta principal ejecutada y resultados obtenidos.")

            # Insertar solo los que son 'CREADO'
            registros_a_insertar = [r for r in resultados if r[columnas.index("estado")] == "CREADO"]
            if registros_a_insertar:
                insert_query = """
                    INSERT INTO usuario_relacion (usuario_id, seccionsubseccion_id, oficina_id, rol_id, punto_radicacion_id)
                    VALUES (%s, %s, %s, %s, NULL)
                """
                insert_data = [(r[columnas.index("usuario_id")],
                                r[columnas.index("seccionsubseccion_id_final")],
                                r[columnas.index("oficina_id_final")],
                                r[columnas.index("rol_id")]) for r in registros_a_insertar]
                cursor.executemany(insert_query, insert_data)
                conn.commit()
                logger.info(f"✅ Se insertaron {len(registros_a_insertar)} nuevos registros en 'usuario_relacion'.")
            else:
                logger.info("ℹ️ No se encontraron nuevos registros para insertar en 'usuario_relacion'.")

            # Exportar a Excel
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
            output_filename = f"roles_asignados_{timestamp}.xlsx"
            wb = Workbook()
            ws = wb.active
            ws.title = "Resultado Asignacion Roles"
            ws.append(columnas)
            for row in resultados:
                ws.append(row)
            wb.save(output_filename)
            logger.info(f"📄 Reporte generado: {output_filename}")

            cursor.close()
            conn.close()

    except FileNotFoundError:
        logger.error(f"❌ Error: El archivo de clave privada SSH '{SSH_PRIVATE_KEY_PATH}' no se encontró. Verifica la ruta.")
    except Exception as e:
        logger.error(f"❌ Error en el proceso principal de asignación de roles: {e}", exc_info=True)
    finally:
        logger.info("=== Proceso de asignación de roles finalizado ===")

# EJECUTAR
if __name__ == "__main__":
    ejecutar_asignacion_y_reporte()