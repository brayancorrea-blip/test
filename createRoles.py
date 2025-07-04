import csv
import psycopg2
from openpyxl import Workbook
from datetime import datetime

# CONFIGURACIÓN
CSV_FILE = "roles.csv"  # Columnas: documento_usuario,nombre_ubicacion,nombre_rol
DB_CONFIG = {
    "host": "34.148.129.131",
    "database": "gcpprolinktic",
    "user": "postgres",
    "password": "IMlCXSQOkvbGNG6i"
}

def ejecutar_asignacion_y_reporte():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Cargar datos CSV
        with open(CSV_FILE, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            filas = [(r['documento_usuario'], r['nombre_ubicacion'], r['nombre_rol']) for r in reader]

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

        # Ejecutar consulta principal
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

        # Exportar Excel
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        archivo = f"roles_asignados_{timestamp}.xlsx"
        wb = Workbook()
        ws = wb.active
        ws.title = "Resultado"
        ws.append(columnas)
        for row in resultados:
            ws.append(row)
        wb.save(archivo)
        print(f"[✅] Exportado: {archivo}")

        cursor.close()
        conn.close()

    except psycopg2.OperationalError as e:
        if "remaining connection slots" in str(e):
            print("❌ Lotes superados en la base de datos")
        else:
            print(f"❌ Error de conexión: {e}")
    except Exception as ex:
        print(f"❌ Error general: {ex}")

# EJECUTAR
if __name__ == "__main__":
    ejecutar_asignacion_y_reporte()
