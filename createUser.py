import csv
import psycopg2
from openpyxl import Workbook
from datetime import datetime
import requests
import logging
from concurrent.futures import ThreadPoolExecutor

# --- Configuración de Logs ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuración de Keycloak ---
KEYCLOAK_URL = "https://sgdea-prod.proyectos-3t.tech"
REALM_NAME = "positiva"
CLIENT_ID = "cliente-registrar"
CLIENT_SECRET = "kmgmRbXYwvoxlwmo461B7kUGRbl1zv8C"

# --- Configuración de Bonita ---
BONITA_URL = "https://sgdea-prod.proyectos-3t.tech"
BONITA_ADMIN_USER = "tech_user_prod"
BONITA_ADMIN_PASS = "nvq8cYzodOav6O"

# --- Configuración General ---
DEFAULT_PASSWORD = "Sgdea2025*"
MAX_THREADS = 5

# Funciones auxiliares
def to_upper_preserve_accents(text):
    return text.upper()

def map_user_type(usertype_raw):
    tipo = usertype_raw.strip().upper()
    if tipo == "PROVEEDOR":
        return "Proveedor"
    elif tipo == "FUNCIONARIO":
        return "Interno"
    return tipo.capitalize()

# --- Funciones de Keycloak ---
def obtener_token_admin():
    url = f"{KEYCLOAK_URL}/realms/{REALM_NAME}/protocol/openid-connect/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    
    resp = requests.post(url, headers=headers, data=data)
    resp.raise_for_status()
    token_data = resp.json()
    return token_data.get("access_token")

def crear_usuario_keycloak(token, user_data):
    try:
        # Verificar si el usuario existe
        url = f"{KEYCLOAK_URL}/admin/realms/{REALM_NAME}/users?username={user_data['username']}"
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        usuarios = resp.json()

        if usuarios:
            # Actualizar contraseña si el usuario existe
            user_id = usuarios[0]["id"]
            url = f"{KEYCLOAK_URL}/admin/realms/{REALM_NAME}/users/{user_id}/reset-password"
            payload = {
                "type": "password",
                "value": DEFAULT_PASSWORD,
                "temporary": False
            }
            headers["Content-Type"] = "application/json"
            resp = requests.put(url, headers=headers, json=payload)
            resp.raise_for_status()
            logger.info(f"[Keycloak] Contraseña actualizada para usuario: {user_data['username']}")
        else:
            # Crear nuevo usuario
            url = f"{KEYCLOAK_URL}/admin/realms/{REALM_NAME}/users"
            payload = {
                "username": user_data["username"],
                "firstName": user_data["firstname"],
                "lastName": user_data["lastname"],
                "enabled": True,
                "credentials": [{
                    "type": "password",
                    "value": DEFAULT_PASSWORD,
                    "temporary": False
                }]
            }
            headers["Content-Type"] = "application/json"
            resp = requests.post(url, headers=headers, json=payload)
            resp.raise_for_status()
            logger.info(f"[Keycloak] Usuario creado: {user_data['username']}")
        
        return True
    except Exception as e:
        logger.error(f"[Keycloak] Error procesando usuario {user_data['username']}: {e}")
        return False

# --- Funciones de Bonita ---
def iniciar_sesion_bonita():
    session = requests.Session()
    login_url = f"{BONITA_URL}/bonita/loginservice"
    data = {
        "username": BONITA_ADMIN_USER,
        "password": BONITA_ADMIN_PASS,
        "redirect": "false"
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    
    resp = session.post(login_url, data=data, headers=headers)
    resp.raise_for_status()
    
    if resp.status_code == 204:
        api_token = session.cookies.get("X-Bonita-API-Token")
        session.headers.update({
            "X-Bonita-API-Token": api_token,
            "Content-Type": "application/json"
        })
        return session
    raise Exception("Error al iniciar sesión en Bonita")

def crear_usuario_bonita(session, user_data):
    try:
        # Verificar si el usuario existe
        url = f"{BONITA_URL}/bonita/API/identity/user?p=0&c=1&f=userName={user_data['username']}"
        resp = session.get(url)
        resp.raise_for_status()
        usuarios = resp.json()

        if usuarios:
            # Actualizar usuario existente
            user_id = usuarios[0]["id"]
            url = f"{BONITA_URL}/bonita/API/identity/user/{user_id}"
            payload = {
                "firstname": user_data["firstname"],
                "lastname": user_data["lastname"],
                "password": DEFAULT_PASSWORD,
                "enabled": "true"
            }
            resp = session.put(url, json=payload)
            resp.raise_for_status()
            logger.info(f"[Bonita] Usuario actualizado: {user_data['username']}")
        else:
            # Crear nuevo usuario
            url = f"{BONITA_URL}/bonita/API/identity/user"
            payload = {
                "userName": user_data["username"],
                "firstname": user_data["firstname"],
                "lastname": user_data["lastname"],
                "password": DEFAULT_PASSWORD,
                "enabled": "true"
            }
            resp = session.post(url, json=payload)
            resp.raise_for_status()
            logger.info(f"[Bonita] Usuario creado: {user_data['username']}")
        
        return True
    except Exception as e:
        logger.error(f"[Bonita] Error procesando usuario {user_data['username']}: {e}")
        return False

# Conexión a PostgreSQL
conn = psycopg2.connect(
    host="34.148.129.131",
    database="gcpprolinktic",
    user="postgres",
    password="IMlCXSQOkvbGNG6i"
)
cursor = conn.cursor()

# Obtener cargos disponibles
cursor.execute("SELECT nombre, id FROM cargos")
cargos_dict = {nombre.upper(): id_ for nombre, id_ in cursor.fetchall()}

usuarios_a_insertar = []
usuarios_excel = []

# Inicializar sesiones de Keycloak y Bonita
try:
    keycloak_token = obtener_token_admin()
    bonita_session = iniciar_sesion_bonita()
    logger.info("Conexiones establecidas")
except Exception as e:
    logger.error(f"Error de conexión: {e}")
    cursor.close()
    conn.close()
    raise

# Función para procesar un usuario individual
def procesar_usuario_individual(row, keycloak_token, bonita_session, cargos_dict):
    try:
        cargo_nombre = to_upper_preserve_accents(row['charge'].strip())
        user_type = map_user_type(row['usertype'])
        nombre_completo = f"{row['name']} {row['lastName']}"
        documento = row['document']
        estado = "Procesado"

        # Validar cargo
        if cargo_nombre not in cargos_dict:
            logger.warning(f"Cargo no válido: {cargo_nombre} - Usuario: {documento}")
            return (nombre_completo, documento, "Cargo no válido", "No procesado", "No procesado", None)

        # Preparar datos de usuario
        user_data = {
            "username": documento,
            "firstname": row['name'],
            "lastname": row['lastName'],
            "password": DEFAULT_PASSWORD
        }

        # Procesar usuario en Keycloak y Bonita
        keycloak_success = crear_usuario_keycloak(keycloak_token, user_data)
        bonita_success = crear_usuario_bonita(bonita_session, user_data)

        estado_keycloak = "Éxito" if keycloak_success else "Error"
        estado_bonita = "Éxito" if bonita_success else "Error"

        if not keycloak_success or not bonita_success:
            estado = "Error en servicios"

        # Preparar datos para inserción en BD
        usuario = None
        if not usuario_existe_en_bd(documento):
            usuario = (
                documento,
                row['name'],
                row['lastName'],
                True,
                row['email'],
                documento,
                1,
                user_type,
                3,
                "Creación de usuario",
                6,
                149,
                cargos_dict[cargo_nombre],
                4
            )
            estado = "Creado en BD" if estado == "Procesado" else estado
        else:
            estado = "Actualizado en servicios" if estado == "Procesado" else estado

        return (nombre_completo, documento, estado, estado_keycloak, estado_bonita, usuario)

    except Exception as e:
        logger.error(f"Error procesando usuario {documento}: {e}")
        return (nombre_completo, documento, f"Error: {str(e)}", "Error", "Error", None)

def usuario_existe_en_bd(documento):
    with psycopg2.connect(
        host="34.148.129.131",
        database="gcpprolinktic",
        user="postgres",
        password="IMlCXSQOkvbGNG6i"
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM usuarios WHERE numero_documento = %s", (documento,))
            return cursor.fetchone() is not None

# Función principal
def main():
    logger.info("=== Iniciando el proceso de gestión de usuarios ===")
    
    try:
        # Inicializar conexiones
        keycloak_token = obtener_token_admin()
        bonita_session = iniciar_sesion_bonita()
        logger.info("✅ Conexiones establecidas con éxito")

        # Obtener cargos disponibles
        with psycopg2.connect(
            host="34.148.129.131",
            database="gcpprolinktic",
            user="postgres",
            password="IMlCXSQOkvbGNG6i"
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT nombre, id FROM cargos")
                cargos_dict = {nombre.upper(): id_ for nombre, id_ in cursor.fetchall()}

        usuarios_a_insertar = []
        usuarios_excel = []

        # Leer y procesar usuarios
        with open('usuarios2.csv', newline='', encoding='utf-8') as csvfile:
            reader = list(csv.DictReader(csvfile))
            logger.info(f"Procesando {len(reader)} usuarios usando {MAX_THREADS} hilos...")
            
            with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
                futures = [executor.submit(
                    procesar_usuario_individual,
                    row,
                    keycloak_token,
                    bonita_session,
                    cargos_dict
                ) for row in reader]

                for future in futures:
                    result = future.result()
                    usuarios_excel.append(result[:5])
                    if result[5]:  # Si hay datos para insertar en BD
                        usuarios_a_insertar.append(result[5])

        # Insertar usuarios en la base de datos
        if usuarios_a_insertar:
            with psycopg2.connect(
                host="34.148.129.131",
                database="gcpprolinktic",
                user="postgres",
                password="IMlCXSQOkvbGNG6i"
            ) as conn:
                with conn.cursor() as cursor:
                    from psycopg2.extras import execute_values
                    insert_query = """
                    INSERT INTO usuarios (
                        user_name, first_name, last_name, enabled, email, numero_documento,
                        modulo_id, tipousuario, tipo_documento, accion_modificacion,
                        departamento_id, municipio_id, cargo, timeout_min
                    ) VALUES %s
                    """
                    execute_values(cursor, insert_query, usuarios_a_insertar)
                    conn.commit()
                    logger.info(f"✅ Se insertaron {len(usuarios_a_insertar)} usuarios en la base de datos")
        else:
            logger.info("ℹ️ No se insertó ningún usuario en la base de datos")

        # Generar reporte Excel
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        output_filename = f"usuarios_insertados_{timestamp}.xlsx"

        wb = Workbook()
        ws = wb.active
        ws.title = "Resultado Usuarios"
        ws.append(["Usuario", "Documento", "Estado", "Estado Keycloak", "Estado Bonita"])
        for usuario in usuarios_excel:
            ws.append(list(usuario))
        wb.save(output_filename)
        logger.info(f"📄 Reporte generado: {output_filename}")

    except Exception as e:
        logger.error(f"Error en el proceso principal: {e}", exc_info=True)
    finally:
        logger.info("=== Proceso de gestión de usuarios finalizado ===")

if __name__ == "__main__":
    main()
