import requests
import csv
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import logging

# --- CONFIGURACIÓN DE KEYCLOAK ---
KEYCLOAK_URL = "https://sgdea-prod.proyectos-3t.tech"
REALM_NAME = "positiva"
CLIENT_ID = "cliente-registrar"
CLIENT_SECRET = "kmgmRbXYwvoxlwmo461B7kUGRbl1zv8C"
ADMIN_USER = "admin" # Este usuario es para obtener el token de cliente si fuera necesario un login directo de usuario.
ADMIN_PASS = "yqGnQtpiZxvj2Eeshjo=" # La autenticación principal es por client_credentials.
CSV_PATH = "usuarios.csv"
MAX_THREADS = 5 # Número de hilos para procesamiento concurrente de usuarios

# --- CONFIGURACIÓN DE LOGS SIMPLIFICADA ---
# Configura el logger principal. Los mensajes con nivel INFO o superior se mostrarán.
# Cambia logging.INFO a logging.DEBUG para ver detalles de cada petición HTTP.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- FUNCIONES DE UTILIDAD (PETICIONES HTTP) ---
def make_request(method, url, headers, json_data=None, data=None):
    """
    Realiza una petición HTTP, maneja errores y simplifica el registro.
    
    Args:
        method (str): Método HTTP (GET, POST, PUT).
        url (str): URL del endpoint.
        headers (dict): Diccionario de cabeceras HTTP.
        json_data (dict, optional): Cuerpo de la petición en formato JSON. Defaults to None.
        data (dict, optional): Cuerpo de la petición en formato form-urlencoded. Defaults to None.
        
    Returns:
        requests.Response: Objeto de respuesta HTTP.
        
    Raises:
        requests.exceptions.RequestException: Si ocurre un error en la petición HTTP.
    """
    try:
        if json_data:
            resp = requests.request(method, url, headers=headers, json=json_data)
        elif data:
            resp = requests.request(method, url, headers=headers, data=data)
        else:
            resp = requests.request(method, url, headers=headers)
        
        resp.raise_for_status() # Lanza una excepción para errores HTTP (4xx o 5xx)
        return resp
    except requests.exceptions.RequestException as e:
        error_msg = f"Error en la petición {method} {url}: {e}"
        if resp is not None:
            error_msg += f" (Status: {resp.status_code}, Body: {resp.text})"
        logger.error(error_msg)
        raise # Vuelve a lanzar la excepción para que el llamador la maneje

# --- TOKEN ADMIN ---
def obtener_token_admin():
    """
    Obtiene un token de acceso de administrador de Keycloak utilizando client_credentials.
    
    Returns:
        str: El token de acceso.
        
    Raises:
        requests.exceptions.RequestException: Si falla la obtención del token.
    """
    logger.info("[Keycloak] Obteniendo token de administrador...")
    url = f"{KEYCLOAK_URL}/realms/{REALM_NAME}/protocol/openid-connect/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        # Si tu configuración de Keycloak requiere que el cliente también autentique como un usuario admin
        # para obtener tokens para la API de administración, descomenta las siguientes líneas:
        # "username": ADMIN_USER,
        # "password": ADMIN_PASS
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    resp = make_request("POST", url, headers, data=data)
    token_data = resp.json()
    access_token = token_data.get("access_token")
    if not access_token:
        raise Exception("No se obtuvo access_token de la respuesta de Keycloak.")
    logger.info("[Keycloak] ✅ Token de administrador obtenido.")
    return access_token

# --- API DE USUARIOS ---
def obtener_usuario(token, username):
    """
    Obtiene un usuario de Keycloak por nombre de usuario.
    
    Args:
        token (str): Token de acceso del administrador.
        username (str): Nombre de usuario a buscar.
        
    Returns:
        dict or None: El objeto de usuario de Keycloak si existe, None en caso contrario.
    """
    url = f"{KEYCLOAK_URL}/admin/realms/{REALM_NAME}/users?username={username}"
    headers = {"Authorization": f"Bearer {token}"}
    logger.debug(f"Buscando usuario: {username}")
    resp = make_request("GET", url, headers)
    usuarios = resp.json()
    return usuarios[0] if usuarios else None

def crear_usuario(token, user):
    """
    Crea un nuevo usuario en Keycloak.
    
    Args:
        token (str): Token de acceso del administrador.
        user (dict): Diccionario con los datos del usuario (username, firstname, lastname, password).
    """
    url = f"{KEYCLOAK_URL}/admin/realms/{REALM_NAME}/users"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "username": user["username"],
        "firstName": user["firstname"],
        "lastName": user["lastname"],
        "enabled": True,
        "credentials": [{
            "type": "password",
            "value": user["password"],
            "temporary": False
        }]
    }
    logger.debug(f"Intentando crear usuario: {user['username']}")
    make_request("POST", url, headers, json_data=payload)
    logger.info(f"✅ Usuario creado: {user['username']}")

def actualizar_password(token, user_id, new_password):
    """
    Actualiza la contraseña de un usuario existente en Keycloak.
    
    Args:
        token (str): Token de acceso del administrador.
        user_id (str): ID del usuario en Keycloak.
        new_password (str): Nueva contraseña.
    """
    url = f"{KEYCLOAK_URL}/admin/realms/{REALM_NAME}/users/{user_id}/reset-password"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "type": "password",
        "value": new_password,
        "temporary": False
    }
    logger.debug(f"Intentando actualizar contraseña para usuario ID {user_id}")
    make_request("PUT", url, headers, json_data=payload)
    logger.info(f"🔁 Contraseña actualizada para usuario ID {user_id}")

# --- CSV ---
def cargar_usuarios_desde_csv(path):
    """
    Carga los datos de los usuarios desde un archivo CSV, validando y eliminando duplicados.
    
    Args:
        path (str): Ruta al archivo CSV.
        
    Returns:
        list: Lista de diccionarios, cada uno representando un usuario único.
        
    Raises:
        FileNotFoundError: Si el archivo CSV no se encuentra.
        Exception: Otros errores durante la lectura del CSV.
    """
    usuarios = []
    seen_usernames = set() # Para detectar y omitir usuarios duplicados por 'username'

    try:
        with open(path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for i, row in enumerate(reader):
                line_num = i + 2 # +1 for header, +1 for 0-index
                username = row.get("username", "").strip()

                if not username:
                    logger.warning(f"Línea {line_num}: 'username' vacío. Fila omitida: {row}")
                    continue

                if username in seen_usernames:
                    logger.warning(f"Línea {line_num}: Usuario '{username}' duplicado en el CSV. Se procesará solo la primera aparición.")
                    continue

                # Asegurarse de que todas las claves existan, proporcionando un string vacío si faltan
                usuarios.append({
                    "username": username,
                    "password": row.get("password", "").strip(),
                    "firstname": row.get("firstname", "").strip(),
                    "lastname": row.get("lastname", "").strip()
                })
                seen_usernames.add(username)
    except FileNotFoundError:
        logger.error(f"Error: El archivo CSV no se encontró en la ruta: {path}")
        raise
    except Exception as e:
        logger.error(f"Error al leer el archivo CSV '{path}': {e}")
        raise
    return usuarios

# --- PROCESAMIENTO CONCURRENTE ---
def procesar_usuario_individual(token, user):
    """
    Procesa un solo usuario: lo busca, crea o actualiza la contraseña en Keycloak.
    Esta función está diseñada para ser ejecutada por ThreadPoolExecutor.
    
    Args:
        token (str): Token de acceso del administrador (se pasa a cada hilo).
        user (dict): Diccionario con los datos del usuario a procesar.
        
    Returns:
        dict: Un diccionario con el resultado del procesamiento del usuario.
    """
    try:
        existing_user = obtener_usuario(token, user["username"])
        if existing_user:
            actualizar_password(token, existing_user["id"], user["password"])
            return {
                "username": user["username"],
                "accion": "actualizado",
                "estado": "éxito",
                "mensaje": "Contraseña actualizada correctamente"
            }
        else:
            crear_usuario(token, user)
            return {
                "username": user["username"],
                "accion": "creado",
                "estado": "éxito",
                "mensaje": "Usuario creado correctamente"
            }
    except Exception as e:
        logger.error(f"❌ Error procesando usuario '{user['username']}': {e}")
        return {
            "username": user["username"],
            "accion": "error",
            "estado": "fallido",
            "mensaje": str(e)
        }

def guardar_reporte(resultados):
    """
    Guarda los resultados del procesamiento de usuarios en un archivo CSV de reporte.
    
    Args:
        resultados (list): Lista de diccionarios con los resultados de cada usuario.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    reporte_path = f"reporte_usuarios_keycloak_{timestamp}.csv"
    
    try:
        with open(reporte_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ["username", "accion", "estado", "mensaje"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(resultados)
        logger.info(f"📝 Reporte de operaciones guardado en: {reporte_path}")
    except IOError as e:
        logger.error(f"Error al guardar el reporte en '{reporte_path}': {e}")
    except Exception as e:
        logger.error(f"Error inesperado al generar el reporte CSV: {e}")

# --- FUNCIÓN PRINCIPAL ---
def main():
    """
    Función principal que orquesta el proceso de gestión de usuarios en Keycloak.
    """
    logger.info("===================================================")
    logger.info("=== Iniciando el proceso de gestión de usuarios en Keycloak ===")
    logger.info("===================================================")
    
    token = None
    try:
        # Paso 1: Obtener el token de administrador (una única vez)
        token = obtener_token_admin()
        
        # Paso 2: Cargar usuarios desde el archivo CSV
        usuarios = cargar_usuarios_desde_csv(CSV_PATH)
        logger.info(f"Se cargarán {len(usuarios)} usuarios únicos del archivo CSV '{CSV_PATH}'.")

        if not usuarios:
            logger.warning("No hay usuarios válidos para procesar en el archivo CSV. Finalizando.")
            return

        resultados_futuros = []
        # Paso 3: Procesar usuarios en paralelo usando ThreadPoolExecutor
        logger.info(f"Procesando usuarios usando {MAX_THREADS} hilos concurrentes...")
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            # Enviar cada usuario a un hilo para procesamiento.
            # El token se pasa a cada llamada, ya que es el mismo para todas las operaciones.
            resultados_futuros = [executor.submit(procesar_usuario_individual, token, user) for user in usuarios]
        
        # Recopilar los resultados de todos los hilos una vez que hayan terminado
        resultados_finales = [future.result() for future in resultados_futuros]
        
        # Paso 4: Guardar el reporte de las operaciones realizadas
        guardar_reporte(resultados_finales)
        
    except requests.exceptions.ConnectionError:
        logger.critical("Error de conexión: No se pudo conectar al servidor de Keycloak. Verifica la URL y tu conexión.")
    except requests.exceptions.Timeout:
        logger.critical("Error de tiempo de espera: La petición a Keycloak tardó demasiado. Puede ser un problema de red o del servidor.")
    except requests.exceptions.HTTPError as e:
        logger.critical(f"Error HTTP inesperado durante el proceso: {e} (Status Code: {e.response.status_code})")
    except FileNotFoundError as e:
        logger.critical(f"Error: {e}. Asegúrate de que el archivo CSV especificado exista.")
    except Exception as e:
        logger.critical(f"Un error inesperado ha ocurrido en el proceso principal: {e}", exc_info=True)
    finally:
        logger.info("=====================================================")
        logger.info("=== Proceso de gestión de usuarios de Keycloak finalizado ===")
        logger.info("=====================================================")

# Punto de entrada del script
if __name__ == "__main__":
    main()