import requests
import csv
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import logging

# --- Configuración ---
BONITA_URL = "https://sgdea-prod.proyectos-3t.tech"
BONITA_ADMIN_USER = "tech_user_prod"
BONITA_ADMIN_PASS = "nvq8cYzodOav6O"
CSV_PATH = "usuarios.csv"
MAX_THREADS = 5 # Número de hilos para procesamiento concurrente

# --- Configuración de Logs Simplificada ---
# Configura el logger principal. Los mensajes con nivel INFO o superior se mostrarán.
# Puedes cambiar logging.INFO a logging.DEBUG para ver logs más detallados de las peticiones.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Funciones de Utilidad (Log y Peticiones HTTP) ---
def make_request(session, method, url, **kwargs):
    """
    Realiza una petición HTTP, maneja errores y simplifica el registro.
    
    Args:
        session (requests.Session): La sesión de requests.
        method (str): Método HTTP (GET, POST, PUT).
        url (str): URL del endpoint.
        **kwargs: Argumentos adicionales para requests.request (json, data, headers, etc.).
    
    Returns:
        requests.Response: Objeto de respuesta HTTP.
        
    Raises:
        requests.exceptions.RequestException: Si ocurre un error en la petición HTTP.
    """
    try:
        resp = session.request(method, url, **kwargs)
        resp.raise_for_status() # Lanza una excepción para errores HTTP (4xx o 5xx)
        return resp
    except requests.exceptions.RequestException as e:
        # Registrar la URL y el método de la petición fallida
        error_msg = f"Error en la petición {method} {url}: {e}"
        if resp is not None:
            # Incluir el código de estado y el cuerpo de la respuesta si están disponibles
            error_msg += f" (Status: {resp.status_code}, Body: {resp.text})"
        logger.error(error_msg)
        raise # Vuelve a lanzar la excepción para que el llamador la maneje

# --- Funciones de Bonita BPM ---

def iniciar_sesion_bonita():
    """
    Inicia sesión en Bonita BPM y configura la sesión de requests con las cookies y headers necesarios.
    
    Returns:
        requests.Session: La sesión de requests autenticada.
    
    Raises:
        Exception: Si faltan cookies de autenticación o hay un error al iniciar sesión.
    """
    session = requests.Session()
    login_url = f"{BONITA_URL}/bonita/loginservice"
    data = {
        "username": BONITA_ADMIN_USER,
        "password": BONITA_ADMIN_PASS,
        "redirect": "false"
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    logger.info(f"[Bonita] Intentando iniciar sesión para el usuario admin: {BONITA_ADMIN_USER}...")
    
    # Usamos make_request para la llamada de login también
    resp = make_request(session, "POST", login_url, data=data, headers=headers)

    # Las respuestas de login de Bonita pueden no ser JSON directo, solo importa el status
    if resp.status_code == 204:
        jsessionid = session.cookies.get("JSESSIONID")
        api_token = session.cookies.get("X-Bonita-API-Token")
        bonita_tenant = session.cookies.get("bonita.tenant") or "1" # Bonita tenant suele ser '1' por defecto

        if not api_token or not jsessionid:
            raise Exception("Faltan cookies necesarias de autenticación después del login. Asegúrate de que las credenciales sean correctas y Bonita esté accesible.")

        # Headers para futuras peticiones JSON. Bonita espera Content-Type application/json.
        session.headers.update({
            "X-Bonita-API-Token": api_token,
            "Content-Type": "application/json"
        })
        # Bonita a veces necesita que estas cookies se fijen explícitamente en la sesión
        session.cookies.set("X-Bonita-API-Token", api_token)
        session.cookies.set("bonita.tenant", bonita_tenant)
        session.cookies.set("BOS_Locale", "en") # Establecer locale a inglés para mensajes de error consistentes

        logger.info("[Bonita] Sesión iniciada correctamente.")
        return session
    else:
        # Esto debería ser manejado por make_request, pero como respaldo:
        raise Exception(f"Fallo inesperado al iniciar sesión: {resp.status_code} - {resp.text}")

def obtener_usuario(session, username):
    """
    Obtiene un usuario de Bonita BPM por nombre de usuario.
    
    Args:
        session (requests.Session): La sesión de requests autenticada para la petición.
        username (str): El nombre de usuario a buscar.
        
    Returns:
        dict or None: El objeto de usuario de Bonita si existe, None en caso contrario.
    """
    url = f"{BONITA_URL}/bonita/API/identity/user?p=0&c=1&f=userName={username}"
    logger.debug(f"Buscando usuario: {username}")
    resp = make_request(session, "GET", url)
    users = resp.json()
    return users[0] if users else None

def crear_usuario(session, user):
    """
    Crea un nuevo usuario en Bonita BPM.
    
    Args:
        session (requests.Session): La sesión de requests autenticada para la petición.
        user (dict): Diccionario con los datos del usuario (username, firstname, lastname, password).
        
    Returns:
        dict: El objeto de usuario creado por Bonita.
    """
    url = f"{BONITA_URL}/bonita/API/identity/user"
    payload = {
        "userName": user["username"],
        "firstname": user["firstname"],
        "lastname": user["lastname"],
        "password": user["password"],
        "enabled": "true" # Asegúrate de que el usuario esté habilitado al crearlo
    }
    logger.debug(f"Intentando crear usuario: {user['username']}")
    resp = make_request(session, "POST", url, json=payload)
    new_user = resp.json()
    logger.info(f"✅ Usuario creado: {user['username']}")
    return new_user

def actualizar_usuario(session, user_id, user_data):
    """
    Actualiza un usuario existente en Bonita BPM.
    
    Args:
        session (requests.Session): La sesión de requests autenticada para la petición.
        user_id (str): El ID del usuario en Bonita.
        user_data (dict): Diccionario con los datos a actualizar (firstname, lastname, password, enabled).
    """
    url = f"{BONITA_URL}/bonita/API/identity/user/{user_id}"
    payload = {
        "firstname": user_data["firstname"],
        "lastname": user_data["lastname"],
        "password": user_data["password"],
        "enabled": "true" # Asegura que el usuario esté habilitado al actualizar
    }
    logger.debug(f"Intentando actualizar usuario ID {user_id}: {user_data['username']}")
    make_request(session, "PUT", url, json=payload)
    logger.info(f"✅ Usuario actualizado y activado: {user_data['username']}")

# --- Lógica de Procesamiento ---

def cargar_usuarios_desde_csv(path):
    """
    Carga los datos de los usuarios desde un archivo CSV, eliminando duplicados por nombre de usuario.
    
    Args:
        path (str): Ruta al archivo CSV.
        
    Returns:
        list: Lista de diccionarios, cada uno representando un usuario único.
        
    Raises:
        FileNotFoundError: Si el archivo CSV no se encuentra.
        Exception: Otros errores durante la lectura del CSV.
    """
    usuarios = []
    seen_usernames = set() # Para eliminar duplicados por username

    try:
        with open(path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                username = row.get("username", "").strip() # Usar .get() para evitar KeyError si la columna no existe
                if username: # Asegurarse de que el username no esté vacío
                    if username not in seen_usernames: 
                        usuarios.append({
                            "username": username,
                            "password": row.get("password", "").strip(),
                            "firstname": row.get("firstname", "").strip(),
                            "lastname": row.get("lastname", "").strip()
                        })
                        seen_usernames.add(username)
                    else:
                        logger.warning(f"Usuario duplicado omitido en CSV: {username}")
                else:
                    logger.warning(f"Fila omitida en CSV debido a 'username' vacío o no encontrado: {row}")
    except FileNotFoundError:
        logger.error(f"Error: El archivo CSV no se encontró en la ruta: {path}")
        raise
    except Exception as e:
        logger.error(f"Error al leer el archivo CSV '{path}': {e}")
        raise
    return usuarios

def procesar_usuario_individual(base_session, user):
    """
    Procesa un solo usuario: lo busca, crea o actualiza en Bonita.
    Esta función está diseñada para ser ejecutada en un hilo separado.
    
    Args:
        base_session (requests.Session): La sesión de requests autenticada (la base para clonar).
        user (dict): Diccionario con los datos del usuario a procesar.
        
    Returns:
        dict: Un diccionario con el resultado del procesamiento del usuario (éxito/fallido, acción, mensaje).
    """
    # Clonar la sesión base para este hilo
    # Esto asegura que cada hilo tenga su propio objeto de sesión y maneje sus cookies/headers de forma independiente.
    thread_session = requests.Session()
    thread_session.headers.update(base_session.headers)
    for cookie in base_session.cookies:
        thread_session.cookies.set(cookie.name, cookie.value, domain=cookie.domain, path=cookie.path)

    try:
        existente = obtener_usuario(thread_session, user["username"])
        if existente:
            actualizar_usuario(thread_session, existente["id"], user)
            return {
                "username": user["username"],
                "accion": "actualizado",
                "estado": "éxito",
                "mensaje": "Usuario actualizado y activado correctamente"
            }
        else:
            crear_usuario(thread_session, user)
            return {
                "username": user["username"],
                "accion": "creado",
                "estado": "éxito",
                "mensaje": "Usuario creado correctamente"
            }
    except Exception as e:
        # Captura cualquier excepción durante el procesamiento de un usuario individual
        # y registra el error con el nombre de usuario afectado.
        return {
            "username": user["username"],
            "accion": "error",
            "estado": "fallido",
            "mensaje": str(e)
        }

def guardar_reporte(resultados):
    """
    Guarda los resultados del procesamiento de usuarios en un archivo CSV de reporte.
    El nombre del archivo incluye una marca de tiempo para evitar sobrescribir reportes anteriores.
    
    Args:
        resultados (list): Lista de diccionarios, cada uno con el resultado de procesar un usuario.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    reporte_path = f"reporte_usuarios_bonita_{timestamp}.csv"
    
    try:
        with open(reporte_path, 'w', newline='', encoding='utf-8') as csvfile:
            # Definir los nombres de las columnas para el CSV de reporte
            fieldnames = ["username", "accion", "estado", "mensaje"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader() # Escribe la fila de encabezados
            writer.writerows(resultados) # Escribe todas las filas de resultados
        logger.info(f"📝 Reporte de operaciones guardado en: {reporte_path}")
    except IOError as e:
        logger.error(f"Error al guardar el reporte en '{reporte_path}': {e}")
    except Exception as e:
        logger.error(f"Error inesperado al generar el reporte CSV: {e}")

# --- Función Principal del Script ---

def main():
    """
    Función principal que orquesta el proceso de inicio de sesión, carga de usuarios,
    procesamiento concurrente y generación de reportes.
    """
    logger.info("===================================================")
    logger.info("=== Iniciando el proceso de gestión de usuarios en Bonita ===")
    logger.info("===================================================")
    
    session = None
    try:
        # Paso 1: Iniciar sesión en Bonita (una sola vez)
        session = iniciar_sesion_bonita()
        
        # Paso 2: Cargar usuarios desde el archivo CSV
        usuarios = cargar_usuarios_desde_csv(CSV_PATH)
        logger.info(f"Se cargarán {len(usuarios)} usuarios únicos del archivo CSV '{CSV_PATH}'.")

        resultados_futuros = []
        # Paso 3: Procesar usuarios en paralelo usando ThreadPoolExecutor
        logger.info(f"Procesando usuarios usando {MAX_THREADS} hilos concurrentes...")
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            # `executor.submit` envía una tarea a un hilo y devuelve un objeto Future
            resultados_futuros = [executor.submit(procesar_usuario_individual, session, user) for user in usuarios]
        
        # Recopilar los resultados de todos los hilos una vez que hayan terminado
        resultados_finales = [future.result() for future in resultados_futuros]
        
        # Paso 4: Guardar el reporte de las operaciones realizadas
        guardar_reporte(resultados_finales)
        
    except requests.exceptions.ConnectionError:
        logger.critical("Error de conexión: No se pudo conectar al servidor de Bonita. Verifica la URL y tu conexión a internet.")
    except requests.exceptions.Timeout:
        logger.critical("Error de tiempo de espera: La petición a Bonita tardó demasiado. Puede ser un problema de red o del servidor.")
    except requests.exceptions.HTTPError as e:
        logger.critical(f"Error HTTP inesperado durante el proceso: {e} (Status Code: {e.response.status_code})")
    except FileNotFoundError as e:
        logger.critical(f"Error: {e}. Asegúrate de que el archivo CSV especificado exista.")
    except Exception as e:
        logger.critical(f"Un error inesperado ha ocurrido en el proceso principal: {e}", exc_info=True)
    finally:
        logger.info("====================================================")
        logger.info("=== Proceso de gestión de usuarios de Bonita finalizado ===")
        logger.info("====================================================")

# Punto de entrada del script
if __name__ == "__main__":
    main()