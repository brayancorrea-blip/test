import csv
import psycopg2
import requests
import logging
from openpyxl import Workbook
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- Configuration ---

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Keycloak Configuration
KEYCLOAK_URL = "https://sgdea-prod.proyectos-3t.tech"
REALM_NAME = "positiva"
CLIENT_ID = "cliente-registrar"
CLIENT_SECRET = "kmgmRbXYwvoxlwmo461B7kUGRbl1zv8C"

# Bonita Configuration
BONITA_URL = "https://sgdea-prod.proyectos-3t.tech"
BONITA_ADMIN_USER = "tech_user_prod"
BONITA_ADMIN_PASS = "nvq8cYzodOav6O"

# Database Configuration (PostgreSQL)
DB_CONFIG = {
    "host": "34.148.129.131",
    "database": "gcpprolinktic",
    "user": "postgres",
    "password": "IMlCXSQOkvbGNG6i"
}

# General Settings
DEFAULT_PASSWORD = "Sgdea2025*" # Default password for new or updated users in Keycloak/Bonita
MAX_THREADS = 5
CSV_FILE = "usersWithRoles.csv" # Input CSV file for users and roles
OUTPUT_EXCEL_PREFIX = "usuarios_y_roles_reporte" # Prefix for the output Excel report file

# --- Helper Functions ---

def map_user_type(usertype_raw: str) -> str:
    """Maps raw user type string to a standardized 'Proveedor' or 'Interno'."""
    # This assumes 'PROVEEDOR' is the only non-Interno type. Adjust as needed.
    return "Proveedor" if usertype_raw.strip().upper() == "PROVEEDOR" else "Interno"

def get_db_connection():
    """Establishes and returns a database connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Successfully connected to the database.")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
        raise

# --- Keycloak Operations ---

class KeycloakClient:
    """Handles Keycloak API interactions."""
    def __init__(self, keycloak_url: str, realm_name: str, client_id: str, client_secret: str):
        self.base_url = f"{keycloak_url}/realms/{realm_name}"
        self.admin_url = f"{keycloak_url}/admin/realms/{realm_name}"
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = self._get_admin_token()

    def _get_admin_token(self) -> str:
        """Obtains an admin access token from Keycloak."""
        url = f"{self.base_url}/protocol/openid-connect/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        try:
            resp = requests.post(url, headers=headers, data=data)
            resp.raise_for_status()
            logger.info("Keycloak admin token obtained successfully.")
            return resp.json().get("access_token")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error obtaining Keycloak admin token: {e}")
            raise

    def create_or_update_user(self, user_data: dict) -> bool:
        """
        Creates a new user in Keycloak or updates an existing one if found.
        Resets password to DEFAULT_PASSWORD if user exists.
        """
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        username = user_data["username"]

        try:
            # Check if user exists
            url_get = f"{self.admin_url}/users?username={username}"
            resp = requests.get(url_get, headers=headers)
            resp.raise_for_status()
            users = resp.json()

            if users:
                user_id = users[0]["id"]
                logger.info(f"Keycloak user '{username}' found, updating password.")
                # Reset password
                url_pw = f"{self.admin_url}/users/{user_id}/reset-password"
                payload = {"type": "password", "value": DEFAULT_PASSWORD, "temporary": False}
                requests.put(url_pw, headers=headers, json=payload).raise_for_status()
                logger.info(f"Keycloak password reset for user '{username}'.")
            else:
                logger.info(f"Keycloak user '{username}' not found, creating new user.")
                # Create new user
                url_create = f"{self.admin_url}/users"
                payload = {
                    "username": username,
                    "firstName": user_data["first_name"],
                    "lastName": user_data["last_name"],
                    "email": user_data.get("email", ""), # Include email if available
                    "enabled": True,
                    "credentials": [{"type": "password", "value": DEFAULT_PASSWORD, "temporary": False}]
                }
                requests.post(url_create, headers=headers, json=payload).raise_for_status()
                logger.info(f"Keycloak user '{username}' created successfully.")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Error creating/updating Keycloak user '{username}': {e}")
            return False

# --- Bonita Operations ---

class BonitaClient:
    """Handles Bonita API interactions."""
    def __init__(self, bonita_url: str, admin_user: str, admin_pass: str):
        self.base_url = f"{bonita_url}/bonita"
        self.admin_user = admin_user
        self.admin_pass = admin_pass
        self.session = self._login_bonita()

    def _login_bonita(self) -> requests.Session:
        """Logs into Bonita and returns a session object."""
        session = requests.Session()
        login_url = f"{self.base_url}/loginservice"
        data = {"username": self.admin_user, "password": self.admin_pass, "redirect": "false"}
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        try:
            resp = session.post(login_url, data=data, headers=headers)
            resp.raise_for_status()
            if resp.status_code == 204:
                token = session.cookies.get("X-Bonita-API-Token")
                session.headers.update({"X-Bonita-API-Token": token, "Content-Type": "application/json"})
                logger.info("Successfully logged into Bonita.")
                return session
            else:
                raise Exception(f"Unexpected status code {resp.status_code} during Bonita login.")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error logging into Bonita: {e}")
            raise

    def create_or_update_user(self, user_data: dict) -> bool:
        """
        Creates a new user in Bonita or updates an existing one.
        """
        username = user_data["username"]
        try:
            # Check if user exists
            url_get = f"{self.base_url}/API/identity/user?p=0&c=1&f=userName={username}"
            resp = self.session.get(url_get)
            resp.raise_for_status()
            users = resp.json()

            if users:
                user_id = users[0]["id"]
                logger.info(f"Bonita user '{username}' found, updating details.")
                url_update = f"{self.base_url}/API/identity/user/{user_id}"
                payload = {
                    "firstname": user_data["first_name"],
                    "lastname": user_data["last_name"],
                    "password": DEFAULT_PASSWORD,
                    "email": user_data.get("email", ""), # Include email if available
                    "enabled": "true"
                }
                self.session.put(url_update, json=payload).raise_for_status()
                logger.info(f"Bonita user '{username}' updated successfully.")
            else:
                logger.info(f"Bonita user '{username}' not found, creating new user.")
                url_create = f"{self.base_url}/API/identity/user"
                payload = {
                    "userName": username,
                    "firstname": user_data["first_name"],
                    "lastname": user_data["last_name"],
                    "password": DEFAULT_PASSWORD,
                    "email": user_data.get("email", ""), # Include email if available
                    "enabled": "true"
                }
                self.session.post(url_create, json=payload).raise_for_status()
                logger.info(f"Bonita user '{username}' created successfully.")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Error creating/updating Bonita user '{username}': {e}")
            return False

# --- Database Operations ---

def get_job_titles_from_db(cursor) -> dict:
    """Fetches job titles and their IDs from the database."""
    try:
        cursor.execute("SELECT nombre, id FROM cargos")
        return {nombre.strip(): id_ for nombre, id_ in cursor.fetchall()}
    except psycopg2.Error as e:
        logger.error(f"Error fetching job titles from DB: {e}")
        raise

def assign_role_in_db(row: dict, cursor) -> str:
    """Assigns a role to a user in the database."""
    documento = row['document']
    role_name = row['role_name']
    location_name = row['location_name'] # This is the 'OFICINA' or 'DEPENDENCIA' from CSV

    try:
        # Get user_id
        cursor.execute("SELECT id FROM usuarios WHERE numero_documento = %s", (documento,))
        user_id = cursor.fetchone()
        if not user_id:
            return "❌ User not found in DB"
        user_id = user_id[0]

        # Get role_id
        cursor.execute("SELECT id FROM roles WHERE nombre = %s", (role_name,))
        role_id = cursor.fetchone()
        if not role_id:
            return f"❌ Role '{role_name}' not found"
        role_id = role_id[0]

        section_id = None
        office_id = None

        # 1. Try to find location_name directly in seccionsubseccion.nombre
        cursor.execute("SELECT idseccionsubseccion FROM seccionsubseccion WHERE nombre = %s", (location_name,))
        seccionsubseccion_match = cursor.fetchone()

        if seccionsubseccion_match:
            section_id = seccionsubseccion_match[0]
            # If it's a seccionsubseccion by name, it might also be an oficina with the same ID.
            # Check if an oficina exists with this section_id as its ID.
            cursor.execute("SELECT id FROM oficina WHERE id = %s", (section_id,))
            office_match = cursor.fetchone()
            if office_match:
                office_id = office_match[0] # The oficina's ID is the same as the section's ID
            logger.info(f"Location '{location_name}' found as seccionsubseccion (ID: {section_id}), office ID: {office_id if office_id else 'N/A'})")
        else:
            # 2. If not found in seccionsubseccion, try to find it in oficina.nombre
            logger.debug(f"Location '{location_name}' not found in seccionsubseccion. Attempting to find in oficina.nombre...")
            cursor.execute("SELECT id, id_dependencia FROM oficina WHERE nombre = %s", (location_name,))
            oficina_match = cursor.fetchone()

            if oficina_match:
                office_id = oficina_match[0] # This is the ID of the oficina
                dependent_section_id = oficina_match[1] # This is the crucial id_dependencia (FK to seccionsubseccion)

                # Now, validate that this dependent_section_id is indeed a valid seccionsubseccion
                cursor.execute("SELECT idseccionsubseccion FROM seccionsubseccion WHERE idseccionsubseccion = %s", (dependent_section_id,))
                if cursor.fetchone():
                    section_id = dependent_section_id
                    logger.info(f"Location '{location_name}' found as oficina (ID: {office_id}), using its id_dependencia ({section_id}) as seccionsubseccion_id.")
                else:
                    logger.warning(f"Location '{location_name}' found as oficina (ID: {office_id}), but its id_dependencia ({dependent_section_id}) is not a valid seccionsubseccion ID. Skipping role assignment.")
                    return f"❌ Location dependency not valid: {location_name}"
            else:
                logger.error(f"Location '{location_name}' was not found in either seccionsubseccion or oficina tables.")
                return f"❌ Location '{location_name}' not found in 'seccionsubseccion' or 'oficina'."

        if not section_id:
            # This should ideally not be reached if the above logic is sound, but as a fallback
            return f"❌ Failed to determine a valid seccionsubseccion ID for location '{location_name}'."

        # Check if relationship already exists
        # Use IS NOT DISTINCT FROM for handling NULLs correctly in comparisons
        cursor.execute("""
            SELECT 1 FROM usuario_relacion
            WHERE usuario_id = %s AND rol_id = %s AND seccionsubseccion_id = %s AND oficina_id IS NOT DISTINCT FROM %s
        """, (user_id, role_id, section_id, office_id))

        if cursor.fetchone():
            return "⚠️ Already exists"

        # Insert new relationship
        cursor.execute("""
            INSERT INTO usuario_relacion (usuario_id, seccionsubseccion_id, oficina_id, rol_id, punto_radicacion_id)
            VALUES (%s, %s, %s, %s, NULL)
        """, (user_id, section_id, office_id, role_id))
        return "✅ Role assigned"

    except psycopg2.Error as e:
        logger.error(f"Error assigning role for document {documento}: {e}")
        return f"❌ DB Error: {e}"

def insert_or_update_user_in_db(row: dict, cargo_id: int, user_type: str, cursor) -> str:
    """Inserts a new user into the database or does nothing if they exist."""
    documento = row['document']
    try:
        # Check if user exists (just a check, not an upsert on entire user record)
        cursor.execute("SELECT 1 FROM usuarios WHERE numero_documento = %s", (documento,))
        if not cursor.fetchone():
            logger.info(f"User '{documento}' not found in DB, inserting.")
            cursor.execute("""
                INSERT INTO usuarios (
                    user_name, first_name, last_name, enabled, email, numero_documento,
                    modulo_id, tipousuario, tipo_documento, accion_modificacion,
                    departamento_id, municipio_id, cargo, timeout_min
                )
                VALUES (%s, %s, %s, TRUE, %s, %s, 1, %s, 3, 'Creación de usuario', 6, 149, %s, 4)
                ON CONFLICT (numero_documento) DO NOTHING
            """, (
                documento, row['first_name'], row['last_name'], row['email'],
                documento, user_type, cargo_id
            ))
            return "✅ User inserted into DB"
        else:
            return "⚠️ User already exists in DB"
    except psycopg2.Error as e:
        logger.error(f"Error inserting/updating user '{documento}' in DB: {e}")
        return f"❌ DB Error: {e}"

# --- Main Processing Logic ---

def process_user_entry(row: dict, keycloak_client: KeycloakClient, bonita_client: BonitaClient, cargos_dict: dict) -> tuple:
    """Processes a single user entry, interacting with Keycloak, Bonita, and the database."""
    full_name = f"{row.get('first_name', '')} {row.get('last_name', '')}"
    document = row.get('document', 'N/A') # Use .get() for safety
    logger.info(f"Processing user: {full_name} ({document})")

    keycloak_status = "Skipped"
    bonita_status = "Skipped"
    db_user_status = "Skipped"
    db_role_status = "Skipped"

    try:
        # Ensure that validation has been done upstream by main()
        cargo_nombre = row['job_title'] # Already stripped and validated by main()
        user_type = map_user_type(row['user_type'])
        
        # Ensure cargo_id is retrieved from the pre-fetched dictionary
        cargo_id = cargos_dict.get(cargo_nombre)

        if cargo_id is None:
            # This should ideally not happen if main() validated job_title correctly
            # but as a fallback, mark as error and skip DB user insertion.
            logger.error(f"Internal error: Job title '{cargo_nombre}' unexpectedly not found in DB dict for user {document}.")
            db_user_status = "❌ Job title lookup error"

        user_data_common = {
            "username": document,
            "first_name": row['first_name'],
            "last_name": row['last_name'],
            "email": row['email']
        }

        # Keycloak
        logger.info(f"Attempting Keycloak creation/update for {document}...")
        keycloak_success = keycloak_client.create_or_update_user(user_data_common)
        keycloak_status = "✔️ Keycloak ready" if keycloak_success else "❌ Keycloak error"

        # Bonita
        logger.info(f"Attempting Bonita creation/update for {document}...")
        bonita_success = bonita_client.create_or_update_user(user_data_common)
        bonita_status = "✔️ Bonita ready" if bonita_success else "❌ Bonita error"

        # Database operations
        # Only proceed if Keycloak and Bonita were successful
        if keycloak_success and bonita_success:
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        # Insert/update user in 'usuarios' table
                        if cargo_id is not None:
                            db_user_status = insert_or_update_user_in_db(row, cargo_id, user_type, cursor)
                        else:
                            # This path means cargo_id was None, so DB user insertion is skipped
                            db_user_status = "❌ Job title not found for DB user insertion"

                        # Assign role in 'usuario_relacion'
                        # Only attempt role assignment if user insertion/existence check was successful
                        # and there wasn't a job title issue for the DB user part.
                        if ("✅ User inserted into DB" in db_user_status or "⚠️ User already exists in DB" in db_user_status) and cargo_id is not None:
                            db_role_status = assign_role_in_db(row, cursor)
                        else:
                            db_role_status = "Skipped role assignment due to preceding DB user issue or job title error"

                    conn.commit() # Commit transaction if everything successful
            except Exception as db_exc:
                logger.error(f"Database operation failed for user {document}: {db_exc}", exc_info=True)
                db_user_status = f"❌ DB Connection/Operation Error: {str(db_exc)}"
                db_role_status = f"❌ DB Connection/Operation Error: {str(db_exc)}"
        else:
            db_user_status = "Skipped due to Keycloak/Bonita failure"
            db_role_status = "Skipped due to Keycloak/Bonita failure"


        final_user_status = f"{keycloak_status}, {bonita_status}, {db_user_status}"
        return (full_name, document, row['email'], row['job_title'], row['user_type'],
                row['location_name'], row['role_name'], final_user_status, db_role_status)

    except Exception as e:
        logger.error(f"Unhandled error processing user {document}: {e}", exc_info=True)
        return (full_name, document, row['email'], row['job_title'], row['user_type'],
                row['location_name'], row['role_name'], f"❌ Unhandled Error: {str(e)}", "❌ Unhandled Error")

# --- Report Generation ---

def generate_excel_report(results: list, file_prefix: str = OUTPUT_EXCEL_PREFIX):
    """Generates an Excel report from processing results."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{file_prefix}_{timestamp}.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.title = "User and Role Processing Report"
    ws.append(["Name", "Document", "Email", "Job Title", "User Type", "Location", "Role", "User Processing Status", "Role Assignment Status"])

    for r in results:
        ws.append(list(r)) # Append the tuple directly

    try:
        wb.save(filename)
        logger.info(f"✅ Report generated: {filename}")
    except Exception as e:
        logger.error(f"Error saving Excel report: {e}")

# --- Main Execution ---

def get_valid_data_from_db(conn) -> tuple:
    """Obtiene los títulos de trabajo, roles y ubicaciones válidas de la base de datos."""
    cursor = conn.cursor()
    try:
        # Obtener títulos de trabajo válidos
        cursor.execute("SELECT DISTINCT nombre FROM cargos ORDER BY nombre")
        valid_job_titles = {row[0].strip() for row in cursor.fetchall()}
        
        # Obtener nombres de roles válidos
        cursor.execute("SELECT DISTINCT nombre FROM roles ORDER BY nombre")
        valid_role_names = {row[0].strip() for row in cursor.fetchall()}
        
        # Obtener ubicaciones válidas de seccionsubseccion
        cursor.execute("SELECT DISTINCT nombre FROM seccionsubseccion ORDER BY nombre")
        valid_locations_seccion = {row[0].strip() for row in cursor.fetchall()}
        
        # Obtener ubicaciones válidas de oficinas que no están en seccionsubseccion
        cursor.execute("""
            SELECT DISTINCT o.nombre 
            FROM oficina o 
            LEFT JOIN seccionsubseccion s ON o.nombre = s.nombre 
            WHERE s.nombre IS NULL 
            ORDER BY o.nombre
        """)
        valid_locations_oficina = {row[0].strip() for row in cursor.fetchall()}
        
        # Combinar todas las ubicaciones válidas
        valid_locations = valid_locations_seccion | valid_locations_oficina
        
        return valid_job_titles, valid_role_names, valid_locations
    except psycopg2.Error as e:
        logger.error(f"Error obteniendo datos válidos de la BD: {e}")
        raise
    finally:
        cursor.close()

def validate_csv_data(csv_data: list, valid_job_titles: set, valid_role_names: set, valid_locations: set) -> tuple:
    """Valida los datos del CSV contra las listas válidas de la base de datos."""
    invalid_jobs = set()
    invalid_roles = set()
    invalid_locations = set()
    
    for row in csv_data:
        if row['job_title'] not in valid_job_titles:
            invalid_jobs.add(row['job_title'])
        if row['role_name'] not in valid_role_names:
            invalid_roles.add(row['role_name'])
        if row['location_name'] not in valid_locations:
            invalid_locations.add(row['location_name'])
    
    return invalid_jobs, invalid_roles, invalid_locations

def main():
    # Establecer conexión a la base de datos
    conn = get_db_connection()
    try:
        # Obtener datos válidos de la BD
        logger.info("Obteniendo datos válidos de la base de datos...")
        valid_job_titles, valid_role_names, valid_locations = get_valid_data_from_db(conn)
        
        # Leer datos del CSV
        logger.info(f"Leyendo datos del archivo CSV: {CSV_FILE}")
        with open(CSV_FILE, 'r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            csv_data = list(csv_reader)


        print("Datos válidos de la base de datos:")
        print("Títulos de trabajo:", valid_job_titles)
        print("Nombres de roles:", valid_role_names)
        print("Ubicaciones:", valid_locations)
        
        # Validar datos del CSV
        invalid_jobs, invalid_roles, invalid_locations = validate_csv_data(
            csv_data, valid_job_titles, valid_role_names, valid_locations
        )
        
        # Verificar si hay datos inválidos
        if invalid_jobs or invalid_roles or invalid_locations:
            logger.error("Se encontraron datos inválidos en el CSV:")
            if invalid_jobs:
                logger.error(f"Títulos de trabajo inválidos: {invalid_jobs}")
            if invalid_roles:
                logger.error(f"Roles inválidos: {invalid_roles}")
            if invalid_locations:
                logger.error(f"Ubicaciones inválidas: {invalid_locations}")
            logger.error("Por favor, corrija los datos y vuelva a intentar.")
            return
        
        # Continuar con el procesamiento si todos los datos son válidos
        logger.info("Todos los datos del CSV son válidos. Continuando con el procesamiento...")
        # ... existing code ...

    except Exception as e:
        logger.error(f"Error durante la ejecución: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()