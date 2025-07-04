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
DEFAULT_PASSWORD = "Sgdea2025*"
MAX_THREADS = 5
CSV_FILE = "usersWithRoles.csv"
OUTPUT_EXCEL_PREFIX = "usuarios_y_roles"

# --- Helper Functions ---

def to_upper_preserve_accents(text: str) -> str:
    """Converts text to uppercase, preserving accented characters."""
    return text.upper()

def map_user_type(usertype_raw: str) -> str:
    """Maps raw user type string to a standardized 'Proveedor' or 'Interno'."""
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
        return {nombre.upper(): id_ for nombre, id_ in cursor.fetchall()}
    except psycopg2.Error as e:
        logger.error(f"Error fetching job titles from DB: {e}")
        raise

def assign_role_in_db(row: dict, cursor) -> str:
    """Assigns a role to a user in the database."""
    documento = row['document']
    role_name = row['role_name']
    location_name = row['location_name']

    try:
        cursor.execute("SELECT id FROM usuarios WHERE numero_documento = %s", (documento,))
        user_id = cursor.fetchone()
        if not user_id:
            return "❌ User not found in DB"
        user_id = user_id[0]

        cursor.execute("SELECT id FROM roles WHERE nombre = %s", (role_name,))
        role_id = cursor.fetchone()
        if not role_id:
            return f"❌ Role '{role_name}' not found"
        role_id = role_id[0]

        # Determine location ID (Oficina or SeccionSubseccion)
        cursor.execute("SELECT id FROM oficina WHERE nombre = %s", (location_name,))
        office_id_result = cursor.fetchone()

        cursor.execute("SELECT idseccionsubseccion FROM seccionsubseccion WHERE nombre = %s", (location_name,))
        section_id_result = cursor.fetchone()

        section_id = office_id_result[0] if office_id_result else (section_id_result[0] if section_id_result else None)
        office_id = office_id_result[0] if office_id_result else None

        if not section_id:
            return f"❌ Location '{location_name}' not found"

        # Check if relationship already exists
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
    full_name = f"{row['first_name']} {row['last_name']}"
    document = row['document']
    logger.info(f"Processing user: {full_name} ({document})")

    keycloak_status = "Skipped"
    bonita_status = "Skipped"
    db_user_status = "Skipped"
    db_role_status = "Skipped"

    try:
        cargo_nombre = to_upper_preserve_accents(row['job_title'].strip())
        user_type = map_user_type(row['user_type'])
        cargo_id = cargos_dict.get(cargo_nombre)

        if cargo_id is None:
            logger.warning(f"Job title '{row['job_title']}' not found in DB for user {document}. Skipping DB user insertion.")
            db_user_status = "❌ Job title not found"

        user_data_common = {
            "username": document,
            "first_name": row['first_name'],
            "last_name": row['last_name'],
            "email": row['email'] # Added email for completeness in user_data
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
        if keycloak_success and bonita_success: # Only proceed if Keycloak and Bonita were successful
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # Insert/update user in 'usuarios' table
                    if cargo_id is not None:
                        db_user_status = insert_or_update_user_in_db(row, cargo_id, user_type, cursor)
                    else:
                        db_user_status = "❌ Job title not found for DB user insertion"

                    # Assign role in 'usuario_relacion'
                    if "✅ User inserted into DB" in db_user_status or "⚠️ User already exists in DB" in db_user_status:
                         db_role_status = assign_role_in_db(row, cursor)
                    else:
                        db_role_status = "Skipped role assignment due to DB user issue"

                conn.commit() # Commit transaction if everything successful
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

def main():
    logger.info("=== Starting Unified User and Role Provisioning Process ===")

    # Initialize clients
    keycloak_client = KeycloakClient(KEYCLOAK_URL, REALM_NAME, CLIENT_ID, CLIENT_SECRET)
    bonita_client = BonitaClient(BONITA_URL, BONITA_ADMIN_USER, BONITA_ADMIN_PASS)

    # Get job titles from DB (needed for DB user insertion)
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cargos_dict = get_job_titles_from_db(cursor)
    except Exception as e:
        logger.critical(f"Failed to initialize: Could not retrieve job titles from DB. Exiting. {e}")
        return

    processed_results = []
    unique_rows_to_process = []
    seen_keys = set() # To track unique combinations of (document, location, role)

    try:
        with open(CSV_FILE, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for i, row in enumerate(reader):
                # Validate essential fields are not empty
                if not all(row.get(col) for col in ['document', 'first_name', 'last_name', 'email', 'job_title', 'user_type', 'location_name', 'role_name']):
                    logger.warning(f"Skipping row {i+2} due to missing essential fields: {row}")
                    # Capture skipped rows for the report if needed, or just log
                    processed_results.append((
                        f"{row.get('first_name', '')} {row.get('last_name', '')}",
                        row.get('document', 'N/A'),
                        row.get('email', 'N/A'),
                        row.get('job_title', 'N/A'),
                        row.get('user_type', 'N/A'),
                        row.get('location_name', 'N/A'),
                        row.get('role_name', 'N/A'),
                        "❌ Skipped (Missing Data)",
                        "❌ Skipped (Missing Data)"
                    ))
                    continue

                # Create a unique key for deduplication
                row_key = (row['document'], row['location_name'], row['role_name'])
                if row_key in seen_keys:
                    logger.info(f"Skipping duplicate entry: {row_key}")
                    processed_results.append((
                        f"{row['first_name']} {row['last_name']}",
                        row['document'],
                        row['email'],
                        row['job_title'],
                        row['user_type'],
                        row['location_name'],
                        row['role_name'],
                        "⚠️ Skipped (Duplicate Entry)",
                        "⚠️ Skipped (Duplicate Entry)"
                    ))
                else:
                    unique_rows_to_process.append(row)
                    seen_keys.add(row_key)

        if not unique_rows_to_process:
            logger.info("No valid unique user entries found in the CSV to process.")
            generate_excel_report(processed_results)
            return

        # Process users concurrently
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = [executor.submit(process_user_entry, row, keycloak_client, bonita_client, cargos_dict)
                       for row in unique_rows_to_process]
            for future in futures:
                processed_results.append(future.result())

    except FileNotFoundError:
        logger.critical(f"Error: CSV file '{CSV_FILE}' not found. Please ensure it's in the correct directory.")
        return
    except Exception as e:
        logger.critical(f"An unexpected error occurred during CSV reading or main processing: {e}", exc_info=True)
        return
    finally:
        # Always generate a report even if errors occurred
        generate_excel_report(processed_results)
        logger.info("✅ Process finalized.")


if __name__ == "__main__":
    main()