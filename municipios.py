import psycopg2
import requests
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple


# ==============================
# CONFIGURACIÓN
# ==============================

DB_CONFIG = {
 "host": "34.75.252.120",
 "port": 5432,
 "dbname": "gcpprolinktic",
 "user": "postgres",
 "password": "rfcw33nN73ae4DE",
}

# idPais de Colombia en tu tabla "departamento"
ID_PAIS_COLOMBIA = 425  # <-- cámbialo según tu caso

# Endpoint oficial de ciudades/municipios de Colombia
API_MUNICIPIOS_COLOMBIA = "https://api-colombia.com/api/v1/City"


# ==============================
# MODELOS / UTILIDADES
# ==============================

@dataclass
class MunicipioColombiaRef:
    departamento: str
    municipio: str
    departamento_id: Optional[int] = None


@dataclass
class IndiceMunicipiosReferencia:
    combos: Set[Tuple[str, str]]
    muni_to_deptos: Dict[str, Set[str]]
    municipios_unicos: Set[str]


DESCRIPTORES_RUIDO = (
    "MUNICIPIO DE ",
    "MUNICIPIO DEL ",
    "CIUDAD DE ",
    "CIUDAD DEL ",
    "CORREGIMIENTO DE ",
    "CORREGIMIENTO DEL ",
    "DISTRITO DE ",
    "DISTRITO DEL ",
    "DEPARTAMENTO DE ",
    "DEPARTAMENTO DEL ",
)

MUNICIPIO_ALIASES = {
    "BOGOTA D C": "BOGOTA",
    "BOGOTA DC": "BOGOTA",
    "BOGOTA DISTRITO CAPITAL": "BOGOTA",
    "BOGOTA D": "BOGOTA",
    "PROVIDENCIA": "PROVIDENCIA Y SANTA CATALINA",
    "PROVIDENCIA Y SANTA CATALINA ISLAS": "PROVIDENCIA Y SANTA CATALINA",
    "SAN ANDRES": "SAN ANDRES",
    "SAN ANDRES ISLAS": "SAN ANDRES",
    "SAN ANDRES Y PROVIDENCIA": "SAN ANDRES",
    "SAN ANDRES Y PROVIDENCIA Y SANTA CATALINA": "SAN ANDRES",
    "SANTA FE DE BOGOTA": "BOGOTA",
    "SANTA FE": "SANTAFE",
}

DEPARTAMENTO_ALIASES = {
    "BOGOTA D C": "BOGOTA",
    "BOGOTA DC": "BOGOTA",
    "BOGOTA DISTRITO CAPITAL": "BOGOTA",
    "ARCHIPIELAGO DE SAN ANDRES": "ARCHIPIELAGO DE SAN ANDRES PROVIDENCIA Y SANTA CATALINA",
    "SAN ANDRES": "ARCHIPIELAGO DE SAN ANDRES PROVIDENCIA Y SANTA CATALINA",
    "ARCHIPIELAGO DE SAN ANDRES PROVIDENCIA Y SANTA CATALINA": "ARCHIPIELAGO DE SAN ANDRES PROVIDENCIA Y SANTA CATALINA",
    "VALLE": "VALLE DEL CAUCA",
    "VALLE DEL CAUCA": "VALLE DEL CAUCA",
    "NORTE DE SANTANDER": "NORTE DE SANTANDER",
    "SUR DE SANTANDER": "SANTANDER",
}


def normalizar_texto(texto: str) -> str:
    """
    Normaliza un texto:
    - Convierte a mayúsculas
    - Quita tildes
    - Quita caracteres no alfanuméricos (excepto espacio)
    - Compacta espacios múltiples
    """
    if texto is None:
        return ""

    nfkd_form = unicodedata.normalize("NFKD", texto)
    sin_tildes = "".join(c for c in nfkd_form if not unicodedata.combining(c))
    sin_tildes = sin_tildes.upper()

    filtrado = []
    for c in sin_tildes:
        if c.isalnum() or c.isspace():
            filtrado.append(c)
    resultado = "".join(filtrado)
    resultado = " ".join(resultado.split())
    return resultado


def limpiar_descriptores_genericos(texto: str) -> str:
    for prefijo in DESCRIPTORES_RUIDO:
        if texto.startswith(prefijo):
            texto = texto[len(prefijo) :]
    return texto.strip()


def normalizar_con_alias(texto: str, alias_map: Dict[str, str]) -> Tuple[str, bool]:
    normalizado = normalizar_texto(texto)
    if not normalizado:
        return "", False
    normalizado = limpiar_descriptores_genericos(normalizado)
    alias = alias_map.get(normalizado, normalizado)
    return alias, alias != normalizado


# ==============================
# API: municipios oficiales de Colombia (API-Colombia)
# ==============================


def obtener_departamentos_colombia_desde_api() -> Dict[int, str]:
    resp = requests.get("https://api-colombia.com/api/v1/Department", timeout=60)
    resp.raise_for_status()
    data = resp.json()
    departamentos: Dict[int, str] = {}
    for item in data:
        dept_id = item.get("id")
        nombre = item.get("name") or ""
        if dept_id is None:
            continue
        departamentos[dept_id] = nombre
    return departamentos


def obtener_municipios_colombia_desde_api() -> List[MunicipioColombiaRef]:
    """
    Llama a https://api-colombia.com/api/v1/City y construye la lista
    de municipios oficiales de Colombia con su departamento.
    """
    resp = requests.get(API_MUNICIPIOS_COLOMBIA, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    municipios: List[MunicipioColombiaRef] = []
    departamentos_map = obtener_departamentos_colombia_desde_api()

    # Estructura esperada según docs:
    # [
    #   {
    #     "id": 1,
    #     "name": "Bogotá",
    #     "departmentId": 11,
    #     "department": {
    #        "id": 11,
    #        "name": "Cundinamarca",
    #        ...
    #     },
    #     ...
    #   },
    #   ...
    # ]
    for item in data:
        nombre_muni = item.get("name", "")
        dept = item.get("department") or {}
        nombre_depto = dept.get("name", "")
        dept_id = item.get("departmentId")
        if not nombre_depto and dept_id in departamentos_map:
            nombre_depto = departamentos_map.get(dept_id, "")

        if not nombre_muni:
            continue

        municipios.append(
            MunicipioColombiaRef(
                departamento=nombre_depto,
                municipio=nombre_muni,
                departamento_id=dept_id,
            )
        )

    return municipios


def construir_indices_municipios_colombia_ref(
    municipios_ref: List[MunicipioColombiaRef],
) -> IndiceMunicipiosReferencia:
    """
    Construye estructuras de referencia para validar municipios
    considerando normalización y alias.
    """
    combos: Set[Tuple[str, str]] = set()
    muni_to_deptos: Dict[str, Set[str]] = defaultdict(set)

    for ref in municipios_ref:
        muni_norm, _ = normalizar_con_alias(ref.municipio, MUNICIPIO_ALIASES)
        depto_norm, _ = normalizar_con_alias(ref.departamento, DEPARTAMENTO_ALIASES)

        if not muni_norm:
            continue

        combos.add((muni_norm, depto_norm))
        if depto_norm:
            muni_to_deptos[muni_norm].add(depto_norm)

    municipios_unicos = {m for m, deptos in muni_to_deptos.items() if len(deptos) == 1}

    return IndiceMunicipiosReferencia(
        combos=combos,
        muni_to_deptos=muni_to_deptos,
        municipios_unicos=municipios_unicos,
    )


def evaluar_coincidencia(
    muni_norm: str, depto_norm: str, indice: IndiceMunicipiosReferencia
) -> Tuple[bool, str, List[str]]:
    """Retorna (coincide, etiqueta, departamentos_oficiales)."""
    clave = (muni_norm, depto_norm)
    if clave in indice.combos:
        return True, "match_directo", sorted(indice.muni_to_deptos.get(muni_norm, []))

    deptos_validos = sorted(indice.muni_to_deptos.get(muni_norm, []))

    if deptos_validos:
        if not depto_norm:
            if len(deptos_validos) == 1:
                return True, "match_por_municipio_unico", deptos_validos
            return False, "departamento_requerido", deptos_validos

        if depto_norm in deptos_validos:
            # Debería haber coincidido, pero por seguridad se considera match.
            return True, "match_directo", deptos_validos

        return False, "departamento_diferente", deptos_validos

    return False, "municipio_no_en_api", deptos_validos


# ==============================
# BD: consulta de municipios en tu sistema
# ==============================

def obtener_municipios_bd() -> List[Dict]:
    """
    Obtiene todos los municipios con idPais = ID_PAIS_COLOMBIA,
    junto con el nombre del departamento.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    m.idmunicipio,
                    m.nombre AS nombre_municipio,
                    d.nombre AS nombre_departamento,
                    d.idpais,
                    m.codigodivipola,
                    m.idgeodivision,
                    m.origen
                FROM municipio m
                JOIN departamento d ON m.iddepartamento = d.iddepartamento
                WHERE d.idpais = %s;
                """,
                (ID_PAIS_COLOMBIA,),
            )
            rows = cur.fetchall()

        cols = [
            "idmunicipio",
            "nombre_municipio",
            "nombre_departamento",
            "idpais",
            "codigodivipola",
            "idgeodivision",
            "origen",
        ]

        municipios = [dict(zip(cols, row)) for row in rows]
        return municipios
    finally:
        conn.close()


# ==============================
# LÓGICA PRINCIPAL
# ==============================

def identificar_municipios_no_colombianos():
    print("Consultando municipios oficiales de Colombia desde API-Colombia...")
    municipios_ref = obtener_municipios_colombia_desde_api()
    print(f"Total municipios oficiales obtenidos desde API: {len(municipios_ref)}")

    print("Construyendo conjunto normalizado de referencia...")
    indice_ref = construir_indices_municipios_colombia_ref(municipios_ref)
    print(f"Total combinaciones municipio+departamento en referencia: {len(indice_ref.combos)}")
    print(f"Municipios únicos por departamento: {len(indice_ref.municipios_unicos)}")

    print("Consultando municipios de Colombia en la BD...")
    municipios_bd = obtener_municipios_bd()
    print(f"Total municipios en BD con idPais = {ID_PAIS_COLOMBIA}: {len(municipios_bd)}")

    no_coinciden = []
    motivos_counter: Counter[str] = Counter()
    stats: Counter[str] = Counter()

    for m in municipios_bd:
        muni_norm, alias_muni = normalizar_con_alias(
            m.get("nombre_municipio"), MUNICIPIO_ALIASES
        )
        depto_norm, alias_depto = normalizar_con_alias(
            m.get("nombre_departamento"), DEPARTAMENTO_ALIASES
        )

        coincide, etiqueta, deptos_validos = evaluar_coincidencia(
            muni_norm, depto_norm, indice_ref
        )

        if coincide:
            if etiqueta == "match_por_municipio_unico":
                stats["match_por_municipio_unico"] += 1
            elif alias_muni or alias_depto:
                stats["match_por_alias"] += 1
            else:
                stats["match_directo"] += 1
            continue

        stats["no_oficiales"] += 1
        motivos_counter[etiqueta] += 1

        no_coinciden.append(
            {
                **m,
                "nombre_municipio_norm": muni_norm,
                "nombre_departamento_norm": depto_norm,
                "tipo_no_coincidencia": etiqueta,
                "departamentos_oficiales": "|".join(deptos_validos),
            }
        )

    print("\n========================")
    print("MUNICIPIOS QUE NO COINCIDEN CON LA LISTA OFICIAL (API-COLOMBIA)")
    print("========================")
    print(f"Total: {len(no_coinciden)}\n")

    for m in no_coinciden[:200]:  # mostrar solo los primeros 200 en consola
        print(
            f"ID: {m['idmunicipio']}, "
            f"Municipio: {m['nombre_municipio']} ({m['nombre_municipio_norm']}), "
            f"Departamento: {m['nombre_departamento']} ({m['nombre_departamento_norm']}), "
            f"DIVIPOLA: {m['codigodivipola']}, "
            f"idGeo: {m['idgeodivision']}, "
            f"origen: {m['origen']}"
        )

    print("\nResumen de coincidencias:")
    print(f"- Match directo: {stats['match_directo']}")
    print(f"- Match usando alias: {stats['match_por_alias']}")
    print(f"- Match por municipio único: {stats['match_por_municipio_unico']}")
    print(f"- Posibles no oficiales: {stats['no_oficiales']}")

    if motivos_counter:
        print("\nMotivos más frecuentes sin coincidencia:")
        for motivo, cantidad in motivos_counter.most_common(10):
            print(f"  · {motivo}: {cantidad}")

    if no_coinciden:

        import csv

        filename = "municipios_no_oficiales_colombia.csv"
        print(f"\nGuardando detalle completo en {filename} ...")
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=no_coinciden[0].keys())
            writer.writeheader()
            writer.writerows(no_coinciden)
        print("Archivo CSV generado correctamente.")


# ==============================
# MAIN
# ==============================

if __name__ == "__main__":
    identificar_municipios_no_colombianos()
