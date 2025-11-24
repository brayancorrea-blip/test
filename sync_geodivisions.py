#!/usr/bin/env python3
"""
Herramienta para sincronizar departamentos y municipios externos usando el backend SGDEA.

Características:
- Descarga los países configurados en SGDEA y filtra los que tienen idPositiva (externos).
- Usa hilos (ThreadPoolExecutor) con el número de cores disponibles para paralelizar
  la sincronización por país y por departamento.
- Invoca la API externa de Positiva para obtener divisiones/municipios y luego reusa los
  endpoints locales `/geodivision/actualizar-lote-*` para persistir reutilizando la lógica
  de `GeoDivisionPersistenceService` (sin duplicados).
- Desde esta versión adapta dinámicamente el tamaño de los lotes cuando aparece un HTTP 5xx,
  genera un CSV con los fallos y puede auto-obtener los tokens del backend y de Positiva
  usando las credenciales suministradas (curl equivalente).

Requisitos:
- Python 3.10+
- Dependencias: requests (`pip install requests`)
- Variables necesarias: token Bearer válido para el backend (y opcionalmente uno para Positiva).
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import json
import logging
import os
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Set, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry

POSITIVA_BASE_URL = "https://core-positiva-apis-pre-apicast-staging.apps.openshift4.positiva.gov.co"
DEFAULT_BACKEND_TOKEN_URL = "http://localhost:8081/api/v1/autenticacion/token/all/platforms"
DEFAULT_EXTERNAL_TOKEN_URL = (
    "https://keycloak-sso-app.apps.openshift4.positiva.gov.co/"
    "auth/realms/apis-pre/protocol/openid-connect/token"
)

thread_local = threading.local()

@dataclass
class SyncFailure:
    country_id: int
    external_id: Optional[int]
    stage: str
    status: Optional[int]
    message: str
    payload_size: int = 0
    sample: Optional[str] = None


@dataclass
class SyncResult:
    country_id: int
    external_id: Optional[int]
    departments_sent: int = 0
    municipalities_sent: int = 0
    failures: List[SyncFailure] = field(default_factory=list)


def build_session(token: str) -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=5,
        read=5,
        connect=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
    )
    return session


def chunked(items: Iterable[dict], size: int) -> Iterable[List[dict]]:
    bucket: List[dict] = []
    for item in items:
        bucket.append(item)
        if len(bucket) >= size:
            yield bucket
            bucket = []
    if bucket:
        yield bucket


def throttle(delay_ms: int) -> None:
    if delay_ms > 0:
        time.sleep(delay_ms / 1000)


def extract_http_context(error: Exception) -> Tuple[Optional[int], Optional[str]]:
    response = getattr(error, "response", None)
    status = None
    body = None
    if response is not None:
        try:
            status = response.status_code
            body = (response.text or "")[:500]
        except Exception:  # pragma: no cover - acceso defensivo
            body = "<cuerpo no disponible>"
    return status, body


def serialize_sample(payload: Optional[dict]) -> Optional[str]:
    if not payload:
        return None
    try:
        return json.dumps(payload, ensure_ascii=False)[:500]
    except Exception:  # pragma: no cover - serialización defensiva
        return str(payload)[:500]


def write_failures_report(failures: List[SyncFailure], destination: str) -> str:
    directory = os.path.dirname(destination)
    if directory:
        os.makedirs(directory, exist_ok=True)

    fieldnames = [
        "country_id",
        "external_id",
        "stage",
        "status",
        "message",
        "payload_size",
        "sample",
    ]
    with open(destination, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for failure in failures:
            writer.writerow(
                {
                    "country_id": failure.country_id,
                    "external_id": failure.external_id,
                    "stage": failure.stage,
                    "status": failure.status or "",
                    "message": failure.message,
                    "payload_size": failure.payload_size,
                    "sample": failure.sample or "",
                }
            )
    return destination


def default_report_path() -> str:
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return os.path.join(os.getcwd(), "exports", f"sync_failures_{timestamp}.csv")


def obtain_backend_token(token_url: str, username: str, password: str) -> str:
    logging.info("Solicitando token del backend en %s para el usuario %s.", token_url, username)
    payload = {"username": username, "password": password}
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    try:
        resp = requests.post(token_url, json=payload, headers=headers, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"No fue posible obtener el token del backend: {exc}") from exc

    try:
        data = resp.json()
    except ValueError as exc:
        raise RuntimeError("La respuesta del backend no contiene JSON válido.") from exc

    token_section = (
        data.get("tokenIDP")
        or data.get("TokenIDP")
        or data.get("tokenIdp")
        or data.get("token_idp")
    )
    access_token = None
    if isinstance(token_section, dict):
        access_token = (
            token_section.get("accessToken")
            or token_section.get("AccessToken")
            or token_section.get("access_token")
        )
    elif isinstance(token_section, str):
        access_token = token_section

    if not access_token:
        access_token = (
            data.get("accessToken")
            or data.get("AccessToken")
            or data.get("access_token")
            or data.get("token")
        )

    if not access_token:
        raise RuntimeError("No se encontró el accessToken dentro de la respuesta del backend.")

    logging.info("Token del backend obtenido correctamente.")
    return access_token


def obtain_external_token(
    token_url: str,
    client_id: str,
    client_secret: str,
    cookie: Optional[str] = None,
) -> str:
    logging.info("Solicitando token externo en %s para el client_id %s.", token_url, client_id)
    headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}
    if cookie:
        headers["Cookie"] = cookie

    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
    }
    try:
        resp = requests.post(token_url, data=payload, headers=headers, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"No fue posible obtener el token externo: {exc}") from exc

    try:
        data = resp.json()
    except ValueError as exc:
        raise RuntimeError("La respuesta del servidor de autenticación externo no es JSON válida.") from exc

    token = data.get("access_token")
    if not token:
        raise RuntimeError("No se encontró el campo access_token en la respuesta externa.")

    logging.info("Token externo obtenido correctamente.")
    return token


def get_backend_session(token: str) -> requests.Session:
    if not hasattr(thread_local, "backend_session"):
        session = build_session(token)
        session.headers.update({"Content-Type": "application/json"})
        thread_local.backend_session = session
    return thread_local.backend_session


def get_external_session(token: str) -> requests.Session:
    if not hasattr(thread_local, "external_session"):
        thread_local.external_session = build_session(token)
    return thread_local.external_session


def fetch_countries(backend_url: str, token: str) -> List[Dict]:
    session = build_session(token)
    url = f"{backend_url.rstrip('/')}/geodivision"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_departments_from_external(external_id: int, token: str, throttle_ms: int) -> List[Dict]:
    session = get_external_session(token)
    throttle(throttle_ms)
    resp = session.get(
        f"{POSITIVA_BASE_URL}/pdp/v1/positiva/scp/parametrica/consultaDivisionPolitica",
        params={"idPais": external_id},
        timeout=60,
    )
    resp.raise_for_status()
    raw_departments = resp.json() or []

    dedup: Dict[int, Dict] = {}
    for dep in raw_departments:
        key = (
            dep.get("idDivisionPolitica")
            or dep.get("ID_DIVISION_POLITICA")
            or dep.get("idDepartamento")
            or dep.get("ID_DEPARTAMENTO")
        )
        if key is None:
            continue
        dedup.setdefault(int(key), dep)
    return list(dedup.values())


def fetch_municipios_from_external(
    external_id: int, division_id: int, token: str, throttle_ms: int
) -> List[Dict]:
    session = get_external_session(token)
    throttle(throttle_ms)
    resp = session.get(
        f"{POSITIVA_BASE_URL}/ciudades/v1/positiva/scp/parametrica/ConsultaCiudades",
        params={"idPais": external_id, "idDivisionPolitica": division_id},
        timeout=60,
    )
    resp.raise_for_status()
    raw_municipios = resp.json() or []

    dedup: Dict[int, Dict] = {}
    for muni in raw_municipios:
        key = (
            muni.get("consecutivo")
            or muni.get("CONSECUTIVO")
            or muni.get("conscutivo")
            or muni.get("idMunicipio")
            or muni.get("ID_MUNICIPIO")
        )
        if key is None:
            continue
        dedup.setdefault(int(key), muni)
    return list(dedup.values())


def build_departamento_payload(external_id: int, dep: Dict) -> Dict:
    division_id = (
        dep.get("idDivisionPolitica")
        or dep.get("ID_DIVISION_POLITICA")
        or dep.get("idDepartamento")
        or dep.get("ID_DEPARTAMENTO")
    )
    nombre = (
        dep.get("nombreDepartamento")
        or dep.get("NombreDepartamento")
        or dep.get("nombreEstado")
        or dep.get("NOMBRE_ESTADO")
        or dep.get("nombre")
    )
    return {
        "idPais": external_id,
        "idDepartamento": division_id,
        "idDivisionPolitica": division_id,
        "nombreDepartamento": nombre,
        "nombreEstado": dep.get("nombreEstado") or dep.get("NOMBRE_ESTADO") or nombre,
    }


def build_municipio_payload(
    external_id: int,
    dep_division_id: int,
    dep_remote_id: int,
    municipio: Dict,
    dep_name: Optional[str],
) -> Dict:
    consecutivo = (
        municipio.get("consecutivo")
        or municipio.get("CONSECUTIVO")
        or municipio.get("conscutivo")
        or municipio.get("idMunicipio")
        or municipio.get("ID_MUNICIPIO")
    )
    nombre_municipio = (
        municipio.get("nombreCiudad")
        or municipio.get("NOMBRE_CIUDAD")
        or municipio.get("nombreMunicipio")
        or municipio.get("NOMBRE_MUNICIPIO")
    )
    return {
        "idPais": external_id,
        "idDepartamento": dep_remote_id,
        "idDivisionPolitica": dep_division_id,
        "idMunicipio": consecutivo,
        "nombreDepartamento": dep_name,
        "nombreMunicipio": nombre_municipio,
        "divipola": municipio.get("codigoDivipola") or municipio.get("CODIGO_DIVIPOLA"),
    }


def persist_entities(
    endpoint_suffix: str,
    stage: str,
    backend_url: str,
    token: str,
    entities: List[Dict],
    chunk_size: int,
    country_id: int,
    external_id: int,
) -> Tuple[int, List[SyncFailure]]:
    if not entities:
        return 0, []

    session = get_backend_session(token)
    url = f"{backend_url.rstrip('/')}/{endpoint_suffix.lstrip('/')}"
    pending = deque(chunked(entities, chunk_size))
    sent = 0
    failures: List[SyncFailure] = []

    while pending:
        batch = pending.popleft()
        if not batch:
            continue
        try:
            resp = session.post(url, json=batch, timeout=120)
            resp.raise_for_status()
            sent += len(batch)
        except requests.HTTPError as exc:
            status, body = extract_http_context(exc)
            if status and 500 <= status < 600 and len(batch) > 1:
                new_size = max(1, len(batch) // 2)
                logging.warning(
                    "País %s/%s: HTTP %s en %s con lote de %d registros. Reintentando con lotes de %d.",
                    country_id,
                    external_id,
                    status,
                    stage,
                    len(batch),
                    new_size,
                )
                right = batch[new_size:]
                left = batch[:new_size]
                if right:
                    pending.appendleft(right)
                if left:
                    pending.appendleft(left)
                continue

            failures.append(
                SyncFailure(
                    country_id=country_id,
                    external_id=external_id,
                    stage=stage,
                    status=status,
                    message=f"HTTP error en {stage}: {exc}",
                    payload_size=len(batch),
                    sample=serialize_sample(batch[0]),
                )
            )
        except requests.RequestException as exc:
            failures.append(
                SyncFailure(
                    country_id=country_id,
                    external_id=external_id,
                    stage=stage,
                    status=None,
                    message=f"Request error en {stage}: {exc}",
                    payload_size=len(batch),
                    sample=serialize_sample(batch[0]),
                )
            )
        except Exception as exc:  # pragma: no cover - defensa general
            failures.append(
                SyncFailure(
                    country_id=country_id,
                    external_id=external_id,
                    stage=stage,
                    status=None,
                    message=f"Error inesperado en {stage}: {exc}",
                    payload_size=len(batch),
                    sample=serialize_sample(batch[0]),
                )
            )

    return sent, failures


def persist_departments(
    backend_url: str,
    token: str,
    departments: List[Dict],
    chunk_size: int,
    country_id: int,
    external_id: int,
) -> Tuple[int, List[SyncFailure]]:
    return persist_entities(
        "geodivision/actualizar-lote-departamento",
        "persist_departamentos",
        backend_url,
        token,
        departments,
        chunk_size,
        country_id,
        external_id,
    )


def persist_municipios(
    backend_url: str,
    token: str,
    municipios: List[Dict],
    chunk_size: int,
    country_id: int,
    external_id: int,
) -> Tuple[int, List[SyncFailure]]:
    return persist_entities(
        "geodivision/actualizar-lote-municipios",
        "persist_municipios",
        backend_url,
        token,
        municipios,
        chunk_size,
        country_id,
        external_id,
    )


def sync_country(
    country: Dict,
    backend_url: str,
    backend_token: str,
    external_token: str,
    throttle_ms: int,
    chunk_size: int,
    municipality_workers: int,
) -> SyncResult:
    country_id = country["idPais"]
    external_id = country.get("idPositiva")
    result = SyncResult(country_id=country_id, external_id=external_id or 0)

    if external_id is None:
        logging.info("País %s no tiene idPositiva. Se omite.", country_id)
        return result

    logging.info("País %s/%s: descargando departamentos.", country_id, external_id)
    try:
        remote_departments = fetch_departments_from_external(external_id, external_token, throttle_ms)
    except requests.HTTPError as exc:
        status, body = extract_http_context(exc)
        result.failures.append(
            SyncFailure(
                country_id=country_id,
                external_id=external_id,
                stage="fetch_departamentos",
                status=status,
                message=f"No se pudieron obtener departamentos: {exc}",
                sample=body,
            )
        )
        return result
    except requests.RequestException as exc:
        result.failures.append(
            SyncFailure(
                country_id=country_id,
                external_id=external_id,
                stage="fetch_departamentos",
                status=None,
                message=f"Error de red al obtener departamentos: {exc}",
            )
        )
        return result

    if not remote_departments:
        logging.info("País %s/%s: sin departamentos en API externa.", country_id, external_id)
        return result

    dept_payloads = [build_departamento_payload(external_id, dep) for dep in remote_departments]
    deps_sent, dep_failures = persist_departments(
        backend_url,
        backend_token,
        dept_payloads,
        chunk_size,
        country_id,
        external_id,
    )
    result.departments_sent = deps_sent
    result.failures.extend(dep_failures)
    logging.info(
        "País %s/%s: %d/%d departamentos enviados (%d fallos).",
        country_id,
        external_id,
        deps_sent,
        len(dept_payloads),
        len(dep_failures),
    )

    municipios_payload: List[Dict] = []
    seen_municipios: Set[Tuple[int, int]] = set()
    seen_lock = threading.Lock()
    payload_lock = threading.Lock()
    failures_lock = threading.Lock()

    def process_department_municipios(dep: Dict) -> None:
        division_id = (
            dep.get("idDivisionPolitica")
            or dep.get("ID_DIVISION_POLITICA")
            or dep.get("idDepartamento")
            or dep.get("ID_DEPARTAMENTO")
        )
        remote_dep_id = dep.get("idDepartamento") or dep.get("ID_DEPARTAMENTO") or division_id
        dep_name = (
            dep.get("nombreEstado")
            or dep.get("NOMBRE_ESTADO")
            or dep.get("nombreDepartamento")
            or dep.get("NombreDepartamento")
            or dep.get("nombre")
        )
        if division_id is None or remote_dep_id is None:
            failure = SyncFailure(
                country_id=country_id,
                external_id=external_id,
                stage="fetch_municipios",
                status=None,
                message="Departamento sin identificadores válidos",
                sample=serialize_sample(dep),
            )
            with failures_lock:
                result.failures.append(failure)
            return
        try:
            division_id_int = int(division_id)
            remote_dep_id_int = int(remote_dep_id)
        except (TypeError, ValueError):
            failure = SyncFailure(
                country_id=country_id,
                external_id=external_id,
                stage="fetch_municipios",
                status=None,
                message="Departamento con identificadores no numéricos",
                sample=serialize_sample(dep),
            )
            with failures_lock:
                result.failures.append(failure)
            return
        try:
            fetched = fetch_municipios_from_external(
                external_id,
                division_id_int,
                external_token,
                throttle_ms,
            )
        except requests.HTTPError as exc:
            status, body = extract_http_context(exc)
            failure = SyncFailure(
                country_id=country_id,
                external_id=external_id,
                stage="fetch_municipios",
                status=status,
                message=f"No se pudieron obtener municipios para departamento {remote_dep_id_int}: {exc}",
                sample=body,
            )
            with failures_lock:
                result.failures.append(failure)
            return
        except requests.RequestException as exc:
            failure = SyncFailure(
                country_id=country_id,
                external_id=external_id,
                stage="fetch_municipios",
                status=None,
                message=f"Error de red obteniendo municipios para departamento {remote_dep_id_int}: {exc}",
            )
            with failures_lock:
                result.failures.append(failure)
            return

        local_batch: List[Dict] = []
        for muni in fetched:
            muni_identifier = (
                muni.get("consecutivo")
                or muni.get("CONSECUTIVO")
                or muni.get("conscutivo")
                or muni.get("idMunicipio")
                or muni.get("ID_MUNICIPIO")
            )
            if muni_identifier is None:
                continue
            try:
                muni_id_int = int(muni_identifier)
            except (TypeError, ValueError):
                continue
            key = (remote_dep_id_int, muni_id_int)
            with seen_lock:
                if key in seen_municipios:
                    continue
                seen_municipios.add(key)
            local_batch.append(
                build_municipio_payload(external_id, division_id_int, remote_dep_id_int, muni, dep_name)
            )

        if local_batch:
            with payload_lock:
                municipios_payload.extend(local_batch)

    with concurrent.futures.ThreadPoolExecutor(max_workers=municipality_workers) as muni_pool:
        list(muni_pool.map(process_department_municipios, remote_departments))

    mun_sent, mun_failures = persist_municipios(
        backend_url,
        backend_token,
        municipios_payload,
        chunk_size,
        country_id,
        external_id,
    )
    result.municipalities_sent = mun_sent
    result.failures.extend(mun_failures)
    logging.info(
        "País %s/%s: %d/%d municipios enviados (%d fallos).",
        country_id,
        external_id,
        mun_sent,
        len(municipios_payload),
        len(mun_failures),
    )
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sincroniza departamentos y municipios externos usando hilos."
    )
    parser.add_argument(
        "--backend-url",
        default="http://localhost:8081",
        help="URL base del backend SGDEA (por ej. http://localhost:8081)",
    )
    parser.add_argument(
        "--backend-token",
        help="Token Bearer para invocar los endpoints del backend. Si se omite, se intentará obtener con credenciales.",
    )
    parser.add_argument(
        "--external-token",
        help="Token Bearer para la API externa de Positiva. Si se omite se usa el mismo del backend.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=os.cpu_count() or 4,
        help="Número máximo de hilos para procesar países.",
    )
    parser.add_argument(
        "--municipality-workers",
        type=int,
        default=max(2, (os.cpu_count() or 4) // 2),
        help="Número de hilos internos para municipios por país.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=250,
        help="Cantidad de registros por batch al persistir.",
    )
    parser.add_argument(
        "--throttle-ms",
        type=int,
        default=50,
        help="Retardo en ms entre llamadas a la API externa para evitar throttling.",
    )
    parser.add_argument(
        "--countries",
        type=int,
        nargs="*",
        help="IDs locales de país a sincronizar. Si se omite se procesan todos los externos.",
    )
    parser.add_argument(
        "--report-file",
        help="Ruta del CSV con los fallos detectados. Por defecto se guarda en ./exports.",
    )
    parser.add_argument(
        "--backend-token-url",
        default=DEFAULT_BACKEND_TOKEN_URL,
        help="Endpoint para solicitar el token del backend (POST JSON username/password).",
    )
    parser.add_argument(
        "--backend-username",
        help="Usuario a utilizar si se desea obtener automáticamente el token del backend.",
    )
    parser.add_argument(
        "--backend-password",
        help="Contraseña a utilizar si se desea obtener automáticamente el token del backend.",
    )
    parser.add_argument(
        "--external-token-url",
        default=DEFAULT_EXTERNAL_TOKEN_URL,
        help="Endpoint OAuth2 client_credentials del proveedor externo.",
    )
    parser.add_argument(
        "--external-client-id",
        help="Client ID para solicitar el token externo (client_credentials).",
    )
    parser.add_argument(
        "--external-client-secret",
        help="Client secret para solicitar el token externo (client_credentials).",
    )
    parser.add_argument(
        "--external-cookie",
        help="Cookie opcional requerida por el endpoint externo (si aplica).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(asctime)s - %(message)s",
    )

    backend_token = args.backend_token
    if not backend_token:
        if args.backend_username and args.backend_password:
            backend_token = obtain_backend_token(
                args.backend_token_url,
                args.backend_username,
                args.backend_password,
            )
        else:
            raise SystemExit(
                "Debe proporcionar --backend-token o las credenciales --backend-username/--backend-password."
            )

    external_token = args.external_token
    if not external_token:
        if args.external_client_id and args.external_client_secret:
            external_token = obtain_external_token(
                args.external_token_url,
                args.external_client_id,
                args.external_client_secret,
                args.external_cookie,
            )
        else:
            logging.info("No se proporcionó token externo ni credenciales. Se reutilizará el token del backend.")
            external_token = backend_token

    countries = fetch_countries(args.backend_url, backend_token)
    targets = [
        country
        for country in countries
        if country.get("idPositiva")
        and country.get("idPositiva") != 170
        and (not args.countries or country.get("idPais") in args.countries)
    ]

    if not targets:
        logging.warning("No se encontraron países externos para sincronizar.")
        return

    logging.info("Iniciando sincronización para %d países.", len(targets))
    results: List[SyncResult] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as pool:
        futures = [
            pool.submit(
                sync_country,
                country,
                args.backend_url,
                backend_token,
                external_token,
                args.throttle_ms,
                args.chunk_size,
                args.municipality_workers,
            )
            for country in targets
        ]

        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as exc:
                logging.exception("Error sincronizando país: %s", exc)

    total_departments = sum(r.departments_sent for r in results)
    total_municipios = sum(r.municipalities_sent for r in results)
    failures = [failure for r in results for failure in r.failures]
    logging.info(
        "Sincronización finalizada: %d países procesados, %d departamentos y %d municipios enviados.",
        len(results),
        total_departments,
        total_municipios,
    )

    if failures:
        destination = args.report_file or default_report_path()
        report_location = write_failures_report(failures, destination)
        logging.warning(
            "Se registraron %d fallos en %d países. Revisa el reporte: %s",
            len(failures),
            len({f.country_id for f in failures}),
            report_location,
        )
    else:
        logging.info("Todos los lotes se procesaron sin errores reportados.")


if __name__ == "__main__":
    main()

