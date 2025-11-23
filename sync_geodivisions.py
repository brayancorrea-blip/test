#!/usr/bin/env python3
"""
Herramienta para sincronizar departamentos y municipios externos usando el backend SGDEA.

Características:
- Descarga los países configurados en SGDEA y filtra los que tienen idPositiva (externos).
- Usa hilos (ThreadPoolExecutor) con el número de cores disponibles para paralelizar
  la sincronización por país y por departamento.
- Invoca la API externa de Positiva para obtener divisiones/muncipios y luego reusa los
  endpoints locales `/geodivision/actualizar-lote-*` para persistir reutilizando la lógica
  de `GeoDivisionPersistenceService` (sin duplicados).
- Evita reenvíos duplicados deduplicando por `idDivisionPolitica` e `idMunicipio`.

Requisitos:
- Python 3.10+
- Dependencias: requests (`pip install requests`)
- Variables necesarias: token Bearer válido para el backend (y opcionalmente uno para Positiva).
"""

from __future__ import annotations

import argparse
import concurrent.futures
import logging
import os
import threading
import time
from typing import Dict, Iterable, List, Optional, Set, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry

POSITIVA_BASE_URL = "https://core-positiva-apis-pre-apicast-staging.apps.openshift4.positiva.gov.co"

thread_local = threading.local()


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
        key = dep.get("idDivisionPolitica") or dep.get("idDepartamento")
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
        key = muni.get("consecutivo") or muni.get("conscutivo") or muni.get("idMunicipio")
        if key is None:
            continue
        dedup.setdefault(int(key), muni)
    return list(dedup.values())


def build_departamento_payload(external_id: int, dep: Dict) -> Dict:
    division_id = dep.get("idDivisionPolitica") or dep.get("idDepartamento")
    nombre = dep.get("nombreDepartamento") or dep.get("nombreEstado") or dep.get("nombre")
    return {
        "idPais": external_id,
        "idDepartamento": division_id,
        "idDivisionPolitica": division_id,
        "nombreDepartamento": nombre,
        "nombreEstado": dep.get("nombreEstado", nombre),
    }


def build_municipio_payload(
    external_id: int, dep_division_id: int, dep_remote_id: int, municipio: Dict
) -> Dict:
    consecutivo = municipio.get("consecutivo") or municipio.get("conscutivo") or municipio.get("idMunicipio")
    return {
        "idPais": external_id,
        "idDepartamento": dep_remote_id,
        "idDivisionPolitica": dep_division_id,
        "idMunicipio": consecutivo,
        "nombreMunicipio": municipio.get("nombreCiudad") or municipio.get("nombreMunicipio"),
        "divipola": municipio.get("codigoDivipola"),
    }


def persist_departments(
    backend_url: str, token: str, departments: List[Dict], chunk_size: int
) -> None:
    if not departments:
        return
    session = get_backend_session(token)
    url = f"{backend_url.rstrip('/')}/geodivision/actualizar-lote-departamento"
    for batch in chunked(departments, chunk_size):
        resp = session.post(url, json=batch, timeout=120)
        resp.raise_for_status()


def persist_municipios(
    backend_url: str, token: str, municipios: List[Dict], chunk_size: int
) -> None:
    if not municipios:
        return
    session = get_backend_session(token)
    url = f"{backend_url.rstrip('/')}/geodivision/actualizar-lote-municipios"
    for batch in chunked(municipios, chunk_size):
        resp = session.post(url, json=batch, timeout=120)
        resp.raise_for_status()


def sync_country(
    country: Dict,
    backend_url: str,
    backend_token: str,
    external_token: str,
    throttle_ms: int,
    chunk_size: int,
    municipality_workers: int,
) -> None:
    country_id = country["idPais"]
    external_id = country["idPositiva"]
    if external_id is None:
        logging.info("País %s no tiene idPositiva. Se omite.", country_id)
        return

    logging.info("País %s/%s: descargando departamentos.", country_id, external_id)
    remote_departments = fetch_departments_from_external(external_id, external_token, throttle_ms)
    if not remote_departments:
        logging.info("País %s/%s: sin departamentos en API externa.", country_id, external_id)
        return

    dept_payloads = [build_departamento_payload(external_id, dep) for dep in remote_departments]
    persist_departments(backend_url, backend_token, dept_payloads, chunk_size)
    logging.info("País %s/%s: %d departamentos enviados.", country_id, external_id, len(dept_payloads))

    municipios_payload: List[Dict] = []
    seen_municipios: Set[Tuple[int, int]] = set()

    def process_department_municipios(dep: Dict) -> None:
        division_id = dep.get("idDivisionPolitica") or dep.get("idDepartamento")
        remote_dep_id = dep.get("idDepartamento") or division_id
        fetched = fetch_municipios_from_external(external_id, division_id, external_token, throttle_ms)
        for muni in fetched:
            key = (int(remote_dep_id), int(muni.get("consecutivo") or muni.get("conscutivo") or muni.get("idMunicipio")))
            if key in seen_municipios:
                continue
            seen_municipios.add(key)
            municipios_payload.append(
                build_municipio_payload(external_id, division_id, remote_dep_id, muni)
            )

    with concurrent.futures.ThreadPoolExecutor(max_workers=municipality_workers) as muni_pool:
        list(muni_pool.map(process_department_municipios, remote_departments))

    persist_municipios(backend_url, backend_token, municipios_payload, chunk_size)
    logging.info(
        "País %s/%s: %d municipios enviados.",
        country_id,
        external_id,
        len(municipios_payload),
    )


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
        required=True,
        help="Token Bearer para invocar los endpoints del backend.",
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(asctime)s - %(message)s",
    )

    external_token = args.external_token or args.backend_token
    countries = fetch_countries(args.backend_url, args.backend_token)
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
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as pool:
        futures = [
            pool.submit(
                sync_country,
                country,
                args.backend_url,
                args.backend_token,
                external_token,
                args.throttle_ms,
                args.chunk_size,
                args.municipality_workers,
            )
            for country in targets
        ]

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                logging.exception("Error sincronizando país: %s", exc)

    logging.info("Sincronización finalizada.")


if __name__ == "__main__":
    main()

