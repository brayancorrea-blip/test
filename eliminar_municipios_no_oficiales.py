import csv
from pathlib import Path
from typing import List

import psycopg2

from municipios import DB_CONFIG

CSV_FILENAME = "municipios_no_oficiales_colombia.csv"
CHUNK_SIZE = 500


def cargar_ids_desde_csv(path: Path) -> List[int]:
    if not path.exists():
        raise FileNotFoundError(f"No se encontró el archivo {path}")

    with path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        ids = [
            int(row["idmunicipio"])
            for row in reader
            if row.get("idmunicipio") and row["idmunicipio"].isdigit()
        ]
    return ids


def eliminar_municipios(ids: List[int]) -> int:
    if not ids:
        print("No se encontraron IDs para eliminar.")
        return 0

    conn = psycopg2.connect(**DB_CONFIG)
    eliminados = 0
    try:
        with conn:
            with conn.cursor() as cur:
                for i in range(0, len(ids), CHUNK_SIZE):
                    chunk = ids[i : i + CHUNK_SIZE]
                    cur.execute(
                        "DELETE FROM municipio WHERE idmunicipio = ANY(%s) RETURNING idmunicipio",
                        (chunk,),
                    )
                    borrados = len(cur.fetchall())
                    eliminados += borrados
                    print(
                        f"Lote {i // CHUNK_SIZE + 1}: solicitados {len(chunk)}, eliminados {borrados}"
                    )
    finally:
        conn.close()

    return eliminados


def main():
    ruta_csv = Path(CSV_FILENAME)
    ids = cargar_ids_desde_csv(ruta_csv)
    print(f"Total de IDs detectados en {CSV_FILENAME}: {len(ids)}")
    total = eliminar_municipios(ids)
    print(f"\nEliminación concluida. Total eliminados: {total}")


if __name__ == "__main__":
    main()

