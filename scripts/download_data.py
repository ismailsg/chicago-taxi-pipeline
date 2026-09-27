import argparse
import csv
import os
import sys
import time
from datetime import datetime

import requests


BASE_URL = "https://data.cityofchicago.org/resource/wrvz-psew.csv"


def get_last_timestamp(out_path: str):
    """
    Lit la dernière ligne du CSV et récupère son trip_start_timestamp.
    Permet de reprendre un téléchargement existant.
    """
    if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
        return None

    with open(out_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        last_timestamp = None

        for row in reader:
            value = row.get("trip_start_timestamp")

            if value:
                last_timestamp = value

        return last_timestamp


def download(
    start: str,
    end: str,
    out_path: str,
    page_size: int,
    max_rows: int,
    retries: int,
):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    existing_rows = 0
    last_timestamp = None

    # Vérifier si un fichier existe déjà
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:

        print("Fichier existant détecté.")
        print("Recherche du dernier timestamp...")

        with open(out_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)

            for row in reader:
                existing_rows += 1

                timestamp = row.get("trip_start_timestamp")

                if timestamp:
                    last_timestamp = timestamp

        print(f"Lignes déjà présentes : {existing_rows}")
        print(f"Dernier timestamp : {last_timestamp}")

    else:
        print("Aucun fichier existant. Début du téléchargement.")

    if existing_rows >= max_rows:
        print(f"Le fichier contient déjà {existing_rows} lignes.")
        print("Téléchargement terminé.")
        return

    total_rows = existing_rows

    # Si on reprend un fichier existant,
    # on commence après son dernier timestamp.
    current_start = last_timestamp if last_timestamp else f"{start}T00:00:00"

    mode = "a" if existing_rows > 0 else "w"

    with open(
        out_path,
        mode,
        newline="",
        encoding="utf-8",
    ) as f_out:

        writer = None

        while total_rows < max_rows:

            remaining = max_rows - total_rows
            current_limit = min(page_size, remaining)

            # Première requête
            if last_timestamp is None:
                where = (
                    f"trip_start_timestamp >= '{current_start}' "
                    f"AND trip_start_timestamp <= '{end}T23:59:59'"
                )

            else:
                where = (
                    f"trip_start_timestamp > '{last_timestamp}' "
                    f"AND trip_start_timestamp <= '{end}T23:59:59'"
                )

            params = {
                "$where": where,
                "$limit": current_limit,
                "$order": "trip_start_timestamp",
            }

            success = False

            for attempt in range(1, retries + 1):

                print(
                    f"Requête : timestamp>{last_timestamp}, "
                    f"limit={current_limit}, "
                    f"tentative={attempt}/{retries}"
                )

                try:
                    response = requests.get(
                        BASE_URL,
                        params=params,
                        timeout=180,
                    )

                    response.raise_for_status()

                    lines = response.text.splitlines()

                    if len(lines) <= 1:
                        print("Plus de données disponibles.")
                        success = True
                        return

                    reader = csv.reader(lines)

                    rows = list(reader)

                    header = rows[0]
                    data_rows = rows[1:]

                    if writer is None:
                        writer = csv.writer(f_out)

                        if existing_rows == 0:
                            writer.writerow(header)

                    if not data_rows:
                        print("Aucune nouvelle ligne.")
                        return

                    # Écriture des données
                    for row in data_rows:
                        writer.writerow(row)

                    f_out.flush()

                    total_rows += len(data_rows)

                    # Récupérer le timestamp de la dernière ligne
                    timestamp_index = header.index("trip_start_timestamp")

                    new_last_timestamp = data_rows[-1][timestamp_index]

                    # Sécurité contre une boucle infinie
                    if (
                        last_timestamp is not None
                        and new_last_timestamp <= last_timestamp
                    ):
                        print(
                            "ERREUR : le timestamp n'avance plus."
                        )
                        return

                    last_timestamp = new_last_timestamp

                    print(
                        f"  -> {total_rows:,} lignes téléchargées"
                    )

                    print(
                        f"  -> dernier timestamp : {last_timestamp}"
                    )

                    success = True
                    break

                except requests.exceptions.RequestException as e:

                    print(f"Erreur : {e}")

                    if attempt < retries:

                        wait_time = 5 * attempt

                        print(
                            f"Nouvelle tentative dans "
                            f"{wait_time} secondes..."
                        )

                        time.sleep(wait_time)

                    else:

                        print(
                            "Nombre maximum de tentatives atteint."
                        )

                        print(
                            "Le téléchargement peut être repris "
                            "avec le même fichier."
                        )

                        return

            if not success:
                return

            # Petite pause pour éviter de surcharger l'API
            time.sleep(0.5)

    print()
    print("========================================")
    print("Téléchargement terminé")
    print("========================================")
    print(f"Lignes totales : {total_rows:,}")
    print(f"Fichier : {out_path}")
    print(f"Dernier timestamp : {last_timestamp}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Téléchargement des Chicago Taxi Trips via SODA API"
    )

    parser.add_argument(
        "--start",
        required=True,
        help="Date de début YYYY-MM-DD",
    )

    parser.add_argument(
        "--end",
        required=True,
        help="Date de fin YYYY-MM-DD",
    )

    parser.add_argument(
        "--out",
        default="./data/raw/taxi_trips_2023.csv",
        help="Fichier CSV de sortie",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=25000,
        help="Nombre de lignes par requête",
    )

    parser.add_argument(
        "--max-rows",
        type=int,
        default=3_000_000,
        help="Nombre maximum de lignes à télécharger",
    )

    parser.add_argument(
        "--retries",
        type=int,
        default=5,
        help="Nombre de tentatives en cas d'erreur",
    )

    args = parser.parse_args()

    download(
        start=args.start,
        end=args.end,
        out_path=args.out,
        page_size=args.limit,
        max_rows=args.max_rows,
        retries=args.retries,
    )

    sys.exit(0) 