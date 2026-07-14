"""Télécharge le référentiel officiel des communes françaises (geo.api.gouv.fr) et le
sauvegarde en local (JSON). Exécuté une fois au build de l'image Docker (voir Dockerfile) :
le référentiel change rarement, pas besoin de le retélécharger à chaque démarrage.
"""
import json
import logging
import sys

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

COMMUNES_URL = "https://geo.api.gouv.fr/communes?fields=nom,code,codesPostaux,centre,population"
OUTPUT_FILE = "/data/communes.json"


def fetch_communes() -> list[dict]:
    logger.info(f"Téléchargement du référentiel communes: {COMMUNES_URL}")
    resp = requests.get(COMMUNES_URL, timeout=60)
    resp.raise_for_status()
    communes = resp.json()
    logger.info(f"{len(communes)} communes reçues")
    return communes


if __name__ == "__main__":
    try:
        communes = fetch_communes()
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(communes, f, ensure_ascii=False)
        logger.info(f"Référentiel sauvegardé: {OUTPUT_FILE} ({len(communes)} communes)")
    except Exception as e:
        logger.error(f"Échec téléchargement référentiel: {e}")
        sys.exit(1)
