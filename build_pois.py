"""Télécharge l'extrait OSM de la France entière (Geofabrik, ~5 Go, mis à jour quotidien-
nement en amont) et en extrait les POI (restaurants, commerces, services...) dans une base
SQLite locale — pour ne plus dépendre d'Overpass API (serveur public partagé, sujet à des
timeouts sous charge) pour les recherches "un restaurant près de X".

Exécuté au build de l'image Docker (voir Dockerfile). Le fichier PBF (~5 Go) est supprimé
en fin de script — seule la base SQLite résultante (bien plus petite) est conservée dans
l'image. Pour rafraîchir les données, il suffit de reconstruire l'image.
"""
import logging
import os
import sqlite3
import sys
import time

import osmium
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PBF_URL = "https://download.geofabrik.de/europe/france-latest.osm.pbf"
PBF_PATH = "/tmp/france.osm.pbf"
DB_PATH = "/data/pois.db"

# category (ce que places/poi_request expose côté 'amenity') -> tags OSM (key, value) à
# matcher. Couvre amenity=*, shop=*, tourism=*, leisure=* — pas juste amenity=* qui rate
# une bonne partie du commerce (boulangeries, supermarchés... taggés shop=*).
CATEGORY_TAGS = {
    "restaurant": [("amenity", "restaurant")],
    "fast_food": [("amenity", "fast_food")],
    "cafe": [("amenity", "cafe")],
    "bar": [("amenity", "bar"), ("amenity", "pub")],
    "pharmacy": [("amenity", "pharmacy")],
    "bakery": [("shop", "bakery")],
    "supermarket": [("shop", "supermarket"), ("shop", "convenience")],
    "fuel": [("amenity", "fuel")],
    "bank": [("amenity", "bank")],
    "hospital": [("amenity", "hospital"), ("amenity", "clinic")],
    "doctor": [("amenity", "doctors"), ("amenity", "dentist")],
    "cinema": [("amenity", "cinema")],
    "theatre": [("amenity", "theatre")],
    "museum": [("tourism", "museum")],
    "hotel": [("tourism", "hotel"), ("tourism", "guest_house"), ("tourism", "hostel")],
    "hairdresser": [("shop", "hairdresser")],
    "butcher": [("shop", "butcher")],
    "clothes": [("shop", "clothes")],
    "bookshop": [("shop", "books")],
    "florist": [("shop", "florist")],
    "park": [("leisure", "park")],
    "sports_centre": [("leisure", "sports_centre"), ("leisure", "fitness_centre")],
    "library": [("amenity", "library")],
    "post_office": [("amenity", "post_office")],
    "veterinary": [("amenity", "veterinary")],
}

TAG_TO_CATEGORY = {
    (key, value): category
    for category, tag_pairs in CATEGORY_TAGS.items()
    for key, value in tag_pairs
}


def download_pbf():
    logger.info(f"Téléchargement {PBF_URL} -> {PBF_PATH}")
    resp = requests.get(PBF_URL, stream=True, timeout=300)
    resp.raise_for_status()
    total = int(resp.headers.get("content-length", 0))
    written = 0
    last_log = time.time()
    with open(PBF_PATH, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
            f.write(chunk)
            written += len(chunk)
            if time.time() - last_log > 10:
                pct = 100 * written / total if total else 0
                logger.info(f"  {written / 1e6:.0f} Mo / {total / 1e6:.0f} Mo ({pct:.0f}%)")
                last_log = time.time()
    logger.info(f"Téléchargement terminé: {written / 1e6:.0f} Mo")


class POIHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.pois = []
        self.count_by_source = {"node": 0, "way": 0}

    def _match(self, tags):
        for key in ("amenity", "shop", "tourism", "leisure"):
            val = tags.get(key)
            if val and (key, val) in TAG_TO_CATEGORY:
                return TAG_TO_CATEGORY[(key, val)]
        return None

    def node(self, n):
        category = self._match(n.tags)
        if category and n.tags.get("name"):
            self.pois.append((n.tags.get("name"), category, n.tags.get("cuisine", ""), n.location.lat, n.location.lon))
            self.count_by_source["node"] += 1

    def way(self, w):
        category = self._match(w.tags)
        if not category or not w.tags.get("name"):
            return
        lats = [n.location.lat for n in w.nodes if n.location.valid()]
        lons = [n.location.lon for n in w.nodes if n.location.valid()]
        if not lats:
            return
        self.pois.append((w.tags.get("name"), category, w.tags.get("cuisine", ""), sum(lats) / len(lats), sum(lons) / len(lons)))
        self.count_by_source["way"] += 1


def extract_pois() -> list[tuple]:
    idx = osmium.index.create_map("sparse_mem_array")
    lh = osmium.NodeLocationsForWays(idx)
    lh.ignore_errors()

    logger.info("Extraction des POI en cours (peut prendre ~20 minutes pour la France entière)...")
    start = time.time()
    handler = POIHandler()
    osmium.apply(PBF_PATH, lh, handler)
    elapsed = time.time() - start
    logger.info(f"Extraction terminée en {elapsed:.0f}s: {len(handler.pois)} POI "
                f"({handler.count_by_source['node']} nodes, {handler.count_by_source['way']} ways)")
    return handler.pois


def build_db(pois: list[tuple]):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE pois (
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            cuisine TEXT,
            lat REAL NOT NULL,
            lon REAL NOT NULL
        )
    """)
    conn.executemany("INSERT INTO pois (name, category, cuisine, lat, lon) VALUES (?, ?, ?, ?, ?)", pois)
    # Index composite (category, lat, lon) : la recherche filtre toujours par catégorie
    # d'abord, puis restreint par une bounding box sur lat/lon avant le raffinement
    # haversine — cf. main.py._search_poi_local.
    conn.execute("CREATE INDEX idx_category_lat_lon ON pois (category, lat, lon)")
    conn.commit()
    conn.close()
    logger.info(f"Base SQLite construite: {DB_PATH} ({len(pois)} POI)")


if __name__ == "__main__":
    try:
        download_pbf()
        pois = extract_pois()
        build_db(pois)
        os.remove(PBF_PATH)
        logger.info(f"PBF supprimé ({PBF_PATH}) — pipeline terminé")
    except Exception as e:
        logger.error(f"Échec pipeline POI: {e}")
        sys.exit(1)
