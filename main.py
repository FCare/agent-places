import asyncio
import json
import logging
import math
import os
import sqlite3
import sys

import aiohttp
from rapidfuzz import process, fuzz
from nexus_client import NexusClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

VK_URL = os.environ["VK_URL"]
MQTT_HOST = os.environ["MQTT_HOST"]
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
SERVICE_USERNAME = os.environ["MQTT_SERVICE_USERNAME"]
SERVICE_API_KEY = os.environ["MQTT_SERVICE_API_KEY"]

DATATOURISME_API_KEY = os.environ.get("DATATOURISME_API_KEY")

COMMUNES_FILE = os.environ.get("COMMUNES_FILE", "/data/communes.json")
POIS_DB_FILE = os.environ.get("POIS_DB_FILE", "/data/pois.db")
# Score rapidfuzz (0-100, plus haut = plus proche) — en dessous, on ne fait pas confiance
# au meilleur résultat local et on tente le repli BAN. Calibré empiriquement : une faute de
# frappe/absence de tiret atterrit à 85-100, une requête sans rapport (ou une déformation
# phonétique lourde type ASR) tombe sous 65-70.
LOCAL_MATCH_THRESHOLD = float(os.environ.get("LOCAL_MATCH_THRESHOLD", "70"))

AGENT_NAME = "places"
_subscribed_sessions: set[str] = set()


def _load_communes() -> list[dict]:
    with open(COMMUNES_FILE, encoding="utf-8") as f:
        communes = json.load(f)
    entries = []
    for c in communes:
        centre = c.get("centre") or {}
        coords = centre.get("coordinates")
        if not coords:
            continue
        lon, lat = coords
        entries.append({
            "name": c["nom"],
            "norm": c["nom"].replace("-", " ").lower(),
            "insee_code": c["code"],
            "postcode": (c.get("codesPostaux") or [""])[0],
            "lat": lat,
            "lon": lon,
            "population": c.get("population") or 0,
        })
    logger.info(f"{len(entries)} communes chargées depuis {COMMUNES_FILE}")
    return entries


_COMMUNES = _load_communes()
_NORM_NAMES = [c["norm"] for c in _COMMUNES]


def _search_local(query: str, n_results: int) -> list[dict]:
    # Normalisation espaces/tirets/casse symétrique à celle du référentiel — c'est ce qui
    # fait matcher "Magny les hameaux" (dit à l'oral) avec "Magny-les-Hameaux" (nom
    # officiel) à score 100 plutôt que de pénaliser la différence de séparateur.
    norm_query = query.replace("-", " ").lower()
    top = process.extract(norm_query, _NORM_NAMES, scorer=fuzz.ratio, limit=n_results)
    matches = []
    for norm_name, score, idx in top:
        c = _COMMUNES[idx]
        matches.append({
            "name": c["name"],
            "postcode": c["postcode"],
            "lat": c["lat"],
            "lon": c["lon"],
            "population": c["population"],
            "score": score,
            "source": "local",
        })
    return matches


async def _search_ban_fallback(session: aiohttp.ClientSession, query: str) -> dict | None:
    """Repli sur la Base Adresse Nationale (live) — utile pour une adresse précise (rue,
    numéro) ou une commune très récente pas encore dans le référentiel local téléchargé au
    build de l'image. Restreint aux communes (pas de rues/POI) pour rester cohérent avec ce
    que l'index local sait résoudre — un futur besoin d'adresses complètes élargirait ce
    filtre plutôt que de le retirer."""
    try:
        async with session.get(
            "https://api-adresse.data.gouv.fr/search/",
            params={"q": query, "type": "municipality", "limit": 1},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            features = data.get("features", [])
            if not features:
                return None
            props = features[0]["properties"]
            lon, lat = features[0]["geometry"]["coordinates"]
            return {
                "name": props.get("city") or props.get("name") or props.get("label"),
                "postcode": props.get("postcode", ""),
                "lat": lat,
                "lon": lon,
                "population": props.get("population") or 0,
                "score": None,
                "source": "ban_fallback",
            }
    except Exception as e:
        logger.error(f"Repli BAN échoué pour {query!r}: {e}")
        return None


async def _resolve_location(session: aiohttp.ClientSession, query: str, n_results: int = 3) -> list[dict]:
    """Logique partagée entre places/request et places/poi_request : résolution locale
    d'abord, repli BAN si le score est trop faible."""
    matches = _search_local(query, n_results)
    best = matches[0] if matches else None

    if not best or best["score"] < LOCAL_MATCH_THRESHOLD:
        fallback = await _search_ban_fallback(session, query)
        if fallback:
            logger.info(f"Repli BAN utilisé pour {query!r} (local: "
                        f"{best['score'] if best else 'aucun résultat'})")
            matches = [fallback] + [m for m in matches if m["name"] != fallback["name"]]

    return matches


EARTH_RADIUS_KM = 6371.0


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return EARTH_RADIUS_KM * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _format_address(tags: dict) -> str:
    parts = []
    if tags.get("addr:housenumber") and tags.get("addr:street"):
        parts.append(f"{tags['addr:housenumber']} {tags['addr:street']}")
    elif tags.get("addr:street"):
        parts.append(tags["addr:street"])
    if tags.get("addr:postcode") or tags.get("addr:city"):
        parts.append(f"{tags.get('addr:postcode', '')} {tags.get('addr:city', '')}".strip())
    return ", ".join(p for p in parts if p)


def _search_nearby_communes(
    lat: float, lon: float, radius_km: float, n_results: int, exclude_name: str | None = None,
) -> list[dict]:
    """Communes du référentiel local (déjà chargé pour la résolution de noms, cf.
    _COMMUNES) triées par distance — pas besoin d'une source distincte : le référentiel
    INSEE/geo.api.gouv.fr couvre déjà toutes les communes françaises avec leurs
    coordonnées, plus fiable que les nœuds place=* d'OpenStreetMap pour cet usage."""
    results = []
    for c in _COMMUNES:
        if exclude_name and c["name"] == exclude_name:
            continue
        distance_km = _haversine_km(lat, lon, c["lat"], c["lon"])
        if distance_km > radius_km:
            continue
        results.append({
            "name": c["name"],
            "postcode": c["postcode"],
            "population": c["population"],
            "distance_km": round(distance_km, 2),
        })
    results.sort(key=lambda r: r["distance_km"])
    return results[:n_results]


def _bbox_degrees(lat: float, radius_km: float) -> tuple[float, float]:
    lat_delta = radius_km / 111.0
    lon_delta = radius_km / (111.0 * max(math.cos(math.radians(lat)), 0.01))
    return lat_delta, lon_delta


def _query_pois_local_sync(
    lat: float, lon: float, radius_km: float, amenity: str, cuisine: str | None, n_results: int,
) -> list[dict]:
    """Requête synchrone (sqlite3 est bloquant) — appelée via asyncio.to_thread. Filtre par
    bounding box en lat/lon (indexé, cf. idx_category_lat_lon dans build_pois.py) avant
    raffinement précis par haversine, pour éviter de scanner toute la table."""
    lat_delta, lon_delta = _bbox_degrees(lat, radius_km)
    conn = sqlite3.connect(POIS_DB_FILE)
    try:
        sql = (
            "SELECT name, category, cuisine, lat, lon FROM pois "
            "WHERE category = ? AND lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?"
        )
        params = [amenity, lat - lat_delta, lat + lat_delta, lon - lon_delta, lon + lon_delta]
        if cuisine:
            sql += " AND cuisine LIKE ?"
            params.append(f"%{cuisine}%")
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    results = []
    for name, category, poi_cuisine, poi_lat, poi_lon in rows:
        distance_km = _haversine_km(lat, lon, poi_lat, poi_lon)
        if distance_km > radius_km:
            continue
        results.append({
            "name": name,
            "category": poi_cuisine or category,
            "address": "",
            "lat": poi_lat,
            "lon": poi_lon,
            "distance_km": round(distance_km, 2),
            "source": "osm_local",
        })
    results.sort(key=lambda r: r["distance_km"])
    return results[:n_results]


async def _search_poi_local(
    lat: float, lon: float, radius_km: float, amenity: str, cuisine: str | None, n_results: int,
) -> list[dict]:
    """Recherche dans l'extrait OSM France téléchargé et indexé au build de l'image (voir
    build_pois.py) — pas d'appel réseau, pas de dépendance à Overpass API. Source
    principale désormais ; Overpass ne sert plus que de filet de sécurité, cf. _search_poi."""
    return await asyncio.to_thread(_query_pois_local_sync, lat, lon, radius_km, amenity, cuisine, n_results)


async def _search_poi_overpass(
    session: aiohttp.ClientSession, lat: float, lon: float, radius_km: float,
    amenity: str, cuisine: str | None, n_results: int,
) -> list[dict]:
    """Recherche de POI (restaurants, commerces...) autour d'un point via Overpass API
    (OpenStreetMap) — gratuit, sans clé, données crowdsourcées. amenity/cuisine suivent le
    vocabulaire de tags OSM (https://wiki.openstreetmap.org/wiki/Key:amenity, Key:cuisine).
    Bonne couverture du commerce du quotidien (pharmacie, boulangerie...), tags parfois
    incomplets (ex: un restaurant sans son 'cuisine' renseigné)."""
    radius_m = int(radius_km * 1000)
    cuisine_filter = f'["cuisine"~"{cuisine}",i]' if cuisine else ""
    query = f"""
    [out:json][timeout:25];
    (
      node["amenity"="{amenity}"]{cuisine_filter}(around:{radius_m},{lat},{lon});
      way["amenity"="{amenity}"]{cuisine_filter}(around:{radius_m},{lat},{lon});
    );
    out center {n_results * 3};
    """
    try:
        async with session.post(
            "https://overpass-api.de/api/interpreter",
            data={"data": query},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
    except Exception as e:
        logger.error(f"Recherche POI Overpass échouée: {e}")
        return []

    results = []
    for el in data.get("elements", []):
        tags = el.get("tags", {})
        name = tags.get("name")
        if not name:
            continue
        el_lat = el.get("lat") or el.get("center", {}).get("lat")
        el_lon = el.get("lon") or el.get("center", {}).get("lon")
        if el_lat is None or el_lon is None:
            continue
        results.append({
            "name": name,
            "category": tags.get("cuisine", amenity),
            "address": _format_address(tags),
            "lat": el_lat,
            "lon": el_lon,
            "distance_km": round(_haversine_km(lat, lon, el_lat, el_lon), 2),
            "source": "overpass",
        })

    results.sort(key=lambda r: r["distance_km"])
    return results


# amenity (vocabulaire OSM, ce que le tool expose) -> type DATAtourisme (vocabulaire
# schema.org-ish) le plus proche, pour les catégories où une correspondance directe existe.
# Best-effort : les amenities du quotidien sans équivalent tourisme (pharmacy, bank, fuel...)
# sont simplement absentes ici et n'interrogent pas DATAtourisme, Overpass suffit pour elles.
_DATATOURISME_TYPE_MAP = {
    "restaurant": "Restaurant",
    "cafe": "Restaurant",
    "bar": "Restaurant",
    "cinema": "Cinema",
    "theatre": "Theater",
    "museum": "Museum",
}


async def _search_poi_datatourisme(
    session: aiohttp.ClientSession, lat: float, lon: float, radius_km: float,
    amenity: str, cuisine: str | None, n_results: int,
) -> list[dict]:
    """Recherche complémentaire via DATAtourisme (données officielles des offices de
    tourisme français) — descriptions et infos plus riches/fiables que l'OSM crowdsourcé,
    mais couverture plus étroite (orientée tourisme/loisirs : hôtels, restaurants,
    attractions, événements — pas le commerce du quotidien)."""
    if not DATATOURISME_API_KEY:
        return []
    dt_type = _DATATOURISME_TYPE_MAP.get(amenity)
    if not dt_type:
        return []

    params = {
        "geo_distance": f"{lat},{lon},{radius_km}km",
        "type": dt_type,
        "page_size": str(n_results * 2),
        "fields": "label,type,isLocatedAt,hasDescription",
    }
    if cuisine:
        params["search"] = cuisine

    try:
        async with session.get(
            "https://api.datatourisme.fr/v1/catalog",
            params=params,
            headers={"X-API-Key": DATATOURISME_API_KEY},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
    except Exception as e:
        logger.error(f"Recherche POI DATAtourisme échouée: {e}")
        return []

    results = []
    for o in data.get("objects", []):
        name = o.get("label", {}).get("@fr") or o.get("label", {}).get("@en")
        located = (o.get("isLocatedAt") or [{}])[0]
        geo = located.get("geo") or {}
        el_lat, el_lon = geo.get("latitude"), geo.get("longitude")
        if not name or el_lat is None or el_lon is None:
            continue
        addr = (located.get("address") or [{}])[0]
        street = (addr.get("streetAddress") or [""])[0]
        address = ", ".join(p for p in [street, f"{addr.get('postalCode', '')} {addr.get('addressLocality', '')}".strip()] if p)
        results.append({
            "name": name,
            "category": cuisine or amenity,
            "address": address,
            "lat": el_lat,
            "lon": el_lon,
            "distance_km": round(_haversine_km(lat, lon, el_lat, el_lon), 2),
            "source": "datatourisme",
        })

    results.sort(key=lambda r: r["distance_km"])
    return results


async def _search_poi(
    session: aiohttp.ClientSession, lat: float, lon: float, radius_km: float,
    amenity: str, cuisine: str | None, n_results: int,
) -> list[dict]:
    """Fusionne l'extrait OSM local (source principale, aucun réseau), DATAtourisme (données
    officielles, couverture tourisme/loisirs) et — seulement en complément si les deux
    premières sources ne suffisent pas — Overpass API en direct, en une seule recherche : un
    seul outil côté Joshua, plusieurs sources interrogées en interne."""
    local_results, datatourisme_results = await asyncio.gather(
        _search_poi_local(lat, lon, radius_km, amenity, cuisine, n_results),
        _search_poi_datatourisme(session, lat, lon, radius_km, amenity, cuisine, n_results),
    )

    combined = local_results + datatourisme_results

    # Overpass n'est interrogé qu'en filet de sécurité (POI ajoutés à OSM depuis le dernier
    # rebuild de l'image) — plus la source par défaut, ce qui évite les timeouts 504 de ce
    # serveur public partagé sur le cas nominal, cf. build_pois.py.
    if len(combined) < n_results:
        overpass_results = await _search_poi_overpass(session, lat, lon, radius_km, amenity, cuisine, n_results)
        combined += overpass_results

    combined.sort(key=lambda r: r["distance_km"])

    # Dédoublonnage : un même établissement peut apparaître dans les deux sources, ou en
    # node ET en way côté Overpass (bâtiment) — normalisation légère du nom pour comparer.
    seen = set()
    deduped = []
    for r in combined:
        key = r["name"].strip().lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    return deduped[:n_results]


async def on_user_connected(topic: str, payload):
    if not isinstance(payload, dict):
        return

    username = payload.get("username")
    session_id = payload.get("session_id")
    private_topics = payload.get("private_topics", [])

    if not username or not session_id:
        return

    agent_topics_topic = None
    for agent_entry in private_topics:
        for t in agent_entry.get("topics", []):
            if t["topic"].endswith("/agent_topics"):
                agent_topics_topic = t["topic"]
                break

    if not agent_topics_topic:
        logger.warning(f"[{username}] agent_topics topic introuvable, skip")
        return

    request_topic = f"users/{username}/{session_id}/places/request"
    result_topic = f"users/{username}/{session_id}/places/result"
    poi_request_topic = f"users/{username}/{session_id}/places/poi_request"
    poi_result_topic = f"users/{username}/{session_id}/places/poi_result"
    nearby_request_topic = f"users/{username}/{session_id}/places/nearby_request"
    nearby_result_topic = f"users/{username}/{session_id}/places/nearby_result"

    nexus = NexusClient.from_api_key(VK_URL, MQTT_HOST, SERVICE_USERNAME, SERVICE_API_KEY, MQTT_PORT)

    await nexus.publish(
        agent_topics_topic,
        [{
            "agent": AGENT_NAME,
            "topics": [
                {
                    "topic": request_topic,
                    "description": (
                        "Résout un nom de lieu français approximatif ou mal orthographié "
                        "(espaces au lieu de tirets, faute de frappe) vers une commune réelle "
                        "avec ses coordonnées. Utiliser AVANT tout autre outil géographique "
                        "dès qu'un nom de lieu est incertain ou peu clair."
                    ),
                    "access": "write",
                    "response_topic": result_topic,
                    "format": {"query": "Magny les hameaux", "n_results": 3},
                },
                {
                    "topic": result_topic,
                    "description": "Communes candidates, triées par pertinence décroissante (score décroissant).",
                    "access": "read",
                    "format": {"results": [{"name": "string", "postcode": "string", "lat": 0.0, "lon": 0.0}]},
                },
                {
                    "topic": poi_request_topic,
                    "description": (
                        "Cherche des commerces précis (restaurant, boulangerie, pharmacie...) autour "
                        "d'une ville — 'un resto/une pharmacie/etc. à/vers X'. 'amenity' = vocabulaire "
                        "OpenStreetMap (restaurant, cafe, bar, pharmacy, bakery, supermarket...). "
                        "'cuisine' optionnel, restaurant uniquement (sushi, italian, pizza...)."
                    ),
                    "access": "write",
                    "response_topic": poi_result_topic,
                    "format": {
                        "location": "Plaisir",
                        "amenity": "restaurant",
                        "cuisine": "(optionnel) sushi",
                        "radius_km": 5,
                        "n_results": 5,
                    },
                },
                {
                    "topic": poi_result_topic,
                    "description": (
                        "Commerces trouvés, triés par distance croissante depuis le lieu résolu. "
                        "Fusionne deux sources en interne (OpenStreetMap + données officielles des "
                        "offices de tourisme) — champ 'source' par résultat à titre indicatif seulement."
                    ),
                    "access": "read",
                    "format": {
                        "location_resolved": {"name": "string", "lat": 0.0, "lon": 0.0},
                        "results": [{"name": "string", "category": "string", "address": "string", "distance_km": 0.0, "source": "string"}],
                    },
                },
                {
                    "topic": nearby_request_topic,
                    "description": (
                        "Liste les communes françaises situées à proximité d'un lieu, dans un rayon "
                        "donné en km. Utiliser pour 'quelles villes/communes autour de X', 'aux alentours "
                        "de X', 'dans un rayon de N km de X' — PAS pour chercher un commerce précis "
                        "(voir places/poi_request pour ça)."
                    ),
                    "access": "write",
                    "response_topic": nearby_result_topic,
                    "format": {"location": "Magny-les-Hameaux", "radius_km": 10, "n_results": 10},
                },
                {
                    "topic": nearby_result_topic,
                    "description": "Communes à proximité, triées par distance croissante depuis le lieu résolu.",
                    "access": "read",
                    "format": {
                        "location_resolved": {"name": "string", "lat": 0.0, "lon": 0.0},
                        "results": [{"name": "string", "postcode": "string", "population": 0, "distance_km": 0.0}],
                    },
                },
            ],
        }],
    )
    logger.info(f"[{username}/{session_id}] Topics déclarés sur {agent_topics_topic}")

    if session_id in _subscribed_sessions:
        logger.debug(f"[{username}/{session_id}] Déjà abonné, skip")
        return
    _subscribed_sessions.add(session_id)

    async def on_places_request(t: str, p):
        if not isinstance(p, dict):
            return
        query = (p.get("query") or "").strip()
        if not query:
            return
        n_results = int(p.get("n_results", 3))

        logger.info(f"[{username}] Requête: {query!r}, n={n_results}")

        async with aiohttp.ClientSession() as session:
            matches = await _resolve_location(session, query, n_results)

        await nexus.publish(result_topic, {"results": matches, "query": query})
        logger.info(f"[{username}/{session_id}] Résultat publié ({len(matches)} candidat(s))")

    async def on_poi_request(t: str, p):
        if not isinstance(p, dict):
            return
        location = (p.get("location") or "").strip()
        if not location:
            return
        amenity = (p.get("amenity") or "restaurant").strip()
        cuisine = (p.get("cuisine") or "").strip() or None
        radius_km = float(p.get("radius_km", 5))
        n_results = int(p.get("n_results", 5))

        logger.info(f"[{username}] Requête POI: location={location!r}, amenity={amenity!r}, "
                    f"cuisine={cuisine!r}, radius_km={radius_km}")

        async with aiohttp.ClientSession() as session:
            location_matches = await _resolve_location(session, location, 1)
            if not location_matches:
                await nexus.publish(poi_result_topic, {
                    "location_resolved": None,
                    "results": [],
                    "error": f"Lieu '{location}' introuvable",
                })
                return
            resolved = location_matches[0]
            poi_results = await _search_poi(
                session, resolved["lat"], resolved["lon"], radius_km, amenity, cuisine, n_results,
            )

        await nexus.publish(poi_result_topic, {
            "location_resolved": {"name": resolved["name"], "lat": resolved["lat"], "lon": resolved["lon"]},
            "results": poi_results,
        })
        logger.info(f"[{username}/{session_id}] Résultat POI publié ({len(poi_results)} commerce(s))")

    async def on_nearby_request(t: str, p):
        if not isinstance(p, dict):
            return
        location = (p.get("location") or "").strip()
        if not location:
            return
        radius_km = float(p.get("radius_km", 10))
        n_results = int(p.get("n_results", 10))

        logger.info(f"[{username}] Requête communes proches: location={location!r}, radius_km={radius_km}")

        async with aiohttp.ClientSession() as session:
            location_matches = await _resolve_location(session, location, 1)
            if not location_matches:
                await nexus.publish(nearby_result_topic, {
                    "location_resolved": None,
                    "results": [],
                    "error": f"Lieu '{location}' introuvable",
                })
                return
            resolved = location_matches[0]

        nearby = _search_nearby_communes(
            resolved["lat"], resolved["lon"], radius_km, n_results, exclude_name=resolved["name"],
        )

        await nexus.publish(nearby_result_topic, {
            "location_resolved": {"name": resolved["name"], "lat": resolved["lat"], "lon": resolved["lon"]},
            "results": nearby,
        })
        logger.info(f"[{username}/{session_id}] Résultat communes proches publié ({len(nearby)} commune(s))")

    nexus.subscribe(request_topic, on_places_request)
    nexus.subscribe(poi_request_topic, on_poi_request)
    nexus.subscribe(nearby_request_topic, on_nearby_request)
    nexus.start_listening()
    logger.info(f"[{username}/{session_id}] Abonné à {request_topic}, {poi_request_topic} et {nearby_request_topic}")


async def main():
    nexus = NexusClient.from_api_key(VK_URL, MQTT_HOST, SERVICE_USERNAME, SERVICE_API_KEY, MQTT_PORT)
    nexus.subscribe("common/user_connected", on_user_connected)
    nexus.start_listening()
    logger.info("Places service démarré")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
