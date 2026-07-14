FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY build_index.py .
# Référentiel téléchargé au build : les communes françaises changent rarement, pas besoin
# de le retélécharger à chaque démarrage du conteneur. Un rebuild de l'image suffit à le
# rafraîchir.
RUN mkdir -p /data && python build_index.py

COPY build_pois.py .
# Extrait OSM France (~5 Go) téléchargé et traité au build (~20-30 min) — supprimé en fin
# de script, seule la base SQLite résultante (bien plus petite) reste dans l'image. Évite
# toute dépendance à Overpass API (serveur public partagé, sujet à des timeouts) au
# moment des requêtes. Rafraîchir : reconstruire l'image (idéalement pas plus souvent que
# hebdomadaire/mensuel, le référentiel POI ne change pas vite).
RUN python build_pois.py

COPY main.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
