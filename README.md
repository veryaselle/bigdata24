BigData - Link Prediction

From 
https://git.informatik.uni-leipzig.de/dbs/big-data-praktikum/-/blob/master/05_playlist_link_prediction.md

Predict tracks that could belong to a playlist with Knowledge Graph Embeddings

Motivation Link prediction aims to predict relations that likely are missing in a knowledge graph. A variety of models exist nowadays that encode entities and relations of a knowledge graph in a low-dimensional vector space in order to predict missing links. The structure of information from the music domain is well fitted to be represented as a knowledge graph, with artists, albums, tracks and playlists as nodes and a variety of relationship types connecting them. Several datasets exist, where spotify playlist data was shared, with the goal of performing music recommendation.

--------------------------------------------------------

Spotify dump: https://cloud.scadsai.uni-leipzig.de/index.php/s/doqXzJPJFsGNi2d  (4.7 GB)

Order of python scripts (python3.9)

1. crc.py
2. verify.py
3. joins_playlist.py
4. joins_track.py
5. dump.py

   15% of total data: 471569 in dump.csv / after fix: 570419 dump.csv

6. triples_create.py
7. training_pykeen.py



