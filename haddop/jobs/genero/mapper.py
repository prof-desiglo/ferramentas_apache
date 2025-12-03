#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.rsplit(",", 1)

    if len(parts) != 2:
        continue

    movie_id_title, genres = parts
    genres = genres.strip()

    if genres == "(no genres listed)" or genres == "":
        continue

    for g in genres.split("|"):
        print(f"{g}\t1")

