A collection of connectors that are intended to be run on a one-off, quarterly, or annual basis. Scripts that don’t qualify as near-real-time, and don’t have associated CRON jobs.

Folder structure:

- Dockerfile
- start.sh
- .dockerignore
- .env.sample
- time.cron
+ contents
+- main.py
+- requirements.txt
++ src
++- __init__.py
+++ utilities
+++- carto.py
+++- misc.py
+++- ...
+ data
+- ...
