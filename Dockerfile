# Astro Runtime image (Airflow 3.2, patch 5). Base image only — all
# project-specific Python dependencies live in requirements.txt (runtime)
# and requirements-dev.txt (local tooling, not part of the built image).
FROM astrocrpublic.azurecr.io/runtime:3.2-5
