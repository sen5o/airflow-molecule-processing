# Astro Runtime image (Airflow 3.2, patch 5). Base image only — all
# project-specific Python dependencies live in requirements.txt. Local-only
# tooling (mypy, ruff) is documented in README.md's Testing section instead
# of a separate requirements file.
FROM astrocrpublic.azurecr.io/runtime:3.2-5
