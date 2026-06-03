ARG AIRFLOW_VERSION=3.2.2
FROM apache/airflow:${AIRFLOW_VERSION}

COPY --from=ghcr.io/astral-sh/uv:0.9.4 /uv /uvx /bin/

WORKDIR /opt/airflow

# Install the locked third-party dependencies. `--no-emit-project` drops the
# `-e .` line uv would otherwise emit for this project: the editable install
# would fail here because src/ isn't present at this layer (and we install the
# scraper source separately below). We avoid `uv sync`, which would try to
# reconcile the whole environment against the Airflow base image.
COPY pyproject.toml uv.lock ./
RUN uv export --frozen --no-emit-project --format requirements.txt --output-file requirements.txt \
    && uv pip install --python /home/airflow/.local/bin/python -r requirements.txt \
    && rm requirements.txt

# Playwright Chromium + its OS libraries, so the scraper can drive a browser
# inside the worker. Installed (as root) into a shared, world-readable path that
# the airflow user resolves at runtime via PLAYWRIGHT_BROWSERS_PATH.
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/playwright
USER root
RUN /home/airflow/.local/bin/python -m playwright install-deps chromium \
    && /home/airflow/.local/bin/python -m playwright install chromium \
    && chmod -R a+rx /opt/playwright
USER airflow

# Add the scraper source. The DAG puts /opt/airflow/src on sys.path to import
# the urbania_scraper package; its deps were installed above.
COPY src ./src
