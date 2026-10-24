FROM python:3.12-slim-trixie

COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /uvx /usr/local/bin/

ARG ALLURE_CLI_VERSION
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl default-jre-headless \
    && curl -fsSL \
       "https://github.com/allure-framework/allure2/releases/download/${ALLURE_CLI_VERSION}/allure-${ALLURE_CLI_VERSION}.tgz" \
       -o /tmp/allure.tgz \
    && tar -xzf /tmp/allure.tgz -C /opt \
    && ln -s "/opt/allure-${ALLURE_CLI_VERSION}/bin/allure" /usr/local/bin/allure \
    && rm -rf /var/lib/apt/lists/* /tmp/allure.tgz

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PATH="/opt/venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-install-project

COPY . .
