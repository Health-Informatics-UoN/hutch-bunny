# Install uv
FROM python:3.13-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.6.9 /uv /uvx /bin/

# Change the working directory to the `app` directory
WORKDIR /app

# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-editable
    
# Copy the project into the intermediate image
COPY . /app

# Sync the project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-editable

# ------
FROM python:3.13-slim

# Create a non-root user and group
RUN groupadd -r app && useradd --no-log-init -r -g app app

LABEL org.opencontainers.image.title=Hutch\ Bunny
LABEL org.opencontainers.image.description=Hutch\ Bunny
LABEL org.opencontainers.image.vendor=University\ of\ Nottingham
LABEL org.opencontainers.image.url=https://github.com/Health-Informatics-UoN/hutch-bunny/pkgs/container/hutch%2Fbunny
LABEL org.opencontainers.image.documentation=https://health-informatics-uon.github.io/hutch/bunny
LABEL org.opencontainers.image.source=https://github.com/Health-Informatics-UoN/hutch-bunny
LABEL org.opencontainers.image.licenses=MIT

# Copy the environment, but not the source code
COPY --from=builder --chown=app:app /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH" 

# Switch to the non-root user
USER app

# Run Bunny Daemon by default; user can override and run the CLI by specifying `bunny`
CMD ["bunny-daemon"]
