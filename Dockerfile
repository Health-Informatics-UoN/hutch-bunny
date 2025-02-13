FROM ghcr.io/astral-sh/uv:bookworm-slim

LABEL org.opencontainers.image.title=Hutch\ Bunny
LABEL org.opencontainers.image.description=Hutch\ Bunny
LABEL org.opencontainers.image.vendor=University\ of\ Nottingham
LABEL org.opencontainers.image.url=https://github.com/Health-Informatics-UoN/hutch-bunny/pkgs/container/hutch%2Fbunny
LABEL org.opencontainers.image.documentation=https://health-informatics-uon.github.io/hutch/bunny
LABEL org.opencontainers.image.source=https://github.com/Health-Informatics-UoN/hutch-bunny
LABEL org.opencontainers.image.licenses=MIT


# Install uv
FROM python:3.13-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Change the working directory to the `app` directory
WORKDIR /app

# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-editable
    
# Copy the project into the intermediate image
COPY . /app
# ADD or COPY? COPY should be fine

# Sync the project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-editable

FROM python:3.13-slim

# Copy the environment, but not the source code
COPY --from=builder --chown=app:app /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH" 

# Run the application
# CMD ["/app/.venv/bin/hello"]
# ENTRYPOINT ["uv", "run", "bunny-daemon"]
