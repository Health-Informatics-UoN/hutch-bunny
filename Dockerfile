FROM ghcr.io/astral-sh/uv:bookworm-slim

LABEL org.opencontainers.image.title=Hutch\ Bunny
LABEL org.opencontainers.image.description=Hutch\ Bunny
LABEL org.opencontainers.image.vendor=University\ of\ Nottingham
LABEL org.opencontainers.image.url=https://github.com/Health-Informatics-UoN/hutch-bunny/pkgs/container/hutch%2Fbunny
LABEL org.opencontainers.image.documentation=https://health-informatics-uon.github.io/hutch/bunny
LABEL org.opencontainers.image.source=https://github.com/Health-Informatics-UoN/hutch-bunny
LABEL org.opencontainers.image.licenses=MIT

COPY . /app
WORKDIR /app

RUN uv sync --frozen

ENTRYPOINT ["uv", "run", "bunny-daemon"]
