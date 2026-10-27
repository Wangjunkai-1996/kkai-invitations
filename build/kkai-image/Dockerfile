# syntax=docker/dockerfile:1.8@sha256:e87caa74dcb7d46cd820352bfea12591f3dba3ddc4285e19c7dcd13359f7cefd

ARG RUST_IMAGE=rust:1.96.0-alpine3.23@sha256:5dc2af9dd547c33f64d5fc1d299ab93b51f39eaa16c426c476b990ce6caf5b3e
ARG DISTROLESS_IMAGE=gcr.io/distroless/static-debian12:nonroot@sha256:b7bb25d9f7c31d2bdd1982feb4dafcaf137703c7075dbe2febb41c24212b946f

FROM ${RUST_IMAGE} AS application

ARG APP_VERSION
ARG SOURCE_REVISION
ENV APP_VERSION=${APP_VERSION} SOURCE_REVISION=${SOURCE_REVISION}
RUN apk add --no-cache musl-dev
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && printf '%s\n' 'fn main() {}' >src/main.rs \
    && cargo build --locked --release \
    && rm -rf src
COPY migrations ./migrations
COPY src ./src
COPY tests ./tests
RUN cargo test --locked --all-targets
RUN test -n "${APP_VERSION}" && test -n "${SOURCE_REVISION}" \
    && cargo clean --release --package invitations \
    && cargo build --locked --release \
    && mkdir -p /out \
    && cp target/release/invitations /out/invitations \
    && strip /out/invitations

FROM ${RUST_IMAGE} AS runtime-tools

RUN apk add --no-cache musl-dev
WORKDIR /build
COPY build/kkai-image/entrypoint.rs ./entrypoint.rs
RUN rustc --edition=2021 --test entrypoint.rs -o entrypoint-tests \
    && ./entrypoint-tests
RUN mkdir -p /out \
    && rustc --edition=2021 -C opt-level=z -C panic=abort -C strip=symbols \
      entrypoint.rs -o /out/invitations-entrypoint \
    && strip /out/invitations-entrypoint

FROM ${DISTROLESS_IMAGE}

ARG APP_VERSION
ARG SOURCE_REVISION
LABEL org.opencontainers.image.title="KKAI Invitations" \
      org.opencontainers.image.description="Hardened KKAI Invitations production image" \
      org.opencontainers.image.source="https://github.com/Wangjunkai-1996/kkai-invitations" \
      org.opencontainers.image.version="${APP_VERSION}" \
      org.opencontainers.image.revision="${SOURCE_REVISION}"

COPY --from=application --chown=10009:10009 /out/invitations /usr/local/bin/invitations
COPY --from=runtime-tools --chown=10009:10009 /out/invitations-entrypoint /usr/local/bin/invitations-entrypoint

ENV LISTEN_ADDR=0.0.0.0:6212 \
    RUST_LOG=invitations=info,tower_http=warn,sqlx=warn
WORKDIR /tmp
USER 10009:10009
EXPOSE 6212
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/invitations-entrypoint"]
