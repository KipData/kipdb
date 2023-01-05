FROM rust:1.62 as builder

ADD ./src ./builder/src
ADD ./Cargo.toml ./builder/Cargo.toml
ADD ./.cargo ./builder/.cargo
ADD ./build.rs ./builder/build.rs

WORKDIR /builder

RUN cargo build --release


FROM alpine:latest

ARG APP_SERVER=server
ARG APP_CLI=cli

WORKDIR /kip-db

ENV GLIBC_REPO=https://gitee.com/tonnyluo/alpine-pkg-glibc
ENV GLIBC_VERSION=2.31-r0

RUN set -ex && \
    apk --update add libstdc++ curl ca-certificates && \
    for pkg in glibc-${GLIBC_VERSION} glibc-bin-${GLIBC_VERSION}; \
        do curl -sSL ${GLIBC_REPO}/releases/download/${GLIBC_VERSION}/${pkg}.apk -o /tmp/${pkg}.apk; done && \
    apk add --allow-untrusted /tmp/*.apk && \
    rm -v /tmp/*.apk && \
    /usr/glibc-compat/sbin/ldconfig /lib /usr/glibc-compat/lib

EXPOSE 6333

COPY --from=builder /builder/target/release/${APP_SERVER} ${APP_SERVER}
COPY --from=builder /builder/target/release/${APP_CLI} ${APP_CLI}

ENTRYPOINT [ "./server" ]
