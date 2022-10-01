FROM rust:1.62 as builder

ADD ./ ./builder

WORKDIR /builder

RUN cargo build --release


FROM alpine:latest

ARG APP=server

WORKDIR /kip-db

RUN sed -i.bak 's/dl-cdn.alpinelinux.org/mirrors.cloud.tencent.com/g' /etc/apk/repositories
RUN apk add --update bash vim git perf perl thttpd
RUN git clone --depth=1 https://gitee.com/jason91/FlameGraph
RUN echo 'perf record -g -p $1' >  record.sh && \
    echo 'perf script | FlameGraph/stackcollapse-perf.pl | FlameGraph/flamegraph.pl > $1' > plot.sh && \
    chmod +x *.sh

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

COPY --from=builder /builder/target/release/${APP} ${APP}

ENTRYPOINT [ "./server" ]
