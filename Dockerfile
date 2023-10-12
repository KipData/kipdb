FROM rust:1.62 as builder

ADD ./src ./builder/src
ADD ./Cargo.toml ./builder/Cargo.toml
ADD ./build.rs ./builder/build.rs

RUN apt update && apt install -y protobuf-compiler

WORKDIR /builder

RUN rustup default nightly
RUN cargo build --release

FROM frolvlad/alpine-glibc

ARG APP_SERVER=server
ARG APP_CLI=cli

WORKDIR /kip-db

ENV IP="127.0.0.1"

EXPOSE 6333

COPY --from=builder /builder/target/release/${APP_SERVER} ${APP_SERVER}
COPY --from=builder /builder/target/release/${APP_CLI} ${APP_CLI}

#CMD ["./$APP_SERVER", "--ip", "$IP"]