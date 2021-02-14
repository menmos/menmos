FROM rust:latest as build

ADD . /build

WORKDIR /build

RUN cargo build -p menmosd -p amphora --release

FROM ubuntu:latest as menmosd
WORKDIR /app
RUN apt-get update && apt-get install -y libssl-dev
COPY --from=build /build/target/release/menmosd ./menmosd
ENTRYPOINT ["/app/menmosd"]


FROM ubuntu:latest as amphora
WORKDIR /app
RUN apt-get update && apt-get install -y libssl-dev
COPY --from=build /build/target/release/amphora ./amphora
ENTRYPOINT ["/app/amphora"]
