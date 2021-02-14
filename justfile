docker:
    docker build -t menmos/menmosd --target menmosd .
    docker build -t menmos/amphora --target amphora .

lint:
    rm -rf target
    cargo check
    cargo clippy

unit:
    cargo test --workspace --lib

integration +args="":
    cargo test --workspace --test '*'

test:
    @just lint
    @just unit
    @just integration
