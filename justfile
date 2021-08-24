docker:
    docker build -t menmos/menmosd --target menmosd .
    docker build -t menmos/amphora --target amphora .

lint:
    cargo check
    cargo clippy

bundle $MENMOS_WEBUI="latest" +args="":
    cargo build --features "webui" -p menmosd {{args}}

unit +args="":
    cargo test --workspace --lib {{args}}

integration +args="":
    cargo test --workspace --test '*'

test:
    @just lint
    @just unit
    @just integration
