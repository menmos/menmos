lint:
    rm -rf target
    cargo check
    cargo clippy

unit:
    cargo test --workspace

integration +args="":
    #!/bin/bash
    cargo build --workspace
    cd python
    poetry install
    poetry run pytest -v {{args}}

test:
    @just unit
    @just integration
