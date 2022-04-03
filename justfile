# Installs tools to work with menmos
setup:
    cargo install cargo-nextest
    cargo install cargo-release

# -- Build Workflows --

# Build menmosd & amphora docker images
docker:
    docker build -t menmos/menmosd --target menmosd .
    docker build -t menmos/amphora --target amphora .

# Lint all packages
lint:
    cargo check
    cargo clippy

bundle $MENMOS_WEBUI="branch=master" +args="":
    @echo "Bundle target: $MENMOS_WEBUI"
    cargo build --features "webui" -p menmosd {{args}}


# -- Test Workflows --

# Run all tests and validations
test +args="":
    cargo nextest run {{args}}

# -- Local Setup Workflows --
# TODO: Add trace-level logging preset

export WORKDIR := "./tmp"

clean:
    rm -rf {{WORKDIR}}/blob-cache {{WORKDIR}}/blobs {{WORKDIR}}/db  {{WORKDIR}}/storage_db

# Run menmosd using the local setup.
menmosd loglevel="normal":
    MENMOS_LOG_LEVEL="{{loglevel}}" cargo run -p menmosd -- --cfg {{WORKDIR}}/menmosd.toml

# Run amphora using the local setup.
amphora loglevel="normal":
    MENMOS_LOG_LEVEL="{{loglevel}}" cargo run -p amphora -- --cfg {{WORKDIR}}/amphora.toml
