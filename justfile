# Set the batch size for opentelemetry exports
export OTEL_BSP_MAX_EXPORT_BATCH_SIZE := "128"

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
    RUST_LIB_BACKTRACE=1 cargo nextest run {{args}}

# -- Local Setup Workflows --
export WORKDIR := "./tmp"

clean:
    rm -rf {{WORKDIR}}/blob-cache {{WORKDIR}}/blobs {{WORKDIR}}/db  {{WORKDIR}}/storage_db

# Run menmosd using the local setup.
menmosd loglevel="normal":
    MENMOS_LOG_LEVEL="{{loglevel}}" cargo run -p menmosd -- --cfg {{WORKDIR}}/menmosd.toml --xecute tmp/xecute.json

# Run amphora using the local setup.
amphora loglevel="normal":
    MENMOS_LOG_LEVEL="{{loglevel}}" cargo run -p amphora -- --cfg {{WORKDIR}}/amphora.toml --xecute tmp/xecute.json
