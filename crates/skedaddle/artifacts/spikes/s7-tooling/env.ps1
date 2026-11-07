$env:CARGO_BUILD_JOBS='2'
$env:CARGO_TARGET_DIR='K:/Projects/gigglesdb/crates/skedaddle/target/s7'
$env:CARGO_HOME='K:/Projects/gigglesdb/crates/skedaddle/artifacts/spikes/s1-replication/cargo-home'
$env:TEMP='K:/Projects/gigglesdb/crates/skedaddle/target/s7/temp'
$env:TMP=$env:TEMP
$env:CARGO_TERM_COLOR='never'
$env:PYTHONDONTWRITEBYTECODE='1'
New-Item -ItemType Directory -Force -Path $env:TEMP | Out-Null
$env:PATH="$env:CARGO_HOME/bin;$env:PATH"
