#!/bin/sh
set -eu
: "${INFRAI_API_KEY:?set INFRAI_API_KEY}"
: "${INFRAI_SOURCE_QUEUE:?set INFRAI_SOURCE_QUEUE}"
: "${INFRAI_DEAD_LETTER_QUEUE:?set INFRAI_DEAD_LETTER_QUEUE}"
cargo run --bin queue_worker
