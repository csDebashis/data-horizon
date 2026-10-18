#!/bin/sh
set -eu
# Fresh and existing volumes share one idempotent role definition.
exec /bin/sh /usr/local/bin/ensure-horizon-roles.sh
