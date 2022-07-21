#!/usr/bin/env bash
#
# Configures a BigTable instance with the expected tables
# Usage: init-bigtable.sh [bigtable-instance-name]
# If bigtable-instance-name is not given. It will be the default
# solana-geyser-plugin-bigtable
#

set -e

if [ -n "$1" ]
then
  instance=$1
else
  instance=solana-geyser-plugin-bigtable
fi

cbt=(
  cbt
  -instance
  "$instance"
)
if [[ -n $BIGTABLE_EMULATOR_HOST ]]; then
  cbt+=(-project emulator)
fi

for table in account slot block transaction; do
  (
    set -x
    "${cbt[@]}" createtable $table
    "${cbt[@]}" createfamily $table x
    "${cbt[@]}" setgcpolicy $table x maxversions=1
    "${cbt[@]}" setgcpolicy $table x maxage=360d
  )
done
