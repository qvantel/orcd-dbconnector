#!/bin/bash
function eexit {
	echo "$1" && exit 1
}
which wget > /dev/null || eexit "wget not installed"
which sha1sum > /dev/null || eexit "sha1sum not installed"

wget -N https://raw.githubusercontent.com/musalbas/mcc-mnc-table/master/mcc-mnc-table.json -O temp.json

# Get checksums
t=$(sha1sum temp.json 2>/dev/null | awk '{print $1;}')
j=$(sha1sum mcc-mnc-table.json 2>/dev/null | awk '{print $1;}')

# replace if different
[ t != j ] && mv temp.json mcc-mnc-table.json || rm temp.json
