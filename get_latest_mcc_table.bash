#!/bin/bash
function eexit {
	echo "$1" && exit 1
}

which wget > /dev/null || eexit "ERROR: wget not installed"

if [ ! -f "mcc-mnc-table.json" ]
then
  echo "Downloading mcc table"
  wget http://raw.githubusercontent.com/musalbas/mcc-mnc-table/master/mcc-mnc-table.json
fi

if [ ! -f "src/main/resources/mcc-mnc-table.json" ]
then
  echo "Copying mcc table to main/resources/"
  cp mcc-mnc-table.json src/main/resources/
fi

if [ ! -f "src/test/resources/mcc-mnc-table.json" ]
then
  echo "Copying mcc table to test/resources/"
  cp mcc-mnc-table.json src/test/resources/
fi
