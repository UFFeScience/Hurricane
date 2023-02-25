#!/bin/bash

WORKDIR="/home/maiconbanni/Documentos/UFF/datalake"
HISTORIC_DIR="/historic/"
RAW_DIR="/raw/"

while IFS= read -r file;
do
    original_filename="${file}"
    new_filename=`echo "$file" | sed -e "s/xls/csv/" | sed -e "s/historic/raw/" | sed -r "s/_[12][0-9]{3}[01][0-9][0-3][0-9]//g"`
    mv "${original_filename}" "${new_filename}"
 done < <(find ${WORKDIR}${HISTORIC_DIR} -type f)
