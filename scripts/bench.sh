#!/usr/bin/env bash

set -eu -o pipefail

#################################################################################
# GLOBALS                                                                       #
#################################################################################

export TMP=/tmp/fuzzy-phrase-bench
export S3_DIR=s3://mapbox/playground/boblannon/fuzzy-phrase/bench


#################################################################################
# Download
#
# This downloads test data from s3 and extracts it.  Example:
#
#     ./scripts/bench.sh download phrase us en latn
#
# ...would download the benchmark data for `phrase/` benchmarks for United
# States (us), in English (en), in Latin script (latn).
function download() {
    type=$1
    country=$2
    language=$3
    script=$4
    fname="${country}_${language}_${script}.txt.gz"
    sample_fname="${country}_${language}_${script}_sample.txt.gz"

    mkdir -p "${TMP}/${type}"

    FROM="${S3_DIR}/${type}/${fname}"
    TO="${TMP}/${type}/${fname}"
    echo "Downloading ${FROM}"
    aws s3 cp $FROM $TO
    echo "Extracting ${TO}"
    gunzip $TO

    FROM="${S3_DIR}/${type}/${sample_fname}"
    TO="${TMP}/${type}/${sample_fname}"
    echo "Downloading ${FROM}"
    aws s3 cp $FROM $TO
    echo "Extracting ${TO}"
    gunzip $TO
    exit 0
}

#################################################################################
# Run
#
# This runs benchmarks on a certain type and on certain data. The data is
# presumed to exist in the local $TMP. Example:
#
#     ./scripts/bench.sh run phrase us en latn
#
# ...would run `cargo bench` using the benchmark data for `phrase/` benchmarks
# for United States (us), in English (en), in Latin script (latn).
function run() {
    type=$1
    country=$2
    language=$3
    script=$4
    # this will be used to create filenames ${fbasename}.txt and ${fbasename}_sample.txt
    fbasename="${country}_${language}_${script}"
    echo "running"
    env PHRASE_BENCH="${TMP}/${type}/${fbasename}" cargo bench -v "${type}"
    exit 0
}

# remove tmp dir
function clean() {
    if [[ -d $TMP ]]; then
        echo "ok - Are you sure you wish to wipe ${TMP}? (Y/n)"
        read WRITE_IP

        if [[ $WRITE_IP != "n" ]]; then
            rm -rf $TMP
        fi
    fi
}

VERB=$1

case $VERB in
    download)   download $2 $3 $4 $5;;
    run)        run $2 $3 $4 $5;;
    clean)      clean;;
    *)          echo "not ok - invalid command" && exit 3;;
esac
