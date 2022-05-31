#!/bin/bash

# TBD
./gradlew clean execute -Dexec.args="
    --runner=DataflowRunner
    --gcpTempLocation=gs://lake_biwa/temp_dataflow
    --project=ca-chan-test
    --subnetwork=regions/asia-northeast1/subnetworks/vpc-network-main-subnet
    --inputFile=gs://apache-beam-samples/shakespeare/*
"

