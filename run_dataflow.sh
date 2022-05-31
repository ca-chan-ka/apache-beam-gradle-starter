#!/bin/bash

# Variables
gcp_project=
gcp_temp_location=
region=
subnet=
input_file=

# TBD
./gradlew clean execute -Dexec.args="
    --runner=DataflowRunner
    --gcpTempLocation=$gcp_temp_location
    --project=$gcp_project
    --subnetwork=regions/$region/subnetworks/$subnet
    --inputFile=$input_file
"

