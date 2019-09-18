# Training: Big Data and Machine Learning with Spark

## Prerequisites

1. Check that your MS Windows system has at least 40GB of free space and 4GB of free RAM
2. Re-create default docker machine using:

        docker-machine rm default
        docker-machine create -d virtualbox --virtualbox-memory=4096 \
            --virtualbox-cpu-count=4 --virtualbox-disk-size=40960 \
            --virtualbox-no-vtx-check default
            
3. Pull images that we are going to need during the training:

        TODO
