#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

# Apache Mirror link
APACHE_MIRROR="https://archive.apache.org/distributions"

init_magellan_from_github(){
     # If Apache Flink is not built
     if [[ ! -d $FLINK_SRC ]]; then
        echo "Cloning Flink"
        git clone -b release-1.9 https://github.com/apache/flink.git $FLINK_SRC
        maven_clean_install_no_tests $FLINK_SRC
     fi

     # Get magellan
     if [[ ! -d $MAG_DIR ]]; then
        git clone https://github.com/oibfarhat/magellan.git $MAG_DIR
     fi

     # Move magellan changes
     if [[ -d $MAG_DIR/flink-runtime ]]; then
        sudo rm -r $FLINK_SRC/flink-runtime
     fi

     if [[ -d $MAG_DIR/flink-streaming-java ]]; then
        sudo rm -r $FLINK_SRC/flink-streaming-java
     fi

     sudo chmod -R 777 $MAG_DIR

     cp -r $MAG_DIR/flink-runtime $FLINK_SRC/
     cp -r $MAG_DIR/flink-streaming-java $FLINK_SRC/
     maven_clean_install_no_tests $FLINK_SRC/flink-runtime
     maven_clean_install_no_tests $FLINK_SRC/flink-streaming-java
     maven_clean_install_no_tests $FLINK_SRC/flink-dist
     mv $FLINK_SRC/build-target $FLINK_COMP

     sudo chmod -R 777 $FLINK_COMP
}

setup(){
    ## Install Flink and Magellan targets
    init_magellan_from_github
}

setup