#!/usr/bin/env bash

## Projects Directories
MAG_DIR="$HOME/magellan"
FLINK_SRC="$HOME/flink_src"
FLINK_COMP="$HOME/flink_comp"

## mag directories
MAG_BIN_DIR="$MAG_DIR/bin"


## klink-benchmarks files
#FLINK_CONF_FILE="$FLINK_DIR/conf/flink-conf.yaml"

#WORKLOAD_PROCESSOR_JAR_FILE="$WORKLOAD_PROCESSOR_DIR/target/workload-processor-flink-0.5.0.jar"

# Versions
SCALA_BIN_VERSION=${SCALA_BIN_VERSION:-"2.12"}
FLINK_VERSION=${FLINK_VERSION:-"1.9.0"}

# Commands
MAKE=${MAKE:-make}
GIT=${GIT:-git}
MVN=${MVN:-mvn}

pid_match() {
   local VAL=`ps -aef | grep "$1" | grep -v grep | awk '{print $2}'`
   echo $VAL
}

start_if_needed() {
  local match="$1"
  shift
  local name="$1"
  shift
  local sleep_time="$1"
  shift
  local PID=`pid_match "$match"`

  if [[ "$PID" -ne "" ]];
  then
    echo "$name is already running..."
  else
    "$@" &
    sleep $sleep_time
  fi
}

stop_if_needed() {
  local match="$1"
  local name="$2"
  local PID=`pid_match "$match"`
  if [[ "$PID" -ne "" ]];
  then
    kill "$PID"
    sleep 1
    local CHECK_AGAIN=`pid_match "$match"`
    if [[ "$CHECK_AGAIN" -ne "" ]];
    then
      kill -9 "$CHECK_AGAIN"
    fi
  else
    echo "No $name instance found to stop"
  fi
}

fetch_untar_file() {
  local file="$DOWNLOAD_CACHE_DIR/$1"
  local url=$2
  if [[ -e "$file" ]];
  then
    echo "Using cached File $file"
  else
	mkdir -p $DOWNLOAD_CACHE_DIR
    WGET=`whereis wget`
    CURL=`whereis curl`
    if [[ -n "$WGET" ]];
    then
      wget -O "$file" "$url"
    elif [[ -n "$CURL" ]];
    then
      curl -o "$file" "$url"
    else
      echo "Please install curl or wget to continue.";
      exit 1
    fi
  fi
  tar -xzvf $file
}

maven_clean_install_with_tests(){
    cd $1
    $MVN clean install -Dcheckstyle.skip -Drat.skip=true
}

maven_clean_install_no_tests(){
    cd $1
    $MVN clean install -DskipTests -Dcheckstyle.skip -Drat.skip=true
}

yaml() {
    sudo python3 -c "import yaml;print(yaml.load(open('$1'))$2)"
}

ssh_connect() {
    # $1: host, $2 function, $3 sleep time
    ssh -f $1 "$2" </dev/null
    sleep "$3"
}

if [[ -e "$HOSTS_FILE" ]]; then
    ## Global variables
    HAS_HOSTS=1
    zk_connect
else
    HAS_HOSTS=0
fi
