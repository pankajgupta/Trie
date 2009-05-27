#!/bin/sh
#
# trie init.d script.
#
# All java services require the same directory structure:
#   /opt/local/$APP_NAME-$VERSION
#   /var/log/$APP_NAME (chown daemon, chmod 775)

APP_NAME="trie"
VERSION="1.0"
APP_HOME="/opt/local/$APP_NAME/current"
AS_USER="daemon"
DAEMON="/usr/local/bin/daemon"

HEAP_OPTS="-Xmx13000m -Xms13000m -XX:NewSize=1024m"
JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
JAVA_OPTS="-server -verbosegc -XX:+PrintGCDetails -XX:+UseConcMarkSweepGC -XX:+UseParNewGC $HEAP_OPTS $JMX_OPTS"

pidfile="/var/run/$APP_NAME.pid"
daemon_args="--name $APP_NAME --pidfile $pidfile"
daemon_start_args="--user $AS_USER --stdout=/var/log/$APP_NAME/stdout --stderr=/var/log/$APP_NAME/error"

function running() {
  $DAEMON $daemon_args --running
}

function find_java() {
  if [ ! -z "$JAVA_HOME" ]; then
    return
  fi
  potential=$(ls -r1d /opt/jdk /System/Library/Frameworks/JavaVM.framework/Versions/CurrentJDK/Home /usr/java/default /usr/java/j* 2>/dev/null)
  for p in $potential; do
    if [ -x $p/bin/java ]; then
      JAVA_HOME=$p
      break
    fi
  done
}


# dirs under /var/run can go away between reboots.
for p in /var/run/$APP_NAME /var/log/$APP_NAME; do
  if [ ! -d $p ]; then
    mkdir -p $p
    chmod 775 $p
    chown $AS_USER $p >/dev/null 2>&1 || true
  fi
done

find_java


case "$1" in
  start)
    echo -n "Starting $APP_NAME... "

    if [ ! -r $APP_HOME/$APP_NAME-$VERSION.jar ]; then
      echo "FAIL"
      echo "*** $APP_NAME jar missing: $APP_HOME/$APP_NAME-$VERSION.jar - not starting"
      exit 1
    fi
    if [ ! -x $JAVA_HOME/bin/java ]; then
      echo "FAIL"
      echo "*** $JAVA_HOME/bin/java doesn't exist -- check JAVA_HOME?"
      exit 1
    fi
    if running; then
      echo "already running."
      exit 0
    fi
    
    ulimit -n 8192 || echo -n " (no ulimit)"
    $DAEMON $daemon_args $daemon_start_args -- ${JAVA_HOME}/bin/java ${JAVA_OPTS} -jar ${APP_HOME}/${APP_NAME}-${VERSION}.jar
    tries=0
    while ! running; do
      tries=$((tries + 1))
      if [ $tries -ge 5 ]; then
        echo "FAIL"
        exit 1
      fi
      sleep 1
    done
    echo "done."
  ;;

  stop)
    echo -n "Stopping $APP_NAME... "
    if ! running; then
      echo "wasn't running."
      exit 0
    fi

    kill -TERM $(cat $pidfile)
    tries=0
    while running; do
      tries=$((tries + 1))
      if [ $tries -ge 5 ]; then
        echo "FAIL"
        exit 1
      fi
      sleep 1
    done
    echo "done."
  ;;
  
  status)
    if running; then
      echo "$APP_NAME is running."
    else
      echo "$APP_NAME is NOT running."
    fi
  ;;

  restart)
    $0 stop
    sleep 2
    $0 start
  ;;

  *)
    echo "Usage: /etc/init.d/${APP_NAME}.sh {start|stop|restart|status}"
    exit 1
  ;;
esac

exit 0
