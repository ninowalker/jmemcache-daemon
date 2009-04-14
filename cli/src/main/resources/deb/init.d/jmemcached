#!/bin/sh -e
#
# Written by Philipp Meier <meier@meisterbohne.de> for Jetty
# Modified for JMemcached by Ryan Daum <ryan@thimbleware.com>
#
### BEGIN INIT INFO
# Provides:          jetty
# Required-Start:    $syslog $network
# Required-Stop:     $syslog $network
# Should-Start:      $local_fs
# Should-Stop:       $local_fs
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Start JMemcached
# Description:       Start JMemcached Memcached implementation
### END INIT INFO

PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
NAME=jmemcached
DESC="jmemcached daemon"
JMEMCACHED_HOME=/usr/share/$NAME
START_JAR="$JMEMCACHED_HOME/${project.artifactId}-${project.version}-main.jar"

# The following variables can be overwritten in /etc/default/jmemcached

# Whether to start jmemcached (as a daemon) or not
NO_START=0

# Run Jmemcached as this user ID (default: jmemcached)
# Set this to an empty string to prevent Jmemcached from starting automatically
JMEMCACHED_USER=jmemcached
                                                                                
# The first existing directory is used for JAVA_HOME (if JAVA_HOME is not
# defined in /etc/default/jmemcached)
JDK_DIRS="
	  /usr/lib/jvm/java-6-sun \
	  /usr/lib/jvm/java-1.5.0-sun \
	  /usr/lib/jvm/java-gcj \
          /usr/lib/j2sdk1.6-sun \
          /usr/lib/j2sdk1.5-sun \
          /usr/lib/j2sdk1.4-sun \
	  /usr/lib/j2sdk1.4 \
          /usr/lib/j2se/1.4 \
	  /usr/lib/j2sdk1.3 \
	  /usr/lib/j2se/1.3 \
	  /usr/lib/kaffe/ \
	 "


# Jmemcached uses a config file to setup it's boot classpath
START_CONFIG=/etc/jmemcached/start.config

# End of variables that can be overwritten in /etc/default/jmemcached
                                                                                
# overwrite settings from default file
if [ -f /etc/default/jmemcached ]; then
        . /etc/default/jmemcached
fi

# Check whether jmemcached is still installed (it might not be if this package was
# removed and not purged)
if [ -r "$START_JAR" ]; then
    HAVE_JMEMCACHED=1
else
    exit 0
fi

# Check whether startup has been disabled
if [ "$NO_START" != "0" -a "$1" != "stop" ]; then 
        [ "$VERBOSE" != "no" ] && echo "Not starting jmemcached - edit /etc/default/jmemcached and change NO_START to be 0 (or comment it out).";
        exit 0;
fi

if [ -z "$JMEMCACHED_USER" ]; then
        echo "Not starting/stopping $DESC as configured (JMEMCACHED_USER is"
        echo "empty in /etc/default/jmemcached)."
        exit 0
fi

# Look for the right JVM to use
for jdir in $JDK_DIRS; do
        if [ -d "$jdir" -a -z "${JAVA_HOME}" ]; then
                JAVA_HOME="$jdir"
        fi
done
export JAVA_HOME

export JAVA="$JAVA_HOME/bin/java"

# Set java.awt.headless=true if JAVA_OPTIONS is not set so the
# Xalan XSL transformer can work without X11 display on JDK 1.4+
if [ -z "$JAVA_OPTIONS" ]; then
        JAVA_OPTIONS="-Xmx256m \
	              -Djava.awt.headless=true \
	              -Djava.io.tmpdir=\"$JMEMCACHED_TMP\" \
		      -Djava.library.path=/usr/lib"
fi
export JAVA_OPTIONS
                                                                                
# Define other required variables
PIDFILE="/var/run/$NAME.pid"
LOGDIR="/var/log/jmemcached"

##################################################
# Check for JAVA_HOME
##################################################
if [ -z "$JAVA_HOME" ]; then
    echo "Could not start $DESC because no Java Development Kit"
    echo "(JDK) was found. Please download and install JDK 1.3 or higher and set"
    echo "JAVA_HOME in /etc/default/jmemcached to the JDK's installation directory."
    exit 0

fi

ARGUMENTS="$JAVA_OPTIONS -jar $START_JAR"

##################################################
# Do the action
##################################################
case "$1" in
  start)
	if start-stop-daemon --quiet --test --start --pidfile "$PIDFILE" \
	                --user "$JMEMCACHED_USER" --startas "$JAVA" > /dev/null; then 



	    # Look for rotatelogs/rotatelogs2
	    if [ -x /usr/sbin/rotatelogs ]; then
		ROTATELOGS=/usr/sbin/rotatelogs
	    else
		ROTATELOGS=/usr/sbin/rotatelogs2
	    fi

	    if [ -f $PIDFILE ]
		then
			echo "$PIDFILE exists, but jmemcached was not running. Ignoring $PIDFILE"
	    fi

	    echo -n "Starting $DESC: "
	    	if [ \! -e "$LOGDIR/out.log" ]; then
			touch "$LOGDIR/out.log"
			chown $JMEMCACHED_USER "$LOGDIR/out.log"
		fi
		su -p -s /bin/sh "$JMEMCACHED_USER" \
			-c "$ROTATELOGS \"$LOGDIR/out.log\" 86400" \
			< "$LOGDIR/out.log" &
		su -p -s /bin/sh "$JMEMCACHED_USER" \
			-c "$JAVA $ARGUMENTS >> $LOGDIR/out.log 2>&1 & \
			    echo \$!"  > "$PIDFILE"
                echo "$NAME."
	else
        echo "(already running)."
		exit 1
    fi
        ;;

  stop)
  	echo -n "Stopping $DESC: "

	if start-stop-daemon --quiet --test --start --pidfile "$PIDFILE" \
		--user "$JMEMCACHED_USER" --startas "$JAVA" > /dev/null; then
		if [ -x "$PIDFILE" ]; then
			echo "(not running but $PIDFILE exists)."
		else
			echo "(not running)."
		fi
	else
		start-stop-daemon --quiet --stop \
			--pidfile "$PIDFILE" --user "$JMEMCACHED_USER" \
			--startas "$JAVA" > /dev/null
                while ! start-stop-daemon --quiet --test --start \
			--pidfile "$PIDFILE" --user "$JMEMCACHED_USER" \
			--startas "$JAVA" > /dev/null; do 
                        sleep 1
                        echo -n "."

            echo -n " (killing) "
            start-stop-daemon --stop --signal 15 --oknodo \
                    --quiet --pidfile "$PIDFILE" \
                    --user "$JMEMCACHED_USER"

            done
        	rm -f "$PIDFILE"
                echo "$NAME."
	fi
        ;;

  restart|force-reload)
        $0 stop $*
        sleep 1
        $0 start $*
        ;;

  check)
        echo "Checking arguments to Jmemcached: "
        echo
	echo "PIDFILE        =  $PIDFILE"
	echo "JAVA_OPTIONS   =  $JAVA_OPTIONS"
	echo "JAVA           =  $JAVA"
	echo "JMEMCACHED_USER     =  $JMEMCACHED_USER"
	echo "ARGUMENTS      =  $ARGUMENTS"
        
        if [ -f $PIDFILE ]
        then
            echo "Jmemcached running pid="`cat $PIDFILE`
            exit 0
        fi
        exit 1
        ;;

  *)
        echo "Usage: /etc/init.d/jmemcached {start|stop|restart|force-reload|check}" >&2
	exit 1
	;;
esac

exit 0