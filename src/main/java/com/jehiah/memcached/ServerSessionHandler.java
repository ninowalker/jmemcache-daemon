/**
 *
 * Java Memcached Server
 *
 * http://jehiah.com/projects/j-memcached
 *
 * Distributed under GPL
 * @author Jehiah Czebotar
 */
package com.jehiah.memcached;

import org.apache.mina.common.IdleStatus;
import org.apache.mina.common.IoHandler;
import org.apache.mina.common.IoSession;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Iterator;
import static java.lang.String.valueOf;
import static java.lang.Integer.*;

/**
 * The heart of the daemon, responsible for handling the creation and destruction of network
 * sessions, keeping cache statistics, and (most importantly) processing inbound (parsed) commands and then passing on
 * a response message for output.
 */
public final class ServerSessionHandler implements IoHandler {

    public String version;
    public int curr_items;
    public int total_items;
    public int curr_conns;
    public int total_conns;
    public int get_cmds;
    public int set_cmds;
    public int get_hits;
    public int get_misses;
    public int started;          /* when the process was started */
    public static long bytes_read;
    public static long bytes_written;
    public static long curr_bytes;

    public int idle_limit;
    public boolean verbose;
    protected Cache cache;

    public static CharsetEncoder ENCODER  = Charset.forName("US-ASCII").newEncoder();

    /**
     * Construct the server session handler
     *
     * @param cache the cache to use
     * @param memcachedVersion the version string to return to clients
     * @param verbosity verbosity level for debugging
     * @param idle how long sessions can be idle for
     */
    public ServerSessionHandler(Cache cache, String memcachedVersion, boolean verbosity, int idle) {
        initStats();
        this.cache = cache;

        this.started = Now();
        this.version = memcachedVersion;
        this.verbose = verbosity;
        this.idle_limit = idle;
    }

    /**
     * Handle the creation of a new protocol session.
     *
     * @param session the MINA session object
     */
    public void sessionCreated(IoSession session) {
        int conn = total_conns++;
        session.setAttribute("sess_id", valueOf(conn));
        curr_conns++;
        if (this.verbose) {
            System.out.println(session.getAttribute("sess_id") + " CONNECTED");
        }
    }

    /**
     * Handle the opening of a new session.
     *
     * @param session the MINA session object
     */
    public void sessionOpened(IoSession session) {
        if (this.idle_limit > 0) {
            session.setIdleTime(IdleStatus.BOTH_IDLE, this.idle_limit);
        }

        session.setAttribute("waiting_for", 0);
    }

    /**
     * Handle the closing of a session.
     *
     * @param session the MINA session object
     */
    public void sessionClosed(IoSession session) {
        curr_conns--;
        if (this.verbose) {
            System.out.println(session.getAttribute("sess_id") + " DIS-CONNECTED");
        }
    }

    /**
     * Handle the reception of an inbound command, which has already been pre-processed by the CommandDecoder.
     *
     * @param session the MINA session
     * @param message the message itself
     * @throws CharacterCodingException
     */
    public void messageReceived(IoSession session, Object message) throws CharacterCodingException {
        CommandMessage command = (CommandMessage) message;
        String cmd = command.cmd;
        int cmdKeysSize = command.keys.size();


        if (this.verbose) {
            StringBuffer log = new StringBuffer();
            log.append(session.getAttribute("sess_id")).append(" ");
            log.append(cmd);
            if (command.element != null) {
                log.append(" ").append(command.element.keystring);
            }
            for (int i = 0; i < cmdKeysSize; i++) {
                log.append(" ").append(command.keys.get(i));
            }
            System.err.println(log.toString());
        }

        ResponseMessage r = new ResponseMessage();
        if (cmd == Commands.GET) {
            for (int i = 0; i < cmdKeysSize; i++) {
                MCElement result = get(command.keys.get(i));
                if (result != null) {
                    r.out.putString("VALUE " + result.keystring + " " + result.flags + " " + result.data_length + "\r\n", ENCODER);
                    r.out.put(result.data, 0, result.data_length);
                    r.out.putString("\r\n", ENCODER);
                }
            }

            r.out.putString("END\r\n", ENCODER);
        } else if (cmd == Commands.SET) {
            r.out.putString(set(command.element), ENCODER);
        } else if (cmd == Commands.ADD) {
            r.out.putString(add(command.element), ENCODER);
        } else if (cmd == Commands.REPLACE) {
            r.out.putString(replace(command.element), ENCODER);
        } else if (cmd == Commands.INCR) {
            r.out.putString(get_add(command.keys.get(0), parseInt(command.keys.get(1))), ENCODER);
        } else if (cmd == Commands.DECR) {
            r.out.putString(get_add(command.keys.get(0), -1 * parseInt(command.keys.get(1))), ENCODER);
        } else if (cmd == Commands.DELETE) {
            int time = 0;
            if (cmdKeysSize > 1) {
                time = parseInt(command.keys.get(1));
            }
            r.out.putString(delete(command.keys.get(0), time), ENCODER);
        } else if (cmd == Commands.STATS) {
            String option = "";
            if (cmdKeysSize > 0) {
                option = command.keys.get(0);
            }
            r.out.putString(stat(option), ENCODER);
        } else if (cmd == Commands.VERSION) {
            r.out.putString("VERSION ", ENCODER);
            r.out.putString(version, ENCODER);
            r.out.putString("\r\n", ENCODER);
        } else if (cmd == Commands.QUIT) {
            session.close();
        } else if (cmd == Commands.FLUSH_ALL) {
            int time = 0;
            if (cmdKeysSize > 0) {
                time = parseInt(command.keys.get(0));
            }
            r.out.putString(flush_all(time), ENCODER);
        } else {
            r.out.putString("ERROR\r\n", ENCODER);
            System.err.println("error");
        }
        session.write(r);
    }

    /**
     * Called on message delivery.
     *
     * @param session the MINA session
     * @param message the message sent
     */
    public void messageSent(IoSession session, Object message) {
        if (this.verbose) {
            System.out.println(session.getAttribute("sess_id") + " SENT");
        }
    }

    /**
     * Triggered when a session has gone idle.
     * @param session the MINA session
     * @param status the idle status
     */
    public void sessionIdle(IoSession session, IdleStatus status) {
        // disconnect an idle client
        session.close();
    }

    /**
     * Triggered when an exception is caught by the protocol handler
     * @param session the MINA session
     * @param cause the exception
     */
    public void exceptionCaught(IoSession session, Throwable cause) {
        // close the connection on exceptional situation
        System.err.println(session.getAttribute("sess_id") + " EXCEPTION" + cause.getMessage() + "\r\n");
        cause.printStackTrace();
        session.close();
    }

    /**
     * Handle the deletion of an item from the cache.
     *
     * @param key the key for the item
     * @param time only delete the element if time (time in seconds)
     * @return the message response
     */
    protected String delete(String key, int time) {
        if (is_there(key)) {
            if (time != 0) {
                MCElement el = this.cache.get(key);
                if (el.expire == 0 || el.expire > (Now() + time)) {
                    el.expire = Now() + time; // update the expire time
                    this.cache.put(key, el);
                }// else it expire before the time we were asked to expire it
            } else {
                this.cache.remove(key); // just remove it
            }
            return "DELETED\r\n";
        } else {
            return "NOT_FOUND\r\n";
        }
    }

    /**
     * Add an element to the cache
     *
     * @param e the element to add
     * @return the message response string
     */
    protected String add(MCElement e) {
        if (is_there(e.keystring)) {
            return "NOT_STORED\r\n";
        } else {
            return set(e);
        }
    }

    /**
     * Replace an element in the cache
     *
     * @param e the element to replace
     * @return the message response string
     */
    protected String replace(MCElement e) {
        if (is_there(e.keystring)) {
            return set(e);
        } else {
            return "NOT_STORED\r\n";
        }
    }

    /**
     * Set an element in the cache
     *
     * @param e the element to set
     * @return the message response string
     */
    protected String set(MCElement e) {
        set_cmds += 1;//update stats
        this.cache.put(e.keystring, e);
        return "STORED\r\n";
    }

    /**
     * Increment an (integer) element inthe cache
     * @param key the key to increment
     * @param mod the amount to add to the value
     * @return the message response
     */
    protected String get_add(String key, int mod) {
        // TODO make this atomic by cooperating more directly with the cache
        MCElement e = this.cache.get(key);
        if (e == null) {
            get_misses += 1;//update stats
            return "NOT_FOUND\r\n";
        }
        if (e.expire != 0 && e.expire < Now()) {
            //System.out.println("FOUND BUT EXPIRED");
            get_misses += 1;//update stats
            return "NOT_FOUND\r\n";
        }
        // TODO handle parse failure!
        int old_val = parseInt(new String(e.data)) + mod; // change value
        if (old_val < 0) {
            old_val = 0;
        } // check for underflow
        e.data = valueOf(old_val).getBytes(); // toString
        e.data_length = e.data.length;
        this.cache.put(e.keystring, e); // save new value
        return valueOf(old_val) + "\r\n"; // return new value
    }


    /**
     * Check whether an element is in the cache and non-expired
     * @param key the key for the element to lookup
     * @return whether the element is in the cache and is live
     */
    protected boolean is_there(String key) {
        MCElement e = this.cache.get(key);
        return e != null && !(e.expire != 0 && e.expire < Now());
    }

    /**
     * Get an element from the cache
     * @param key the key for the element to lookup
     * @return the element, or 'null' in case of cache miss.
     */
    protected MCElement get(String key) {
        get_cmds += 1;//updates stats
        MCElement e = this.cache.get(key);
        if (e == null) {
            get_misses += 1;//update stats
            return null;
        }
        if (e.expire != 0 && e.expire < Now()) {
            get_misses += 1;//update stats

            // TODO shouldn't this actually remove the item from cache since it's expired?
            return null;
        }
        get_hits += 1;//update stats
        return e;
    }


    /**
     * @return the current time in seconds (from epoch), used for expiries, etc.
     */
    protected final int Now() {
        return (int) (System.currentTimeMillis() / 1000);
    }

    /**
     * Initialize all statistic counters
     */
    protected void initStats() {
        curr_items = 0;
        total_items = 0;
        curr_bytes = 0;
        curr_conns = 0;
        total_conns = 0;
        get_cmds = set_cmds = get_hits = get_misses = 0;
        bytes_read = 0;
        bytes_written = 0;
    }

    /**
     * Return runtime statistics
     * @param arg additional arguments to the stats command
     * @return the full command response
     */
    protected String stat(String arg) {

        StringBuilder builder = new StringBuilder();

        if (arg.equals("keys")) {
            Iterator itr = this.cache.keys();
            while (itr.hasNext()) {
                builder.append("STAT key ").append(itr.next()).append("\r\n");
            }
            builder.append("END\r\n");
            return builder.toString();
        }

        // stats we know
        builder.append("STAT version ").append(version).append("\r\n");
        builder.append("STAT cmd_gets ").append(valueOf(get_cmds)).append("\r\n");
        builder.append("STAT cmd_sets ").append(valueOf(set_cmds)).append("\r\n");
        builder.append("STAT get_hits ").append(valueOf(get_hits)).append("\r\n");
        builder.append("STAT get_misses ").append(valueOf(get_misses)).append("\r\n");
        builder.append("STAT curr_connections ").append(valueOf(curr_conns)).append("\r\n");
        builder.append("STAT total_connections ").append(valueOf(total_conns)).append("\r\n");
        builder.append("STAT time ").append(valueOf(Now())).append("\r\n");
        builder.append("STAT uptime ").append(valueOf(Now() - this.started)).append("\r\n");
        builder.append("STAT cur_items ").append(valueOf(this.cache.count())).append("\r\n");
        builder.append("STAT limit_maxbytes ").append(valueOf(this.cache.maxSize())).append("\r\n");
        builder.append("STAT current_bytes ").append(valueOf(this.cache.size())).append("\r\n");
        builder.append("STAT free_bytes ").append(valueOf(Runtime.getRuntime().freeMemory())).append("\r\n");

        // stuff we know nothing about
        builder.append("STAT pid 0\r\n");
        builder.append("STAT rusage_user 0:0\r\n");
        builder.append("STAT rusage_system 0:0\r\n");
        builder.append("STAT connection_structures 0\r\n");
        builder.append("STAT bytes_read 0\r\n");
        builder.append("STAT bytes_written 0\r\n");
        builder.append("END\r\n");

        return builder.toString();
    }

    /**
     * Flush all cache entries
     * @return command response
     */
    protected String flush_all() {
        return flush_all(0);
    }

    /**
     * Flush all cache entries with a timestamp after a given expiration time
     * @param expire the flush time in seconds
     * @return command response
     */
    protected String flush_all(int expire) {
        // TODO implement this, it isn't right... but how to handle efficiently? (don't want to linear scan entire cache)
        this.cache.flushAll();

        return "OK\r\n";
    }

    public String getVersion() {
        return version;
    }

    public int getCurr_items() {
        return curr_items;
    }

    public int getTotal_items() {
        return total_items;
    }

    public int getCurr_conns() {
        return curr_conns;
    }

    public int getTotal_conns() {
        return total_conns;
    }

    public int getGet_cmds() {
        return get_cmds;
    }

    public int getSet_cmds() {
        return set_cmds;
    }

    public int getGet_hits() {
        return get_hits;
    }

    public int getGet_misses() {
        return get_misses;
    }

    public int getStarted() {
        return started;
    }

    public static long getBytes_read() {
        return bytes_read;
    }

    public static long getBytes_written() {
        return bytes_written;
    }

    public static long getCurr_bytes() {
        return curr_bytes;
    }

    public int getIdle_limit() {
        return idle_limit;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public Cache getCache() {
        return cache;
    }
}