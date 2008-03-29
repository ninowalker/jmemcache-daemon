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
    protected MCCache data;

    public static CharsetEncoder ENCODER  = Charset.forName("US-ASCII").newEncoder();

    public ServerSessionHandler(MCCache cache, String memcachedVersion, boolean verbosity, int idle) {
        initStats();
        this.data = cache;

        this.started = Now();
        this.version = memcachedVersion;
        this.verbose = verbosity;
        this.idle_limit = idle;
    }



    public int Now() {
        return (int) (System.currentTimeMillis() / 1000);
    }

    public void sessionCreated(IoSession session) throws Exception {
        int conn = total_conns++;
        session.setAttribute("sess_id", valueOf(conn));
        curr_conns++;
        if (this.verbose) {
            System.out.println(session.getAttribute("sess_id") + " CONNECTED");
        }
    }

    public void sessionOpened(IoSession session) {
        if (this.idle_limit > 0) {
            session.setIdleTime(IdleStatus.BOTH_IDLE, this.idle_limit);
        }

        session.setAttribute("waiting_for", 0);
    }

    public void sessionClosed(IoSession session) {
        curr_conns--;
        if (this.verbose) {
            System.out.println(session.getAttribute("sess_id") + " DIS-CONNECTED");
        }
    }

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

    public void messageSent(IoSession session, Object message) {
        if (this.verbose) {
            System.out.println(session.getAttribute("sess_id") + " SENT");
        }
    }

    public void sessionIdle(IoSession session, IdleStatus status) {
        // disconnect an idle client
        session.close();
    }

    public void exceptionCaught(IoSession session, Throwable cause) {
        // close the connection on exceptional situation
        System.err.println(session.getAttribute("sess_id") + " EXCEPTION" + cause.getMessage() + "\r\n");
        cause.printStackTrace();
        session.close();
    }

    public String delete(String keystring, int time) {
        if (is_there(keystring)) {
            if (time != 0) {
                MCElement el = this.data.get(keystring);
                if (el.expire == 0 || el.expire > (Now() + time)) {
                    el.expire = Now() + time; // update the expire time
                    this.data.put(keystring, el);
                }// else it expire before the time we were asked to expire it
            } else {
                this.data.remove(keystring); // just remove it
            }
            return "DELETED\r\n";
        } else {
            return "NOT_FOUND\r\n";
        }
    }

    // add is oposite of replace
    public String add(MCElement e) {
        if (is_there(e.keystring)) {
            return "NOT_STORED\r\n";
        } else {
            return set(e);
        }
    }

    // replace is oposite of add
    public String replace(MCElement e) {
        if (is_there(e.keystring)) {
            return set(e);
        } else {
            return "NOT_STORED\r\n";
        }
    }

    public String set(MCElement e) {
        set_cmds += 1;//update stats
        this.data.put(e.keystring, e);
        return "STORED\r\n";
    }

    public String get_add(String keystring, int mod) {
        MCElement e = this.data.get(keystring);
        if (e == null) {
            get_misses += 1;//update stats
            return "NOT_FOUND\r\n";
        }
        if (e.expire != 0 && e.expire < Now()) {
            //System.out.println("FOUND BUT EXPIRED");
            get_misses += 1;//update stats
            return "NOT_FOUND\r\n";
        }
        int old_val = parseInt(new String(e.data)) + mod; // change value
        if (old_val < 0) {
            old_val = 0;
        } // check for underflow
        e.data = valueOf(old_val).getBytes(); // toString
        e.data_length = e.data.length;
        this.data.put(e.keystring, e); // save new value
        return valueOf(old_val) + "\r\n"; // return new value
    }

    /*
      * this.data.containsKey() would work except it doesn't check the expire time
      */
    public boolean is_there(String keystring) {
        MCElement e = this.data.get(keystring);
        return e != null && !(e.expire != 0 && e.expire < Now());
    }

    public MCElement get(String keystring) {
        get_cmds += 1;//updates stats
        MCElement e = this.data.get(keystring);
        if (e == null) {
            get_misses += 1;//update stats
            return null;
        }
        if (e.expire != 0 && e.expire < Now()) {
            get_misses += 1;//update stats
            return null;
        }
        get_hits += 1;//update stats
        return e;
    }

    public void initStats() {
        curr_items = 0;
        total_items = 0;
        curr_bytes = 0;
        curr_conns = 0;
        total_conns = 0;
        get_cmds = set_cmds = get_hits = get_misses = 0;
        bytes_read = 0;
        bytes_written = 0;
    }

    public String stat(String arg) {

        StringBuilder builder = new StringBuilder();

        if (arg.equals("keys")) {
            Iterator itr = this.data.keys();
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
        builder.append("STAT cur_items ").append(valueOf(this.data.count())).append("\r\n");
        builder.append("STAT limit_maxbytes ").append(valueOf(this.data.maxSize())).append("\r\n");
        builder.append("STAT current_bytes ").append(valueOf(this.data.size())).append("\r\n");
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

    public String flush_all() {
        return flush_all(0);
    }

    public String flush_all(int expire) {
        this.data.flushAll();

        return "OK\r\n";
    }

}