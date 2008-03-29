package com.jehiah.memcached;

/**
 */
public interface Commands {

    String GET = "GET".intern();
    String DELETE = "DELETE".intern();
    String DECR = "DECR".intern();
    String INCR = "INCR".intern();
    String REPLACE = "REPLACE".intern();
    String ADD = "ADD".intern();
    String SET = "SET".intern();
    String STATS = "STATS".intern();
    String VERSION = "VERSION".intern();
    String QUIT = "QUIT".intern();
    String FLUSH_ALL = "FLUSH_ALL".intern();
}
