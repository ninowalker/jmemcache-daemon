package com.thimbleware.jmemcached.protocol;

import java.io.Serializable;

/**
 * Class for holding the current session status.
 */
final class SessionStatus implements Serializable {

    /**
     * Possible states that the current session is in.
     */
    enum State {
        ERROR,
        WAITING_FOR_DATA,
        READY
    }

    // the state the session is in
    public final State state;

    // if we are waiting for more data, how much?
    public int bytesNeeded;

    // the current working command
    public CommandMessage cmd;

    SessionStatus(State state) {
        this.state = state;
    }

    SessionStatus(State state, int bytesNeeded, CommandMessage cmd) {
        this.state = state;
        this.bytesNeeded = bytesNeeded;
        this.cmd = cmd;
    }

    public static SessionStatus error() {
        return new SessionStatus(State.ERROR);
    }

    public static SessionStatus ready() {
        return new SessionStatus(State.READY);
    }

    public static SessionStatus needMoreForCommand(int bytesNeeded, CommandMessage cmd) {
        return new SessionStatus(State.WAITING_FOR_DATA, bytesNeeded, cmd);
    }


}
