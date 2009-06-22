package com.thimbleware.jmemcached.protocol;

import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.MCElement;

import java.io.Serializable;
import java.util.Set;
import java.util.Map;

/**
 * Represents the response to a command.
 */
public final class ResponseMessage implements Serializable {

    public ResponseMessage(CommandMessage cmd) {
        this.cmd = cmd;
    }

    public CommandMessage cmd;
    public MCElement[] elements;
    public Cache.StoreResponse response;
    public Map<String, Set<String>> stats;
    public String version;
    public Cache.DeleteResponse deleteResponse;
    public Integer incrDecrResponse;
    public boolean flushSuccess;

    public ResponseMessage withElements(MCElement[] elements) {
        this.elements = elements;
        return this;
    }

    public ResponseMessage withResponse(Cache.StoreResponse response) {
        this.response = response;
        return this;
    }

    public ResponseMessage withDeleteResponse(Cache.DeleteResponse deleteResponse) {
        this.deleteResponse = deleteResponse;
        return this;
    }

    public ResponseMessage withIncrDecrResponse(Integer incrDecrResp) {
        this.incrDecrResponse = incrDecrResp;

        return this;
    }

    public ResponseMessage withStatResponse(Map<String, Set<String>> stats) {
        this.stats = stats;

        return this;
    }

    public ResponseMessage withFlushResponse(boolean success) {
        this.flushSuccess = success;

        return this;
    }
}
