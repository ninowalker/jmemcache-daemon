package com.thimbleware.jmemcached;

public interface CacheElementFactory {
	public CacheElement newInstance(Key key, int flags, long expire, long casUnique);
}
