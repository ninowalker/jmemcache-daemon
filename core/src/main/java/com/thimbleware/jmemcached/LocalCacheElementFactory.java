package com.thimbleware.jmemcached;

public class LocalCacheElementFactory implements CacheElementFactory {

	public CacheElement newInstance(Key key, int flags, long expire, long casUnique) {
		return new LocalCacheElement(key, flags, expire, casUnique);
	}

}
