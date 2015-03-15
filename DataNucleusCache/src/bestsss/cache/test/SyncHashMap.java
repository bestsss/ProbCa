package bestsss.cache.test;

import java.util.HashMap;
import java.util.Map;

/**
 * Written by Stanimir Simeonoff and released as public domain as described at
 * http://creativecommons.org/publicdomain/zero/1.0/
 * @author Stanimir Simeonoff
 */
public class SyncHashMap<K,V> extends HashMap<K, V>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public synchronized int size() {
		return super.size();
	}

	@Override
	public synchronized boolean isEmpty() {
		return super.isEmpty();
	}

	@Override
	public synchronized V get(Object key) {
		return super.get(key);
	}

	@Override
	public synchronized V put(K key, V value) {
		return super.put(key, value);
	}

	@Override
	public synchronized void putAll(Map<? extends K, ? extends V> m) {
		super.putAll(m);
	}

	@Override
	public synchronized V remove(Object key) {
		return super.remove(key);
	}

	@Override
	public synchronized void clear() {
		super.clear();
	}
	

}
