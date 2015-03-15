package bestsss.cache.test;

import java.util.AbstractMap;
import java.util.Set;

import bestsss.cache.ClosedHashTable;
/**
 * Written by Stanimir Simeonoff and released as public domain as described at
 * http://creativecommons.org/publicdomain/zero/1.0/
 * @author Stanimir Simeonoff
 */
public class MapAdaptor<K,V> extends AbstractMap<K, V> {
  final ClosedHashTable<K, V> table = new ClosedHashTable<>();
  
  @Override
  public Set<java.util.Map.Entry<K, V>> entrySet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    return table.size();
  }

  @Override
  public boolean isEmpty() {
    return table.isEmpty();
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean containsKey(Object key) {
    return table.get((K) key)!=null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public V get(Object key) {
    return table.get((K) key);
  }

  @Override
  public V put(K key, V value) {
    return table.put(key, value);
  }

  @Override
  public V remove(Object key) {
    return table.remove(key);
  }

  @Override
  public void clear() {
    table.clear();
  }
  

}
