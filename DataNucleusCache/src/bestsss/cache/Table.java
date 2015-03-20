package bestsss.cache;

import java.util.Comparator;
import java.util.List;
/*
 * Written by Stanimir Simeonoff and released as public domain as described at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

 /**
  * @author Stanimir Simeonoff
 */
public interface Table<K,V> {
  int size();
  V get(Object key);
  V put(K key, V value);
  V remove(K key);
  void clear();
  boolean isEmpty();
  
  List<K> getExpirable(int entries, final Comparator<V> comparator);
}
