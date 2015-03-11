package bestsss.cache;

import java.util.Comparator;
import java.util.List;

public interface Table<K,V> {
  int size();
  Object get(K key);
  Object put(K key, V value);
  void clear();
  boolean isEmpty();
  
  List<K> getExpirable(int entries, final Comparator<V> comparator);
}
