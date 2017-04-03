package bestsss.cache;

import java.util.*;

import java.util.function.Supplier;
/*
 * Written by Stanimir Simeonoff and released as public domain as described at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
 * Stores collection elements in an optimized memory form, i.e. Object[]. Node alike collections (HashSet/LinkedList) have higher memory footprint than the comparable Object[]
* @author Stanimir Simeonoff
*/
class CollectionReplacement implements SCOWrapper{
  private static final Object[] EMPTY ={}; 
  final Object[] elements;
  final java.util.function.Supplier<Collection<Object>> ctor;
  
  
  public CollectionReplacement(Object[] elements, Supplier<Collection<Object>> ctor) {
    super();
    this.elements = elements;
    this.ctor = ctor;
  }
  
  public Collection<?> unwrap(){ 
    Collection<Object> c = ctor.get();
    for(Object o : elements){
      c.add(o);
    }
    return c;
  }
  
  static Object wrap(Collection<?> c){
    final Supplier<Collection<Object>> ctor;
    if (c instanceof ArrayList) {
      ctor = ArrayList::new;
    } else if (c instanceof LinkedHashSet<?>) {
      ctor = LinkedHashSet::new;
    } else if (c instanceof HashSet<?>) {
      ctor = HashSet::new;
    } else if (c instanceof LinkedList<?>) {
      ctor = LinkedList::new;
    } else if (c instanceof TreeSet && ((TreeSet<?>)c).comparator()==null){
      ctor = TreeSet::new;
    } else{
      return c;
    }
    
    return new CollectionReplacement(c.isEmpty()?EMPTY:c.toArray(), ctor);
  }
}
