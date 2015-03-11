package bestsss.cache;

import java.util.ArrayDeque;
import java.util.Arrays;

//allocates new arrays up for 256 fields
class Allocator {
  private static final int SHIFT = 2;//each 4 size share same element;

  private static final int OR = 1<<SHIFT;
  private static final int MASK = -OR; //~(OR-1);//zero last 2 bits, so it's always div by 4

  final int maxPooled;
  final ArrayDeque<Object[]>[] pools;
  
  @SuppressWarnings("unchecked")
  Allocator(int maxPooled){
	this.maxPooled = maxPooled>0?maxPooled:512;
    pools = new ArrayDeque[idx(256)];
    for (int i = 0; i < pools.length; i++) {
      pools[i]=new ArrayDeque<Object[]>();
    }
  }
  
  Object[] get(int length){
    final  ArrayDeque<Object[]> pool = getPool(idx(length));
    if (pool==null){
      return zap(new Object[length]);//don't cache 
    }
    final Object[] result;
    synchronized(pool){
      result = pool.pollLast();
    }
    return result!=null?result:zap(new Object[length(length)]);
    
  }

  private Object[] zap(Object[] result) {
    Arrays.fill(result, CachedX.NOT_PRESENT);
    return result;
  }
  
  private static int idx(int length) {
    return length>>SHIFT;//each 4 size share same element
  }

  void offer(Object[] array){
	if (array.length!=length(length(MASK)))
	  return;
	
    final ArrayDeque<Object[]> pool = getPool(idx(array.length));
    if (pool==null)
      return;
    
    final int maxPooled = this.maxPooled;
    if (pool.size()>= maxPooled){//NOTE: this is benign data race, and relied on the impl of ArrayDeque
      //it tried to save the sync. and zap costs, and it will be correct over 99% of time when the pool is full
      //still it's a data race that any static analysis is to catch and warn about
      return;
    }
    zap(array);//zap before synchronized
    synchronized(pool){
      if (pool.size()< maxPooled){
        pool.offer(array);
      }
    }
  }
  
  private ArrayDeque<Object[]> getPool(int idx){
    ArrayDeque<Object[]>[] pools = this.pools;
    return idx>=0 && idx<pools.length?pools[idx]:null;
  }
  
  public int size(){//JMX
    int size = 0;
    for (ArrayDeque<?> q : pools){
      synchronized (q) {
        size+=q.size();
      }
    }
    return size;
  }

  public static int length(int length) {
	return (length+OR)&MASK ;//round to the next
  }
}
