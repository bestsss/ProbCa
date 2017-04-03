package bestsss.cache.alloc;

import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class DelayedAllocator  {
  final BusyPool busy;//has to be array based
  final AtomicIntegerArray counters;
  final int length;
  final ArrayPool<Object[]>[] pool;
  
  @SuppressWarnings("unchecked")
  public DelayedAllocator() {
    int CPUs = Runtime.getRuntime().availableProcessors();
    length = CPUs>1?nextPow2(CPUs):1;
    counters = new AtomicIntegerArray(length>1?index(length):1);    
    busy = new BusyPool(Math.min(128, length*16));
    pool = new ArrayPool[256]; 
  }
  private static int nextPow2(int v){
    return Integer.highestOneBit(v)<<1;
  }
  
  public Object[] poll(){
    ArrayPool<Object[]> p = getPool(length);
    return p==null?null:p.poll();
  }
  
  public boolean offer(Object[] obj){
    if (!valid(obj))
      return false;
    
    if (anyInUse()){
      busy.offer(obj);
      return false;
    }
    expungeBusy();
    boolean result = regularOffer(obj);
    return result;
  }
  private void expungeBusy() {
    if (busy.lastUpdated==0)//cheap read, even with volatile
      return;
    
    //need CAS before entering sync
    synchronized (busy) {
      for (int i=0, len = busy.length();i<len;i++){
        Object[] o = busy.get(i);
        if (o==null){
          if (i>busy.lastUpdated)
            break;
          
          continue;
        }        
        regularOffer(o);
      }
      busy.lastUpdated = 0;
    }
  }
  private boolean anyInUse() {
    for (int i=0;i<length; i++){
      if (counters.get(index(i)) != 0)
        return true;
    }
    return false;    
  }
  
  private static int index(int i) {
    return i<<4;
  }
  
  public void lock(){
    int idx = threadId();
    counters.incrementAndGet(index(idx));
  }
  private int threadId() {
    return System.identityHashCode(Thread.currentThread()) & (length-1);
  }
  
  public void unlock(){
    int idx = threadId();
    counters.decrementAndGet(index(idx));
  }



  private boolean regularOffer(Object[] obj) {
    if (obj==null)
      return false;
    
    ArrayPool<Object[]> pool = getPool(obj.length);
    return pool!=null && pool.offer(obj);
  }

  private ArrayPool<Object[]> getPool(int length) {
    ArrayPool<Object[]> p=null;
    ArrayPool<Object[]>[] pool = this.pool;
    if (length<pool.length){
      p = pool[length];
      if (p==null){//racy set, but it doesn't matter - ArrayPool can be published via datarace
        pool[length]=p=new ArrayPool<>(2048);
      }
    }
    return p;
  }
  
  private boolean valid(Object[] obj) {
    return obj!=null && obj.length<pool.length;
  }
  
  static class BusyPool extends AtomicReferenceArray<Object[]>{    
    public BusyPool(int length) {
      super(length);
    }
    
    volatile int lastUpdated;
    void offer(Object[] o){
      for (int i=lastUpdated; i<length(); i++){
        if (get(i)==null && compareAndSet(i, null, o)){
          lastUpdated = i;//cheap non-volatile
          break;
        }
      }
    }
  }
}