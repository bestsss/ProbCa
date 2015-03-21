package bestsss.cache;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;


public class HashTableV2<K,V> {
  private static final int CPUs = Runtime.getRuntime().availableProcessors();

  private static final SecureRandom RANDOM = new SecureRandom();
  private static final int MAX_SEGMENT_LENGTH = 1<<29;

  private final int hashSeed = BigInteger.probablePrime(32,RANDOM).intValue();//randomness on each start


  private final Segment[] segments;//sort of need volatile read/write
  private final int shiftSegment;
  
  private float loadFactor = 0.67f;

  static class Node<K, V> extends AtomicReference<V>{
    final int hash;
    final K key;
    volatile Node<K,V> next;
    
    public Node(int hash, K key, V initialValue) {
      super(initialValue);
      this.hash = hash;
      this.key = key;
    }
    public Node(int hash, K key, V initialValue, Node<K,V> next) {
      this(hash, key, initialValue);
      this.next = next;
    }
    
    V find(int hash, Object key){
      //cant have forwards          
      for (Node<K,V> e= this; ;){
        if (hash==e.hash && HashTableV2.equals(key, e.key)){
          return e.get();
        }
        if  ((e=(e.next))==null){
          return null;
        }
      }
      
    }
  }
  
//  private static final Node<Object, Object> TOMBSTONE = new Node<>(0, null, null);
  private static final int SIZE_MASK = Integer.MAX_VALUE>>1;//2bits
  
  static class Segment extends AtomicReferenceArray<Object>{
    
    private final ReentrantLock lock;    
    private final AtomicIntegerArray modCount;
    final int  threshold;
    volatile Segment nextSegment;
    
    Segment(int length, float loadFactor, ReentrantLock lock){
      super(length<<1);
      this.threshold = Math.round(length*loadFactor);
      this.lock = lock;
      this.modCount = new AtomicIntegerArray(length+1); 
    }
    
    public int size(){
      return modCount.get(modIndex(length()));

    }
    static int modIndex(int i) {//due to false sharing there is no point of having separate locks per k/v
      return i;
      //return i>>>4;//a lock per 8 k/v pairs
    }

    public int modCount(int i) {
      return modCount.get(modIndex(i));
    }

    public int addSize(int delta) {
      return modCount.addAndGet(modIndex(length()), delta);
    }

    public int incModCount(int i) {
      return modCount.incrementAndGet(modIndex(i));
    }    

    public int lock(int i){
      i = modIndex(i);
      int value = modCount.get(i);
      if (1==(value&1)){
        return -1;//locked
      }
      if (!modCount.compareAndSet(i, value++, value)){//can't overlap here, as Integer.MAX_VALUE is odd
        return -2;//locking failed
      }
      return value;
    }
    
    public static boolean isLocked(int lock) {
      return 0!=(lock&1);
    } 
    public static void assertLocked(int lock){
      if (!isLocked(lock))
        throw new AssertionError();
    }

    public void unlock(int i, int lock) {
      i = modIndex(i);
      assertLocked(lock);
      if (!modCount.compareAndSet(i, lock, (lock+1)&Integer.MAX_VALUE))
        assertLocked(0);
    }
  }
  
  public HashTableV2(){
    Segment[] segments = new Segment[4 * nextPow2(Math.max(2,CPUs))];
    for (int i = 0; i < segments.length; i++) {
      segments[i] = new Segment(32, loadFactor, new ReentrantLock());
    }
    this.shiftSegment = Integer.numberOfLeadingZeros(segments.length);
    this.segments = segments;
  }
  private static int nextPow2(int v){
    return Integer.highestOneBit(v-1)<<1;
  }
  
  protected int hash(K k){
    int h = hashSeed;

    h ^= k.hashCode();
//
    // Spread bits to regularize both segment and index locations,
    // using variant of single-word Wang/Jenkins hash.
    h += (h <<  15) ^ 0xffffcd7d;
    h ^= (h >>> 10);
    h += (h <<   3);
    h ^= (h >>>  6);
    h += (h <<   2) + (h << 14);
    return h ^ (h >>> 16);
  }
  private Segment selectSegment(int hash) {
    Segment[] segments = this.segments; 
    return segments[segmentIndex(hash, segments.length)] ;
  }
  private int segmentIndex(int hash, int length) {
    return (hash>>>shiftSegment) & (length-1);
  }


  private static boolean equals(Object key, Object loadedKey) {
    return key==loadedKey || key.equals(loadedKey);
  }


  private static int index(int hash, int len){
    return hash & (len-2);//len must be power of 2; -2 ensures the lowest bit is zero'd, so it selects a key  
  }

  public V get(K key){
    final int hash = hash(key);
    for(;;){
      Segment segment = selectSegment(hash);
      int len=segment.length(), i =index(hash, len);
      Object loadedKey = segment.get(i);
      if (loadedKey==null)
        return null;
      if (loadedKey instanceof Node){
        return ((Node<K, V>) loadedKey).find(hash, key);
      }

      if (equals(key, loadedKey)){
        int modCount = segment.modCount(i);
        Object value = segment.get(i+1);
        if (loadedKey == segment.get(i) && segment.modCount(i)==modCount){//inconsistent read, non-atomic, needs change counter
          return (V) value;
        }
        continue;//modCount failed, try again
      }
      return null;
    }
  }
  
  public V put(K key, V value){
    final int hash = hash(key);
    if (value==null){
      return replace(hash, key, value, null);
    }
    
    for(int loop=0;;){
      Segment segment = selectSegment(hash); 
      final int len=segment.length(), i =index(hash, len);
      Object loadedKey = segment.get(i);
      
      V result = null;
      if (loadedKey==null){//got position, totally empty
        if (!segment.compareAndSet(i, null, key)){
          continue;
        }
        result = (V) segment.get(i+1);
        //can check onlyIfAbsent
        
        segment.lazySet(i+1, value);
      }
      else if (loadedKey instanceof Node){//got full node        
        Node<K,V> node = (Node<K, V>) loadedKey;
        synchronized (node) {
          if (segment.get(i)!=node){//someone swapped us
            continue;          
          }
          for (Node<K, V> e = node;;){
            K eK;
            if (hash==e.hash && equals(key, e.key)){
              result = e.get();
              if (/*!onlyIfAbsent*/true){
                e.lazySet(value);
              }
              break;                
            }
            Node<K, V> pred = e;
            if ((e=e.next) == null){
              pred.next  = new Node<>(hash, key, value);
              break;
            }
          }
        }
      } else if (equals(key, loadedKey)){
        //same key update value, ok this is harder now
        int lock;
        if ((lock=segment.lock(i))<0){
          processLoop(loop++);
          continue;
        }
        try{
          result = (V) segment.get(i+1); 
          //onlyIfAbsent        
          if (loadedKey != segment.get(i) || !segment.compareAndSet(i+1, result, value)){
            continue;          
          }
        }finally{
          segment.unlock(i, lock);
        }
      } else{//must promote non equal key
        int lock;
        if ((lock=segment.lock(i))<0){
          processLoop(loop++);
          continue;
        }
        try{
          V otherVal = (V) segment.get(i+1); 
          if (loadedKey != segment.get(i) || otherVal==null){
            continue;
          }
          Node<K,V> self = new Node<>(hash, key, value);
          Node<K,V> first = new Node<>(hash((K)loadedKey), (K) loadedKey, otherVal, self);
          if (!segment.compareAndSet(i, loadedKey, first)){
            continue;
          }
          if (!segment.compareAndSet(i+1, otherVal, null)){
            throw new AssertionError();
          }
            
        }finally{
          segment.unlock(i, lock);
        }
      }
      
      if (result==null){
        if (segment.addSize(1)>segment.threshold){
          transfer(segment, hash);
        }
      }      
    }
  }
  
  private void transfer(Segment segment, int hash) {
    
  }
  private void processLoop(int loop) {
    if (CPUs==1 || (loop & 0x3ff)==0x3ff){//1023, park a bit
      LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100*Math.min(10, 1+(loop>>>10))));//increase sleep each time
      return;
    }

    if ((loop & 0x7f)==0x7f){//once 127 loops, Thread.yeild
      Thread.yield();
    }    
  }
  private V replace(int hash, K key, V value, Object existingValue) {
    return null;
  }


}

