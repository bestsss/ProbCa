package bestsss.cache;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import bestsss.cache.sort.CacheComparator;

/**
 *
 * 
 * CloseHashTable, i.e. open addressing
 * Main aim is low memory footprint and less garbage collection pressure, along with reasonable concurrency.
 * Presently in MapLoops tests in java.util.concurrent this implementation outperforms java7's CHM.
 *  
 * @author Stanimir Simeonoff
 * 
 * @param <K>
 * @param <V>
 */
@SuppressWarnings("unchecked")
public class  ClosedHashTable<K, V> implements Table<K, V>{
  private static final int CPUs = Runtime.getRuntime().availableProcessors();

  private static final SecureRandom RANDOM = new SecureRandom();
  private static final Object TOMBSTONE = "TOMBSTONE".toCharArray();//must not have equals
  private static final int MAX_SEGMENT_LENGTH = 1<<29;

  private static final boolean BAD_EXPUNGE = Boolean.getBoolean("ClosedHashTable.badExpunge");

  private final int hashSeed = BigInteger.probablePrime(32,RANDOM).intValue();//randomness on each start


  private final Segment[] segments;//sort of need volatile read/write
  private final int shiftSegment;
  
  private float loadFactor = 0.67f;

  public ClosedHashTable(){
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

  public boolean isEmpty(){//check segments, assume non-empty if any size is non zero
    final Segment[] segments = this.segments;
    for (Segment segment : segments){
      if (segment.size()>0){
        return true;
      }
    }
    return false;
  }

  public int size(){//sum up all sizes and check for overflow
    int size = 0;

    final Segment[] segments = this.segments;
    for (Segment segment : segments){
      size+=segment.size();
      if (size<0){
        return Integer.MAX_VALUE;
      }      
    }

    return size;
  }

  public long tombstones(){//sum up all sizes, return long skip overflows
    long tombstones = 0;

    final Segment[] segments = this.segments;
    for (Segment segment : segments){
      tombstones+=segment.tombstones();
    }
    return tombstones;
  }
  

  public final V remove(Object key) {
    return put((K) key, null);
  }
  
  public V put(K key, V value){//lock all through the bitter end of closeDeletion
    final int hash = hash(key);
    final boolean isRemove = value==null;
    for(;;){
      Segment segment = selectSegment(hash);
      for (;segment.isReplacementActive();){
        //resize (or deletion fix) in progress, don't help or anything (the expected architecture has too few cores) 
        segment.lock.lock();
        segment.lock.unlock();
        segment = selectSegment(hash);      
      }
      Object result = TOMBSTONE;
      final int len=segment.length();
      int i=index(hash, len);
      final int start=i;
      int lockIndex=-1;
      for (int loop=0, failures = 0;;) {
        if (lockIndex!=Segment.getLockIndex(i) && segment.lock(i)<0){
          processLoopCounter(loop++);
          continue;
        }
        lockIndex=Segment.getLockIndex(i);
        
        final Object loadedKey = segment.get(i);
        if (loadedKey==null || equals(key, loadedKey)){
          result = loadedKey==null?null: (V) segment.get(i+1);
          if (!isRemove){          
//            if (loadedKey==null)
//              segment.lazySet(i, key);
            
            if (!segment.compareAndSet(i, loadedKey, key)){
              badKey(segment, loadedKey, key); 
            }
            segment.lazySet(i+1, value);
                        
          } 
          else if (loadedKey!=null){//remove and found
            segment.lazySet(i+1, null);
            segment.lazySet(i, null);
            closeDeletion2(segment, i, len, start);
          }
          break;
        }
        if (failures++ > (len>>>4)){
          resize(segment, hash);
          break;
        }
        i=nextKeyIndex(i, len);
      }
      
      unlockFromTo(segment, start, i, len);
      if (segment.isReplacementActive()){//start all over
        continue;
      }
      if (result!=TOMBSTONE){
        int delta = changeSize(result, isRemove);
        segment.addSize(delta);
        if (delta>0 && segment.size()>segment.threshold){
          resize(segment, hash);
        }
        return (V) result;
      }
    }
  }
  private void unlockFromTo(Segment s, int first, int last, int len){
    int startLock = Segment.getLockIndex(first);
    int lastLock = Segment.getLockIndex(last);
    
    for (int i=startLock, idx=first; ;){
      int lock = s.changeLock.get(i);
      Segment.assertLocked(lock--);
      s.rawUnlock(i, lock);
      if (i==lastLock){
        break;
      }
      int j;
      for (;(j=Segment.getLockIndex(idx = nextKeyIndex(idx, len)))==i;);
      i =j;
    }    
    
  }
  
  private static int changeSize(Object result, boolean isRemove){
    if (result==null){
      return isRemove?0:1;//added, previously none
    }
    return  isRemove?-1:0;    
  }
  
  private int closeDeletion2(Segment s, int deleted, final int len, int firstLocked) {
    //Knuth Section 6.4
    
      //deletion attempts to lock the cells
      //and proceed with freeing up the TOMBSTONE 
      //if any locking fails, skip over
//      LinkedHashMap<Integer, Integer> map=new LinkedHashMap<Integer, Integer>();
      int start=-1,end=0;
      boolean fullLoop = false;
      int result = 0;
      int lockIndex = Segment.getLockIndex(deleted);
      firstLocked= Segment.getLockIndex(firstLocked);
      int i = nextKeyIndex(deleted, len);
      for (int loop=0;;){
        int currentLockIndex = Segment.getLockIndex(i);
        
        if (currentLockIndex==firstLocked)
          fullLoop = true;
        
        if (!fullLoop &&  lockIndex != currentLockIndex){
          if ((s.lock(i))<0){          
            processLoopCounter(loop++);
            continue;
          }
          lockIndex = currentLockIndex;
          if (start<0){
            start =i;
          }
          end=i;
        }
        K k = (K) s.get(i);      
        if (k==null){
          break;
        }

        int r = index(hash(k), len);//natural position
        if ((i < r && (r <= deleted || deleted <= i)) || (r <= deleted && deleted <= i)) {

          Object value = s.get(i+1);

          s.lazySet(deleted+1, value);
          s.lazySet(deleted, k);//move the key after the value, zero denotes, no entry


          s.lazySet(i, null);
          s.lazySet(i+1, null);

          deleted=i;
          result++;
        }
        i = nextKeyIndex(i, len);
      }
      if (start>=0)
        unlockFromTo(s, start, end, len);
      
      return result;
    }

  public V __put(K key, V value){

    final int hash = hash(key);
    final boolean isRemove = value==null;

    for(;;){
      Segment segment = selectSegment(hash);
      for (;segment.isReplacementActive();){
        //resize (or deletion fix) in progress, don't help or anything (the expected architecture has too few cores) 
        segment.lock.lock();
        segment.lock.unlock();
        segment = selectSegment(hash);      
      }
      
      for (int len=segment.length(), i=index(hash, len), loop=0, failures = 0;;) {

        final Object loadedKey = segment.get(i);
        if (loadedKey==null || (loadedKey==TOMBSTONE && !isRemove) || equals(key, loadedKey)){//tombstones are ok to put
          final int lock = segment.lock(i);
          if (lock<0){
            processLoopCounter(loop++);
            continue;//more looping
          }
          boolean exit;
          V result = null;
          if (loadedKey==null && isRemove){//removal but didn't find the key
            exit = (null== segment.get(i));//matching loadedKey in the lock, so no changes
          } else{
            //locked now, CAS ensure no changes to the loaded key, which we care about
            exit = segment.compareAndSet(i,loadedKey, isRemove?TOMBSTONE:key);//value==null, remove key as well
            if (exit){
//              result = (V) segment.getAndSet(i+1, value);//expensive a bit, lazy set is way cheaper
              result = (V) segment.get(i+1);
              segment.lazySet(i+1, value);
              
            }
          }
          segment.unlock(i, lock);

          if (exit){
            if (segment.isReplacementActive()){
              //need to repeat the process
              //in the new segment now
              break;
            }
            
            if (!isRemove){
              if (loadedKey==TOMBSTONE){
                //replaced a tombstone, decrease their count
                segment.addTobmstone(-1);
              }
              if (result==null){//previously the cell was empty
                int size = segment.addSize(1);
                if (size > segment.threshold){
                  resize(segment, hash);
                }
              }
            } else{
              if (result!=null){//removal successful, reduce size, increase tombstones
                segment.addSize(-1);
                int tombstones;
                if (len>64 && (tombstones=segment.tombstones()) > (len>>>5) ){//try closeDeletion, if successful, dont cas the tombstones
                  //do nothing, save the cas
                  int closed = closeDeletion(segment, i, len)-1;
                  if (closed>0){
                    segment.addTobmstone(-closed);
                  }
                }
                else {
                  tombstones  =  segment.addTobmstone(1);
                }
                if (tombstones > (len>>>4)){//more than 12.5% (should be 25%) tombstones, attempt to clear up
                  int expunged = expungeTombstones(segment);
                  if (BAD_EXPUNGE && expunged>0 && expunged < tombstones>>1){
                    badExpunge(segment, expunged);
                  }
                }               
              }              
            }

            return result;
          }

          if (failures++>len >>> 4 && len<MAX_SEGMENT_LENGTH){// 1/8 failures, increase the size
            resize(segment, hash);          
            break;//start all over          
          }
          
          i=index(hash, len);//start over
          continue;
        }
        
        i = nextKeyIndex(i, len);
        if (i==index(hash, len)){//started all over
          if (len<MAX_SEGMENT_LENGTH){
            resize(segment, hash);
            break;
          }
          throw new IllegalStateException("No capacity");
        }
      }
    } 
  }

  private void badExpunge(Segment s, int expunged) {    
    System.err.println("badExpunged: "+expunged+" counted: "+s.countTombstones());
  }

  private int segmentIndex(int hash, int length) {
    return (hash>>>shiftSegment) & (length-1);
  }

  private Segment resizeSegmentImpl(final Segment s, final int segmentIndex) {
    final int length = s.length()<<1;
    if (length>MAX_SEGMENT_LENGTH){
      //expunge deleted
      expungeTombstones(s);
      return s;
    } 
    if (!s.lock.tryLock()){
      return s;
    }
    try{
      {//check a replacement already in progress
        Segment replacement = s.replacement; 
        if (replacement!=null){
          return replacement;
        }
      }

      Segment resized = new Segment(length, this.loadFactor, s.lock);
      if (!s.casReplacement(null, resized)){//mark resize in progress
        return s;
      }
      //fill the new segment with the exisiting elements, i.e. rehash
      int size=0;
      for (int len=s.length(), i=0; i<len;i+=2){
        int lock = s.lock(i);
        if (lock<0)
          continue;
        
        K key = (K) s.get(i);
        Object value = s.get(i+1);
        s.unlock(i, lock);
        
        if (key==null || key==TOMBSTONE){//skip empty and deleted          
          continue;
        }
        
        if (value==null ){
          continue;
        }
        
        int index = index(hash(key), length);        
        for(;;){        
          if (resized.get(index)==null){
            resized.lazySet(index, key);
            resized.lazySet(index+1, value);
            break;
          }
          index=nextKeyIndex(index, length);
        }
        size++;
      }
      resized.addSize(size);

      segments[segmentIndex] = resized;
      s.replacement = resized;//force volatile write, get reads replacement
      return resized;
    }finally{
      s.lock.unlock(); 
    }
  }

  private int expungeTombstones(Segment s) {
    if (!s.lock.tryLock()){
      return  0;
    }    
    try{
      if (!s.casReplacement(null, s)){//can't CAS, someone else set it
        return  0;
      }
      try{
        int closedTombstones=0;
        for (int len=s.length(), i=0; i<len;i+=2){
          if (s.get(i)!=TOMBSTONE){
            continue;
          }
          closedTombstones+=closeDeletion(s, i, len);
        }
        s.addTobmstone(-closedTombstones);
        return closedTombstones;
      }finally{
        if (!s.casReplacement(s, null)){
          throw new AssertionError();
        }
      }
    }finally{
      s.lock.unlock();
    }   
  }

  private int closeDeletion(Segment s, int d, final int len) {
	//Knuth Section 6.4
	
    //deletion attempts to lock the cells
    //and proceed with freeing up the TOMBSTONE 
    //if any locking fails, skip over

    int start=d;    
    if (s.lock(start)<0){
      return 0;
    }
    final int startLock = Segment.getLockIndex(start);
    if (s.get(start)!=TOMBSTONE){//not a tombstone under the lock, unlock and exit
      //no modification made, hacked unlock by reverting the value
      s.changeLock.decrementAndGet(startLock);
      return 0;
    }

    int result = 0;
    int lockIndex = startLock;    
    for (int i = nextKeyIndex(d, len);;i = nextKeyIndex(i, len)){
      if (lockIndex != Segment.getLockIndex(i)){
        if (s.lock(i)<0){          
          break;//if failed to lock, fail it all and proceed to unlock
        }
        lockIndex = Segment.getLockIndex(i);        
      }
      K k = (K) s.get(i);      
      if (k==null){
        if (result==0){
          s.lazySet(start, null);
          result++;
        }
        break;
      }
      if (k==TOMBSTONE){
        continue;
      }

      int r = index(hash(k), len);//natural position
      if ((i < r && (r <= d || d <= i)) || (r <= d && d <= i)) {

        Object value = s.get(i+1);

        s.lazySet(d+1, value);
        s.lazySet(d, k);


        s.lazySet(i, null);
        s.lazySet(i+1, null);

        d=i;//d is modified, one tombstone less, keep moving the rest, skip tombstones, though
        result++;
      }
    }

    for (int i=startLock; ;){
      int lock = s.changeLock.get(i);
      Segment.assertLocked(lock--);
      s.rawUnlock(i, lock);
      if (i==lockIndex){
        break;
      }
      int j;
      for (;(j=Segment.getLockIndex(start = nextKeyIndex(start, len)))==i;);
      i =j;
    }
    return result;
  }

  private static void processLoopCounter(int loop) {
    if (CPUs==1 || (loop & 0x3ff)==0x3ff){//1023, park a bit
      LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100*Math.min(10, 1+(loop>>>10))));//increase sleep each time
      return;
    }

    if ((loop & 0x7f)==0x7f){//once 127 loops, Thread.yeild
      Thread.yield();
    }
  }

  public V get(final Object key){
    final int hash = hash(key);
    Segment segment = selectSegment(hash);
    
    for (int len=segment.length(), i=index(hash, len),loops=0,tombstones=0,total=0;;) {
      final int lock = segment.getChangeLock(i);
      if (Segment.isLocked(lock)){//locked, technically can skip and look at the next element in the array
        //but collisions should not be so common to warrant very high complexity
        processLoopCounter(loops++);
        continue;
      }

      Object loadedKey =  segment.get(i);      
      Object loadedValue = segment.get(i+1);

      if (loadedKey==null){
        return null;
      }

      if (lock != segment.getChangeLock(i)){
        processLoopCounter(loops++);
        continue;
      }
      
      if (loadedKey!=TOMBSTONE ){
        if (equals(key, loadedKey)){
          if (!loadedKey.equals(loadedValue)){
            badKey(segment, loadedKey, loadedValue);
          }
          return (V) loadedValue;
        }
      }
      else{
        if (++tombstones==(len>>>2)){//help expunge
          if (total > len<<1){
            resizeForGet(segment, hash);
            tombstones = 0;
          }
          else if (expungeTombstones(segment)>0){
            tombstones = 0;
            i=index(hash, len);//start over
            continue;
          }
        }
        else if (tombstones==len>>1){
          resizeForGet(segment, hash);          
        }
      }
    
      if (++total>len){
        resize(segment, hash);
      }
      i = nextKeyIndex(i, len);      
      if (segment.replacement!=null){//force replacement+
        Segment sPrime = selectSegment(hash);//set in the main array when done
        if (sPrime!=segment){
          segment = sPrime;
          len = segment.length();
          i=index(hash, len);
          tombstones = 0;
          total = 0;          
        }        
      }
    }   
  }

  private void badKey(Segment segment, Object loadedKey, Object loadedValue) {
    segment.countTombstones();
  }

  private void resizeForGet(Segment segment, int hash) {
    if (resize(segment, hash)==segment && segment.length()<MAX_SEGMENT_LENGTH){;//breakpoint friendly
      segment.lock.lock();
      segment.lock.unlock();//wait for completion
    }
  }

  private Segment resize(Segment segment, final int hash) {
    return resizeSegmentImpl(segment, segmentIndex(hash, segments.length));
  }

  private Segment selectSegment(int hash) {
    Segment[] segments = this.segments; 
    return segments[segmentIndex(hash, segments.length)] ;
  }

  private static boolean equals(Object key, Object loadedKey) {
    return key==loadedKey || key.equals(loadedKey);
  }

  protected int hash(Object k){
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

  private static int nextKeyIndex(int i, int len) {
    return (i + 2 < len ? i + 2 : 0);
  }
  private static int index(int hash, int len){
    return hash & (len-2);//len must be power of 2; -2 ensures the lowest bit is zero'd, so it selects a key  
  }

  @SuppressWarnings("serial")
  private static class Segment extends AtomicReferenceArray<Object>{

    private static final AtomicReferenceFieldUpdater<Segment, Segment> replacementUpdater=
        AtomicReferenceFieldUpdater.newUpdater(Segment.class, Segment.class, "replacement");
    private final AtomicIntegerArray changeLock;
    private final Lock lock;
    final int threshold;

    volatile Segment replacement;

    public Segment(int length, float loadFactor, Lock lock) {
      super(length);//size is doubled
      this.threshold = Math.round( (length>>>1)*loadFactor);

      this.lock = lock!=null?lock:new ReentrantLock();
      changeLock = new AtomicIntegerArray(Math.max(1, getLockIndex(length))+2);//2 extra for size and tombstone count
    }

    public boolean casReplacement(Segment existing, Segment replacement) {
      return replacementUpdater.compareAndSet(this, existing, replacement);
    }
    public boolean isReplacementActive(){
      Segment replacement = this.replacement;
      return replacement!=null && replacement!=this;
    }
    
    public int getChangeLock(int i){
      return changeLock.get(getLockIndex(i));
    }
    
    /**
     * 
     * @param i
     * @return the 
     */
    public int lock(int i){
      i = getLockIndex(i);
      int value = changeLock.get(i);
      if (1==(value&1)){
        return -1;//locked
      }
      if (!changeLock.compareAndSet(i, value, value+1)){//can't overlap here, as Integer.MAX_VALUE is odd
        return -2;//locking failed
      }
      return value;
    }

    public void unlock(int i, int locked){
      assertLocked(locked+1);
      i = getLockIndex(i);     

      //check overlapping;
      rawUnlock(i, locked);
    }

    void rawUnlock(int i, int locked) throws AssertionError {
      int next = (locked+2)&Integer.MAX_VALUE;//use 31bits here, the overlap is Integer.MIN_VALUE 
      if (!changeLock.compareAndSet(i, locked+1, next)){
        throw new AssertionError();//failed CAS on unlock!. should be just lazy set
      }
    }
    //note since length is pow2, no point to add
    int size(){
      return  changeLock.get(getLockIndex(length()));
    }
    int addSize(int delta){
      return changeLock.addAndGet(getLockIndex(length()), delta);
    }

    int tombstones(){
      return  changeLock.get(getLockIndex(length())+1);
    }
    int addTobmstone(int delta){
      return changeLock.addAndGet(getLockIndex(length())+1, delta);
    }

    static int getLockIndex(int i) {//due to false sharing there is no point of having separate locks per k/v
      return i>>>4;//a lock per 8 k/v pairs
    }

    public static boolean isLocked(int lock) {
      return 1==(lock&1);
    } 
    public static void assertLocked(int lock){
      if (!isLocked(lock))
        throw new AssertionError();
    }
    public int countTombstones(){
      int tombstones=0;
      for (int len=length(), i=0; i<len;i+=2){
        if (get(i)!=TOMBSTONE){
          continue;
        }
        tombstones++;        
      }
      return tombstones;   
    }

  }
  /////////////////////////////////Tombstobes///////////////////////////////////

  ///////////////////////////////// expiration /////////////////////////////////
  private static class IntSeq implements Iterator<Integer>{
    private int next;    
    private final int end;


    IntSeq(int start, int end){      
      this.end = end;
      this.next = start;
    }


    @Override
    public boolean hasNext() {
      return next<end;
    }


    @Override
    public Integer next() {
      if (next<end)
        return IntegerProvider.get(next++);
      throw new NoSuchElementException();
    }
    
    @Override public void remove() {throw new UnsupportedOperationException();}
  }
  /**
   * 
   * @param entries - number of items for expire 
   * @return Collection of suitable keys to expire
   */
  
  public List<K> getExpirable(int entries, final Comparator<V> comparator){
    //Probabilistic expiration
    final ThreadLocalRandom random = ThreadLocalRandom.current();

    final int sampleSize = entries * 17 ;//k=17 should be 99% chance to hit bottom 15%
    int i=0;
    final Object[] sample = new Object[sampleSize*2];

    final Segment[] segments=this.segments;
    int maxTestSegments= Math.min(4, segments.length);
    ArrayList<Segment> testedSegments =new ArrayList<>(maxTestSegments);
    
    for(;i<sample.length && testedSegments.size()<maxTestSegments;){
      final Segment segment = segments[random.nextInt(segments.length)];
      if (testedSegments.contains(segment)){
        continue;
      }
      testedSegments.add(segment);

      final int length = segment.length();    
      final FastIntSet set = new FastIntSet(sampleSize);    


      for(int retries = Math.max(1, sampleSize>>2); i<sample.length && retries>0;){//find not locked keys in the entire set 
        final int index = random.nextInt(length) & (Integer.MAX_VALUE-1);//lowest bit is 0, so always a key
        if (set.contains(index)){
          retries--;
          continue;
        }
        final int lock = segment.getChangeLock(index);

        if (Segment.isLocked(lock)){
          retries--;         
          continue;
        }
        final Object key = segment.get(index);
        final Object value = segment.get(index+1);

        if (key==null || key==TOMBSTONE || lock!=segment.getChangeLock(index)){//null is quite common, check it 1st
          retries--;
          continue;
        }

        sample[i++]=key;
        sample[i++]=value;
        set.add(index);//mark as processed
      }    
    }
    int count = i>>>1;
    CacheComparator<Integer> x=new CacheComparator<Integer>(){
      @Override
      public int compare(Integer o1, Integer o2) {
        V v1 = (V) sample[(o1<<1)+1];//values are the next
        V v2 = (V) sample[(o2<<1)+1];
        return  comparator.compare(v1, v2);
      }      
    };
    int resultCount = Math.min(entries,count);
    Collection<Integer> expirable = x.leastOf(new IntSeq(0, count), resultCount);
    ArrayList<K> keys = new ArrayList<>(expirable.size());
    for (Integer n : expirable){
      keys.add((K)sample[n<<1]);
    }
    return keys;
  }
  
  public void clear() {    
    final Segment[] segments = this.segments;
    for (int i=0;i<segments.length;i++){
      Segment s  = segments[i];
      s.lock.lock();
      try{
        segments[i] = new Segment(32, loadFactor, s.lock);
      }finally{
        s.lock.lock();
      }
    }
  }
}
