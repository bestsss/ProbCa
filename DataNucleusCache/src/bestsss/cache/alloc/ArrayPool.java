package bestsss.cache.alloc;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class ArrayPool<E> extends AtomicReferenceArray<E> {

  final AtomicLong putIndex=new AtomicLong();  
  final AtomicLong takeIndex=new AtomicLong();
  
  
  public ArrayPool(int length) {
    super(assertPow2(length));
    
  }

  private static int assertPow2(int len) {
    if (len<64 || (len & (len-1))!=0){
      throw new IllegalArgumentException();
    }
    return len;
  }

  private int idx(long index){
    int i = (int)(index &(length()-1));
    int lower = i & 63;
    //swap lowest 2 bit with 4th and 5th
    int ofs = i&3;    
    lower>>>=2;
    lower|=ofs<<4;

    return lower | (i & ~63);//rebuild back
  }
  
  public boolean offer(E e){
    for(;;){
      //proper acquisition order: takeIndex, then putIndex
      long take = this.takeIndex.get();
      long put = this.putIndex.get();
      
      if ( (int) (put - take) >=length()){ 
        return false;
      }
      if (!putIndex.compareAndSet(put, ++put)){
        continue;
      }
      lazySet(idx(put), e);
      return true;
    }   
  }
  public int size(){
    return (int) (-takeIndex.get() + putIndex.get())&(length()-1);//the order must remain
  }
  public E poll(){
    for(;;){
      long take = this.takeIndex.get();
      long put = this.putIndex.get();      
      if (take==put){
        return null;
      }
      
      final int i = idx(take);
      final E e = get(i);
      if (e==null){
        //got race now, it's either the offer or a very stale poll()
        if (put - take > length() >>1){//at least half empty
          if (takeIndex.compareAndSet(take, take+1)){//advance arbitrary
            continue;
          }
          return null;
        } 
      }
      if (takeIndex.compareAndSet(take, take+1) && compareAndSet(i, e, null)){//race between poll() can stall in ... if (e==null) return null 
        return e;     
      }      
    }
  } 
}