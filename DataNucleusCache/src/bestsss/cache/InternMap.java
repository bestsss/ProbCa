package bestsss.cache;

import java.math.BigInteger;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class InternMap<E> {
  @SuppressWarnings("serial")
  private static class Node<E> extends AtomicInteger{
    final E e;
    public Node(E e) {    
      this.e = e;      
    }
	@Override
	public String toString() {
	  return String.format("%s:%d", e, get());
	}
  }
  private static final int prime = BigInteger.probablePrime(32, ThreadLocalRandom.current()).intValue();
  
  //with a load factor of 0.5 it holds 64 elements which should be more than enough
  final AtomicReferenceArray<Node<E>> table=new AtomicReferenceArray<>(128);
    
  
  final int hashSeed = prime  * ThreadLocalRandom.current().nextInt();//cheap-o random
  final int length = table.length();
  
  final AtomicInteger size=new AtomicInteger();
  
  public E intern(E e){
    if (e==null)
      return null;
    final int hash = hash(e);
    
    //linear probe to find 
    E result = getOrAddBelowSize(e, hash);
    if (result!=null){
      return result;
    }
    expungeOne();
    return e;
  }
  

  private void expungeOne() {
    final ThreadLocalRandom r = ThreadLocalRandom.current();
    if (r.nextInt(10)!=0){//10% chance to expunge
      return;
    }
    int selected = -1;
    int min = Integer.MAX_VALUE;
    Node<E> ref = null;
    int count = 0;//count non-null values
    for (int i=0, len=length; i<16; i++){//on average half should be nulls
      int idx = index(r.nextInt(len),len);
      Node<E> node = table.get(idx);
      if (node==null)
        continue;

      count++;
      int hits = node.get(); 
      if (hits < min){
        ref = node;
        min = hits;
        selected = idx;
      }        
    }
    if (count>7 && ref!=null){
      if (table.compareAndSet(selected, ref, null)){                 
        size.decrementAndGet();
      }      
    }
  }


  private static int index(int hash, int len){
    return hash & (len-1);  
  }

  private static int nextKeyIndex(int i, int len) {
    return (++i  < len ? i  : 0);
  }

  private int capacity(){
    return length>>1; 
  }
  
  private E getOrAddBelowSize(E e, int hash) {
    final AtomicReferenceArray<Node<E>> table = this.table;
    for (int len=length, i=index(hash, len), maxAttempts=len>>4;;i=nextKeyIndex(i, len)){
      Node<E> value = table.get(i);
      if (value==null){        
        if (size.get()<capacity()){//free add
          //technically all threads may fill up the table but well
          Node<E> node = new Node<E>(e);
          if (table.compareAndSet(i, null, node)){
        	size.incrementAndGet();
            return e;//no hitcounts, on 1st add
          }
          
          if (size.get()<capacity()){
            i=index(hash, len);//start over
            continue;
          }
        }
        return null;

      }
      else if (e.equals(value.e)){
        value.incrementAndGet();
        return value.e;
      }
      
      if (maxAttempts--==0){
        return e;//too many failures
      }
    }
  }


  private int hash(E k){
    int h = hashSeed;

    h ^= k.hashCode();
    h ^= (h >>> 20) ^ (h >>> 12);
    return h ^ (h >>> 7) ^ (h >>> 4);
  }


  @Override
  public String toString() {
	return String.format("InternMap [table=%s, length=%s, size=%s]", table, length, size);
  }
  
}