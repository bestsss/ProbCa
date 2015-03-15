package bestsss.cache;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
/*
 * Written by Stanimir Simeonoff and released as public domain as described at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

 /**
  * @author Stanimir Simeonoff
 */
public class InternMap<E> {
  @SuppressWarnings("serial")
  private static class Node<E> extends AtomicInteger{
    final E e;
    final int hash;
    public Node(E e, int hash) {    
      this.e = e;
      this.hash = hash;
    }
    @Override
    public String toString() {
      return String.format("%s:%d", e, get());
    }
  }
  private final static int[] PRIMES={
    0xf62df541, 0x9f3ceb2d, 0xf33e13d1, 0x8b35d0bd, 0x95c6c9f9, 0xabe45ee7, 0xafcdffdd, 0xba60c57d, 0xbd679d03, 0xffeea35f, 0x87aeea65, 0xb0e90a45, 0xb67b1269, 0xfa77f3af,
    0x97ac7827, 0x836f3251, 0xc5b9022b, 0xc49af59f, 0xd418d337, 0xadf7f029, 0x90da31e3, 0xedd00add, 0xfa445171, 0xdaf40c19, 0xa00ad7bb, 0x95db0021, 0x855435fd, 0x94ea54c3,
    0xbb2ac535, 0x8988f82d, 0xfb7c8483, 0xa63f1b91, 0x867094f9, 0xa2800579, 0xa56458ab, 0x9020f1d3, 0xc8468195, 0xa22e0fdf, 0xc34d51d5, 0xa62392b9, 0xc6f54635, 0xb41db895,
    0xd37e7a4f, 0x9a0dd78d, 0xffeba2eb, 0xa2646913, 0xe3437f33, 0x8424f851, 0xd3b1a885, 0xad8419d9, 0xb3f08ca5, 0xf6a82f2d, 0xd753e7a3, 0x9c6439d3, 0xcfe1b5af, 0xddcb2e2f,
    0xe11bab2b, 0xea1698d7, 0xdf9fa3e7, 0xb42d40bd, 0xab653e67, 0xeca67e1f, 0xc852e0c9, 0xe0a021a7,
  };
 
  
  //with a load factor of 0.5 it holds 64 elements which should be more than enough
  private final AtomicReferenceArray<Node<E>> table=new AtomicReferenceArray<>(128);
      
  private final int hashSeed = PRIMES[ThreadLocalRandom.current().nextInt(PRIMES.length)]*PRIMES[ThreadLocalRandom.current().nextInt(PRIMES.length)];//cheap-o random
  private final int length = table.length();
  
  private final AtomicInteger size=new AtomicInteger();
  
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
    if (r.nextInt(8)!=0){//12.5% chance to expunge, pow2 is much easier as it doesn't involve mod
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
    if (count>7 && ref!=null){//under load that ref might have been changed and even be the top dog, but we just expunge it
       if (table.compareAndSet(selected, ref, null)){                 
        size.decrementAndGet();
      }      
    }
  }


  private static int index(int hash, int len){
    return hash & (len-1);  
  }

  private static int nextKeyIndex(int i, int len) {
    return (1+i) & (len-1);
  }

  private int capacity(){
    return (length>>1)+(length>>2); //length * .75
  }
  
  private E getOrAddBelowSize(E e, int hash) {
    final AtomicReferenceArray<Node<E>> table = this.table;
//    if (table==null)//prevent traps below, the code seems to degrate performance, though, which is weird
//      return e;

    //pretouch table right here with .length()
    for (int len=table.length(), i=index(hash, len), maxAttempts=len>>4;;i=nextKeyIndex(i, len)){
      Node<E> value = table.get(i);
      if (value==null){        
        final AtomicInteger size = this.size;
        final int capacity = capacity();
        if (size.get()<capacity){//free add
          //technically all threads may fill up the table but well
          final Node<E> node = new Node<E>(e, hash);
          if (table.compareAndSet(i, null, node)){
            size.incrementAndGet();
            return e;//no hitcounts, on 1st add
          }

          if (size.get()<capacity){
            i=index(hash, len);//start over
            continue;
          }
        }
        return null;
      }
      
      if (hash==value.hash && e.equals(value.e)){//hash would load the cache line for value.e, so it should be a free check
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