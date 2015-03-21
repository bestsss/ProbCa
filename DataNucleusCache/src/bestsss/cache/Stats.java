package bestsss.cache;

import java.util.concurrent.TimeUnit;

import jsr166e.LongAdder;

/*
 * Written by Stanimir Simeonoff and released as public domain as described at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

/**
 * @author Stanimir Simeonoff
 */
public final class Stats implements CacheStatistics {

  private final LongAdder hits=new LongAdder();
  private final LongAdder misses=new LongAdder();
  private final LongAdder puts=new LongAdder();
  private final LongAdder removals=new LongAdder();

  private final LongAdder evictedElements=new LongAdder();
  private final LongAdder evictionCount=new LongAdder();
  
  private final LongAdder expiriredElements=new LongAdder();
  private final LongAdder expirationCount=new LongAdder();

  private final LongAdder evictionTime=new LongAdder();
  private final LongAdder expirationTime=new LongAdder();

  public void hit(){
    hits.increment();
  }
  public void miss(){
    misses.increment();  
  }

  public void recordGet(Object result) {
    (result!=null?hits:misses).increment();
  }

  public void recordPut(Object value) {
    (value!=null?puts:removals).increment();
  }

  public void put(){
    puts.increment();
  }

  public void recordEviction(long elapsedTime, int size) {
    evictionTime.add(elapsedTime);
    evictedElements.add(size);
    evictionCount.add(1);
  }
  
  public void recordExpiration(long elapsedTime, int size) {
    expirationTime.add(elapsedTime);
    expiriredElements.add(size);
    expirationCount.add(1);
  }

  public void recordRemoval(Object removed) {
    if (removed!=null)
      removals.add(1);
  }  

  @Override
  public double getEvictionTimeMillis(){
    return evictionTime.doubleValue() /TimeUnit.MILLISECONDS.toNanos(1);
  }

  @Override
  public long getEvictedElements(){
    return evictedElements.longValue();
  }
  @Override
  public long getEvictionCount() {
    return evictionCount.longValue();
  }
  
  @Override
  public double getExpirationTimeMillis(){
    return expirationTime.doubleValue() /TimeUnit.MILLISECONDS.toNanos(1);
  }

  @Override
  public long getExpiredElements(){
    return expiriredElements.longValue();
  }
  public long getExpirationCount(){
    return expirationCount.longValue();
  }


  @Override
  public double getHitRatio(){
    long hits = getHits();
    long misses = getMisses();

    return (double)hits/(hits+misses);
  }


  @Override
  public long getHits(){
    return this.hits.longValue();	
  }

  @Override
  public long getMisses(){
    return this.misses.longValue();		
  }

  @Override
  public long getPuts(){
    return this.misses.longValue();		
  }

  @Override
  public long getRemovals(){
    return this.removals.longValue();		
  }
  public long time() {
    return System.nanoTime();
  }
}
