package bestsss.cache;

import java.util.concurrent.TimeUnit;

import jsr166e.LongAdder;

/**
 * @copyright Playtech 2014
 * @author Stanimir Simeonoff
 */
public final class Stats {
  
  private final LongAdder hits=new LongAdder();
  private final LongAdder misses=new LongAdder();
  private final LongAdder puts=new LongAdder();
  private final LongAdder removals=new LongAdder();
  
  private final LongAdder evictions=new LongAdder();
  private final LongAdder expirations=new LongAdder();
  
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
	evictions.add(size);
  }
  public void recordExpiration(long elapsedTime, int size) {
	expirationTime.add(elapsedTime);
	expirations.add(elapsedTime);
  }
  
  public double getEvictionTimeMillis(){
	return evictionTime.doubleValue() /TimeUnit.NANOSECONDS.toMillis(1);
  }
  public long getEvictions(){
	return evictions.longValue();
  }
  
  public double getExpirationTimeMillis(){
	return expirationTime.doubleValue() /TimeUnit.NANOSECONDS.toMillis(1);
  }
  public long getExpirations(){
	return expirations.longValue();
  }
    
  public double getHitRatio(){
	long hits = getHits();
	long misses = getMisses();
	
	return (double)hits/(hits+misses);
  }
  
  public long getHits(){
	return this.hits.longValue();	
  }
  public long getMisses(){
	return this.misses.longValue();		
  }
  public long getPuts(){
	return this.misses.longValue();		
  }
  public long getRemovals(){
	return this.removals.longValue();		
  }
  public long time() {
	return System.nanoTime();
  }
  
}
