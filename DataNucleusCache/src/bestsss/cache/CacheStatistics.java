package bestsss.cache;

/*
 * Written by Stanimir Simeonoff and released as public domain as described at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

 /**
  * @author Stanimir Simeonoff
 */
public interface CacheStatistics {
  public interface CacheStatisticsProvider{
    CacheStatistics getCacheStatistics();
  }
  double getHitRatio();
  
  double getEvictionTimeMillis();
  long getEvictedElements();
  long getEvictionCount();

  double getExpirationTimeMillis();
  long getExpiredElements();  
  long getExpirationCount();
  

  long getHits();
  long getMisses();
  long getPuts();
  long getRemovals();
}