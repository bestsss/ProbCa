package bestsss.cache;

/*
 * Written by Stanimir Simeonoff and released as public domain as described at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

 /**
  * @author Stanimir Simeonoff
 */
public interface CacheStatistics {

  double getHitRatio();
  
  double getEvictionTimeMillis();
  long getEvictions();

  double getExpirationTimeMillis();
  long getExpirations();

  

  long getHits();
  long getMisses();
  long getPuts();
  long getRemovals();
}