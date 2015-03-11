package bestsss.cache;

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