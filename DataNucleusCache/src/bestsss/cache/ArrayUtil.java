package bestsss.cache;

import java.util.Arrays;

import org.datanucleus.cache.CachedPC;

class ArrayUtil {
  private static final int CLASS = 1;
  private static final int VERSION = 2;
  private static final int HITS = 3;
  private static final int TIME = 4;
  static final int RESERVED = TIME;
  
  static Object[] newArray(CachedPC<?> pc, int length, Allocator allocator, int time) {
    length += RESERVED;
    Object[] fields = allocator.get(length);
    length = fields.length;
    
    fields[length-CLASS] = pc.getObjectClass();
    fields[length-VERSION] = pc.getVersion();
    fields[length-HITS] = IntegerProvider.ZERO;
    fields[length-TIME] = IntegerProvider.get(time);    
    return fields;
  }
  
  static Object[] extend(Object[] allFields, int currentLength, int index) {
    int len = Allocator.length( index+(1+4+RESERVED));//4 extra bytes
    Object[] copy = Arrays.copyOf(allFields, len);
    for (int i=currentLength; i<copy.length-RESERVED;i++){
      copy[i] = CachedX.NOT_PRESENT;
    }
    for (int i=1; i<=RESERVED ;i++){
      copy[copy.length-i] = allFields[allFields.length-i];
    }
    return copy;
  }
  
  static int maxLength(Object[] allFields) {
    return allFields.length - RESERVED;
  }
  static void incHitCount(Object[] fields){
    final int idx = fields.length - HITS;
	Object n = fields[idx];
    if (n instanceof Integer){
      n = IntegerProvider.get( ((Integer)n)+1);//this requires CAS not to miss some elements but we can live with non-precise
    } else{ 
      n = IntegerProvider.ZERO;
    }
    fields[idx] = n;	
  }
  
  static void setTimeAndHitCount(Object[] fields, int time){
    //increase hit count 
    //and set time    
	incHitCount(fields);
    fields[fields.length - TIME] = IntegerProvider.get(time);
  }

  public static int getTime(Object[] o1) {
    return (Integer) o1[o1.length - TIME];
  }

  public static int getHits(Object[] o1) {
    return (Integer) o1[o1.length - HITS];
  }
  public static Class<?> getClass(Object[] o1) {
	return (Class<?>)o1[o1.length - CLASS];
  }
}
