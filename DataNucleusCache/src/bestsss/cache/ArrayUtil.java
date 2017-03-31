package bestsss.cache;

import java.util.Arrays;

import org.datanucleus.cache.CachedPC;

/*
 * Written by Stanimir Simeonoff and released as public domain as described at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

/**
 * @author Stanimir Simeonoff
 */
class ArrayUtil {
  //order has to be preserved
  private static final int CLASS = 1;
  private static final int VERSION = 2;
  private static final int HITS = 3;
  private static final int TIME = 4;
  private static final int ACCESS = 5;
  static final int RESERVED = ACCESS;

  static Object[] newArray(CachedPC<?> pc, int length, int time) {
    length += RESERVED;
    Object[] fields = newArray(length);
    length = fields.length;

    fields[length-CLASS] = pc.getObjectClass();
    fields[length-VERSION] = pc.getVersion();
    fields[length-HITS] = IntegerProvider.ZERO;
    fields[length-ACCESS] = fields[length-TIME] = getTime(time);
    return fields;
  }

  private static Object[] newArray(int length) {
    Object[] result = new Object[length];
    Arrays.fill(result, CachedX.NOT_PRESENT);
    return result;
  }
  
  static Object[] extend(Object[] allFields, int currentLength, int index) {
    int len = Allocator.length( index+(1+4+RESERVED));//4 extra slots
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
  
  private static void incHitCount(Object[] fields){
    final int idx = fields.length - HITS;
    Object n = fields[idx];
    if (n instanceof Integer){
      n = IntegerProvider.get( ((Integer)n)+1);//this requires CAS not to miss some elements but we can live with non-precise
    } else{ 
      n = IntegerProvider.ZERO;
    }
    fields[idx] = n;
  }
  
  //"cache" the last seen time, generally increments, so the within second there should be plenty of hits 
  private static Integer lastTime = IntegerProvider.ZERO;//accessed via races but volatile is overkill here (need lazySet instead)
  private static final Integer getTime(int time){
	  Integer last = lastTime;
	  if (last.intValue()==time)
		  return last;
	  last = IntegerProvider.get(time);
	  lastTime = last;
	  return last;
  }
  
  static void setTimeAndHitCount(Object[] fields, int time){
    //increase hit count 
    //and set time    
    incHitCount(fields);
    fields[fields.length - ACCESS] = getTime(time);
  }

  public static int getCreationTime(Object[] o1) {
    return (Integer) o1[o1.length - TIME];
  }

  public static int getHits(Object[] o1) {
    return (Integer) o1[o1.length - HITS];
  }
  public static Class<?> getClass(Object[] o1) {
    return (Class<?>)o1[o1.length - CLASS];
  }

  static int getAccessTime(Object[] o1) {
    return (Integer) o1[o1.length - ACCESS];
  }
  
  static void setTimeAndAccess(Object[] fields, int time){
    int length = fields.length;
    fields[length-ACCESS] = fields[length-TIME] = getTime(time);
    fields[length-HITS] = IntegerProvider.ZERO;
  
  }
}