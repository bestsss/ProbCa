package bestsss.cache;

import java.util.*;

public class MapReplacement {
  private final Object[] serializedKV;
  private final Class<? extends Map> clazz;
  private MapReplacement(Map<?, ?> map){
    Object[] serializedKV=new Object[map.size()<<1];//can use class as object at zero idx
    int i= 0;
    for (Map.Entry<?, ?> e : map.entrySet()){
      if (i==serializedKV.length){ 
        i=-1;
        break;
      }
      serializedKV[i++] = e.getKey();
      serializedKV[i++] = e.getValue();
    }
    this.serializedKV = serializedKV;
    this.clazz = map.getClass();
  }
  
  public Map<?, ?> toMap(){
    Map<Object, Object> map = createMap();
    Object[] serializedKV = this.serializedKV;
    if (serializedKV==null)
      return Collections.emptyMap();
    
    for (int i=0;i<serializedKV.length;i++){
      map.put(serializedKV[i++], serializedKV[i]);
    }
    return map;
  }

  private Map<Object, Object> createMap() {
    if (HashMap.class == clazz){
      return new HashMap<>(fittingSize());
    }
    if (LinkedHashMap.class == clazz){
      return new LinkedHashMap<>(fittingSize());
    }
    if (TreeMap.class == clazz){
      return new TreeMap<>();
    }
    
    return new LinkedHashMap<>();
  }

  private int fittingSize() {
    int len = serializedKV.length + (serializedKV.length>>2);
    return Integer.highestOneBit(len-1)<<1;
  }
  private static final IdentityHashMap<Class<?>, Boolean> supportedClasses=createSupportedClass();
  public static Object replacement(Map<?, ?> map){
    if (supportedClasses.containsKey(map.getClass()))
      return new MapReplacement(map);
    
    return map;
  }

  private static IdentityHashMap<Class<?>, Boolean> createSupportedClass() {
    IdentityHashMap<Class<?>, Boolean> map = new IdentityHashMap<Class<?>, Boolean>();
    for (Class<?> clazz : new Class[]{HashMap.class, LinkedHashMap.class, TreeMap.class}){
      map.put(clazz, Boolean.TRUE);
    }
    return map;
  }
  
  
}
