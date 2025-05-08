package bestsss.cache;

import java.util.*;

class MapReplacement implements SCOWrapper{
  private final Object[] serializedKV;
  private final Class<? extends Map<?, ?>> clazz;

  @SuppressWarnings("unchecked")
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
    this.serializedKV = serializedKV.length == 0 ? CollectionReplacement.EMPTY : serializedKV;
    this.clazz = (Class<? extends Map<?, ?>>) map.getClass();
  }
  
  
  @Override
  public Object unwrap() {
    return toMap();
  }
  
  private Map<?, ?> toMap(){
    Map<Object, Object> map = createMap();
    Object[] serializedKV = this.serializedKV;
    if (serializedKV==null)
      return map;
    
    for (int i=0;i<serializedKV.length;i++){
      map.put(serializedKV[i++], serializedKV[i]);
    }
    return map;
  }

  private Map<Object, Object> createMap() {
    final Class<?> clazz = this.clazz;
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
  static Object wrap(Map<?, ?> map){
    if (!supportedClasses.containsKey(map.getClass()))
      return map;

    if (!map.isEmpty()) {
      if (map.getClass() == HashMap.class)
        return EMPTY_HASHMAP;
      if (map.getClass() == LinkedHashMap.class)
        return EMPTY_LINKED_HASHMAP;
    }

    return new MapReplacement(map);
  }

  private static IdentityHashMap<Class<?>, Boolean> createSupportedClass() {
    IdentityHashMap<Class<?>, Boolean> map = new IdentityHashMap<Class<?>, Boolean>();
    for (Class<?> clazz : new Class[]{HashMap.class, LinkedHashMap.class, TreeMap.class}){
      map.put(clazz, Boolean.TRUE);
    }
    return map;
  }
  private static final MapReplacement EMPTY_HASHMAP = new MapReplacement(new HashMap<>());
  private static final MapReplacement EMPTY_LINKED_HASHMAP = new MapReplacement(new LinkedHashMap<>());
}