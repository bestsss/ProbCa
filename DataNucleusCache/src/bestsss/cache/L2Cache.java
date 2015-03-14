package bestsss.cache;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.datanucleus.NucleusContext;
import org.datanucleus.cache.CachedPC;
import org.datanucleus.cache.Level2Cache;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;

//all PCs are stored as Object[] (similar to JCredo), it's a lot better than the current CachedPC which is very memory unfriendly
public class L2Cache implements Level2Cache{
  
  private static final long serialVersionUID = 1L;

  private static final int MAX_EVICTION = 3;
  private static final int MAX_EXPIRATION = 7;
  
  private final long created = System.currentTimeMillis();
  
  Table<Object, Object[]> table = new ClosedHashTable<>();
  private final int maxElements;//65536 default
  private final Stats stats = new Stats();
  private final ConcurrentHashMap<String, InternMap<Object>> globalInterns = new ConcurrentHashMap<String, InternMap<Object>>();
  
  private static class ClassMeta{
	final Class<?> clazz;
	final int length;
	final boolean cacheable;
	final int expiration;
	final InternEntry[] interns;
	public ClassMeta(Class<?> clazz, InternEntry[] interns, int length, boolean cacheable, int expiration) {
	  super();
	  this.clazz=clazz;
	  this.interns = interns;
	  this.length = length;
	  this.cacheable = cacheable;
	  this.expiration = expiration;
	}

	public ClassMeta newLength(int length) {
	  return new ClassMeta(clazz, interns, length, cacheable, expiration);
	}
  }
  private static class EvictionInfo{
	int expiredAt;
	int evictedAt;
	EvictionInfo(int time){
	  this.expiredAt = time;
	  this.evictedAt = time;
	}
  }
  private static class InternEntry{
	static final InternEntry[] EMPTY={};
	final int field;
	final InternMap<Object> map;

	public InternEntry(int field, InternMap<Object> map) {
	  super();
	  this.field = field;
	  this.map = map;
	}
  
  }
  private final AtomicReference<IdentityHashMap<Class<?>, ClassMeta>> metaMap=new AtomicReference<IdentityHashMap<Class<?>,ClassMeta>>(new IdentityHashMap<Class<?>,ClassMeta>());
  private final Allocator allocator;
  private NucleusContext nucleusContext;
  
  private final ThreadLocal<EvictionInfo> evictionDone = new ThreadLocal<EvictionInfo>(){
	@Override
	protected EvictionInfo initialValue() {
	  return new EvictionInfo(time());
	} 	
  };
  
  public L2Cache(NucleusContext nucleusContext){
    this.nucleusContext = nucleusContext;
    this.maxElements = resolveMaxElements(nucleusContext);
    this.allocator = new Allocator(nucleusContext.getConfiguration().getIntProperty("bestsss.l2cache.maxPooled"));
  }

  private static int resolveMaxElements(NucleusContext nucleusContext) {
	int size = nucleusContext.getConfiguration().getIntProperty("bestsss.l2cache.size");
	if (size>0)
	  return size;

	long maxMem = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
	if (maxMem<0)
	  return 1<<16;

	return (int) ( maxMem/6656);//around 80k at 512MB
  }

  static Comparator<Object[]> newEvictionComparator(){
    return new Comparator<Object[]>() {          
    @Override
    public int compare(Object[] o1, Object[] o2) {
      int time1 =  ArrayUtil.getTime(o1);
      int hits1 = ArrayUtil.getHits(o1);
      int v1 = calcEntryValue(time1, hits1);
          
      int time2 = ArrayUtil.getTime(o2);
      int hits2 = ArrayUtil.getHits(o2);
      int v2 = calcEntryValue(time2, hits2);
      
      return v1-v2;//lowest values are the ones to be evicted, i.e.higher is better
    }   
  };
  }

  private static int calcEntryValue(int time, int hits){//the higher the better, each hit gives ~1.25seconds extra time
    return time+( hits*5 >> 2);//time+5*hits/4
  }
  
  Comparator<Object[]> newExpirationComparator(final int time){
	return new Comparator<Object[]>() {          
	  @Override
	  public int compare(Object[] o1, Object[] o2) {
		boolean e1 = isExpired(o1, time);
		boolean e2 = isExpired(o1, time);

		return e1==e2?0: (e1?-1:1);
	  }   
	};
  }
  
  boolean isExpired(Object[] o1, int time){
	int time1 =  ArrayUtil.getTime(o1);
	ClassMeta m1 = getMeta(ArrayUtil.getClass(o1));
	return time - time1 > m1.expiration;
  }
  
  
  private ClassMeta addMeta(Class<?> clazz, ClassMeta classMeta){
	for(;;){//copy on write
	  final IdentityHashMap<Class<?>, ClassMeta> map = metaMap.get();    
	  final ClassMeta existing  = map.get(clazz);
	  final int existingLength = existing!=null? existing.length : -1;      

	  if (existingLength >= classMeta.length){
		return existing;
	  }
	
	  IdentityHashMap<Class<?>, ClassMeta> update = new IdentityHashMap<>(map);
	  map.put(clazz, classMeta);
	  if (metaMap.compareAndSet(map, update)){
		return classMeta;
	  }
	}
  }
  
  
  protected int resolveExpiration(AbstractClassMetaData meta) {
	return 45;
  }
  
  protected boolean resolveCacheable(AbstractClassMetaData meta) {
	return meta==null || !Boolean.FALSE.equals(meta.isCacheable());
  }
  
  @Override
  public void close() {//do nothing, really  
  }

  @Override
  public void evict(Object oid) {
    evictImpl(oid);
  }

  private void recycle(Object object) {
    if (object instanceof Object[]){
      allocator.offer((Object[]) object);
    }
  }
  
  @Override
  public void evictAll() {
    table.clear();
  }

  @Override
  public void evictAll(Object[] oids) {
    for (Object key : oids){
      evictImpl(key);
    }
  }
  private void evictImpl(Object key) {
    recycle(table.put(key, null));
  }

  @Override
  public void evictAll(Collection oids) {
    for (Object key : oids){
      evictImpl(key);
    }
  }

  @Override
  public void evictAll(Class pcClass, boolean subclasses) {
    table.clear();//mass eviction
  }

  @Override
  public void pin(Object oid) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void pinAll(Collection oids) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void pinAll(Object[] oids) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void pinAll(Class pcClass, boolean subclasses) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unpin(Object oid) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unpinAll(Collection oids) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unpinAll(Object[] oids) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unpinAll(Class pcClass, boolean subclasses) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNumberOfPinnedObjects() {
    return 0;
  }

  @Override
  public int getNumberOfUnpinnedObjects() {
    return Math.max(0, table.size() - getNumberOfPinnedObjects());
  }

  @Override
  public int getSize() {
    return table.size();
  }

  @Override
  public CachedPC get(Object oid) {
	CachedPC<?> pc = assembleCachedPC(table.get(oid), oid);
	stats.recordGet(pc);
	return pc;
  }


  @Override @SuppressWarnings("rawtypes")
  public Map<Object, CachedPC> getAll(Collection oids) {
    LinkedHashMap<Object, CachedPC> result = new LinkedHashMap<>();
    for (Object key : oids){
      result.put(key, get(key));
    }
    return result;    
  }

  public CacheStatistics getStats(){
	return stats;
  }
  
  @Override
  public CachedPC put(Object oid, CachedPC pc) {
    return putImpl(oid, pc, true);//the return value is never used, so we can ignore it 
  }

  private CachedPC<?> putImpl(Object oid, CachedPC<?> pc, boolean assembleCached) {
	if (!getMeta(pc.getObjectClass()).cacheable){
	  return null;
	}	

    Object existing = table.put(oid, toArray(pc));
    recycle(existing);
    stats.recordPut(pc);
    
    if (assembleCached){
      evictOrExpire();
    }
    return null;
    //return assembleCached? assembleCachedPC(existing, false):null;
  }
  private void evictOrExpire() {
	final int time = time(); 
    final EvictionInfo evictionInfo = evictionDone.get();
    if (time - evictionInfo.evictedAt > MAX_EVICTION ){
      int delta = table.size() - maxElements;
      if (delta > 0){
    	performEviction(delta);
    	evictionInfo.expiredAt = time();
    	return;
      }
    } 
    if (time - evictionInfo.expiredAt > MAX_EXPIRATION ){
      performExpiration();
      evictionInfo.expiredAt = time();
    }
  }

  private void performEviction(int delta) {
	//expire 1/2048 at a time or at least 8
	final long statsTime = stats.time(); 
	int entries = Math.max(delta, Math.max(8, maxElements>>>11));
	Collection<Object> keys = table.getExpirable(entries,newEvictionComparator());
	for (Object key:keys){
	  evictImpl(key);
	}
	stats.recordEviction(stats.time() - statsTime, keys.size());
  }
  
  private void performExpiration() {
	final long statsTime = stats.time();
	final int entries = Math.max(16, maxElements>>>11);
	final int time = time();
	
	final Collection<Object> keys = table.getExpirable(entries, newExpirationComparator(time));
	for (Object key:keys){
	   Object v = table.get(key);
	   if (!(v instanceof Object[]))
		 continue;
	   if (!isExpired((Object[]) v, time))
		 break;
	   
	   evictImpl(key);
	}
	stats.recordExpiration(stats.time() - statsTime, keys.size());
  }
  
  private Object[] toArray(CachedPC<?> pc) {
    if (pc instanceof CachedX<?>){
      return ((CachedX<?>) pc).getArray();
    }
    
    final ClassMeta meta = getMeta(pc.getObjectClass());
	final boolean[] loaded = pc.getLoadedFields();
    final int length = Math.max(meta.length, loaded.length);
    if (length>meta.length){
      addMeta(meta.clazz, meta.newLength(length));
    }
    
    final Object[] fields = ArrayUtil.newArray(pc, length, allocator, time());
    for (int i=0; i<loaded.length; i++){
      if (!loaded[i])
      	continue;
      
      fields[i] = pc.getFieldValue(i);
    }
    
    //process interns
    for (InternEntry e : meta.interns){
      fields[e.field] = e.map.intern(fields[e.field]);
    }
    return fields;
  }
  


  
  @Override @SuppressWarnings("rawtypes")
  public void putAll(Map<Object, CachedPC> objs) {
    for(Map.Entry<Object, CachedPC> e : objs.entrySet()){
      putImpl(e.getKey(), e.getValue(), false);
    }
    evictOrExpire();
  }

  @Override
  public boolean isEmpty() {
    return table.isEmpty();
  }

  @Override
  public boolean containsOid(Object oid) {
    return table.get(oid)!=null;
  }
  
////////////////////
  private CachedPC<?> assembleCachedPC(Object object, Object key) {
    if (!(object instanceof Object[]))      
      return null;
    Object[] array = (Object[]) object;
    
    if (array.length < ArrayUtil.RESERVED){
      throwWrongArray(array);//separate method to reduce code size
    }
    

    int len = array.length;

    @SuppressWarnings("unchecked")
	Class<Object> clazz = (Class<Object>) array[--len];
    if (isExpired(array, time())){//the array ref to cache till be in L1 (so here it's the best place to call isExpired)
      evictImpl(key);
      return null;
    }

    Object version = array[--len];
     
    //touch the original array    
    ArrayUtil.incHitCount(array);
    //need to clone in order to use the allocator, cloning would keep the object in the young gen
    //the array is small should go in the TLAB and die trivially within young gen
    //technically using ref counting and finalize can achieve similar effects but it's too expensive (finalize requires FinalReference and a wide global lock)    
    CachedX<Object> result = new CachedX<Object>(clazz, array.clone(), array.length - ArrayUtil.RESERVED, version);        
    return result;
  }

  private static void throwWrongArray(Object[] array) {
	throw new IllegalStateException("Wrong array: "+Arrays.toString(array));
  }
  
  private ClassMeta getMeta(Class<?> clazz) {
	ClassMeta result = metaMap.get().get(clazz);
	if (result!=null){
	  return result;
	}
    AbstractClassMetaData meta = nucleusContext.getMetaDataManager().getMetaDataForClass(clazz, null);      
    InternEntry[] interns = InternEntry.EMPTY;
    if (meta!=null){
      ArrayList<InternEntry> list = resolveInterns(meta);
      if (!list.isEmpty())
    	interns=list.toArray(interns);
    }
    
    int length = meta!=null?meta.getMemberCount():2;
    ClassMeta classMeta = new ClassMeta(clazz, interns, length, resolveCacheable(meta), resolveExpiration(meta));
    return addMeta(clazz, classMeta);    

  }

  private ArrayList<InternEntry> resolveInterns(AbstractClassMetaData meta) {
	ArrayList<InternEntry> list = new ArrayList<>();
	for (AbstractMemberMetaData fieldMeta : meta.getManagedMembers()){
	  String intern = fieldMeta.getValueForExtension(JdoExtensions.INTERN);
	  if (intern==null)
		continue;

	  if ("default".equals(intern))
		intern = fieldMeta.getName();

	  InternMap<Object> map = globalInterns.get(intern);
	  if (map==null){
		InternMap<Object> existing = globalInterns.putIfAbsent(intern, map=new InternMap<>());
		if (existing!=null)
		  map=existing;
	  }
	  list.add(new InternEntry(fieldMeta.getFieldId(), map));
	}
	return list;
  }
  
  int time(){ 
    //we use System.currentTimeMillis/1024 (i.e. >>>10)
    long elapsed = System.currentTimeMillis() - created;
    elapsed >>>= 10;// shift is way faster than div 1000 to get the seconds and it's 2% off, which is ok
    return (int) elapsed;    
  }  
}
