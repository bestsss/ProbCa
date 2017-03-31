package bestsss.cache;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import jsr166e.ConcurrentHashMapV8;

import org.datanucleus.NucleusContext;
import org.datanucleus.cache.CachedPC;
import org.datanucleus.cache.Level2Cache;
import org.datanucleus.identity.DatastoreId;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.SingleFieldId;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
/*
 * Written by Stanimir Simeonoff and released as public domain as described at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */










import org.datanucleus.metadata.MetaData;

import bestsss.cache.CacheStatistics.CacheStatisticsProvider;

/**
 * @author Stanimir Simeonoff
 */
//all PCs are stored as Object[] (similar to JCredo), it's a lot better than the current CachedPC which is very memory unfriendly
public class L2Cache implements Level2Cache, CacheStatisticsProvider{

  private static final long serialVersionUID = 1L;

  private static final int MAX_EVICTION = 7;
  private static final int MAX_EXPIRATION = 23;

  private final long created = System.currentTimeMillis();

  private final Table<Object, Object[]> table = new ConcurrentHashMapV8<>(); 
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
  private final Set<String> datastoreCacheable = Collections.newSetFromMap(new ConcurrentHashMapV8<String, Boolean>());
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
  }

  private static int resolveMaxElements(NucleusContext nucleusContext) {
    int size = nucleusContext.getConfiguration().getIntProperty("bestsss.l2cache.size");
    if (size>0)
      return size;

    size = nucleusContext.getConfiguration().getIntProperty("datanucleus.cache.level2.size");
    if (size>0)
      return size;

    long maxMem = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
    if (maxMem<0)
      return 1<<16;

    return (int) ( maxMem/4678);//around 115k elements at 512MB
  }
  private enum EvictionComparator implements Comparator<Object[]>{
    instance;
    @Override
    public int compare(Object[] o1, Object[] o2) {
      int v1 = calcEvictionValue(o1);       
      int v2 = calcEvictionValue(o2);

      return v1-v2;//lowest values are the ones to be evicted, i.e.higher is better
    }

    private static int calcEvictionValue(Object[] o) {
      int created =  ArrayUtil.getCreationTime(o);
      int hits = ArrayUtil.getHits(o);
      int access = ArrayUtil.getAccessTime(o);
      int value = calcEntryValue(created, access, hits);
      return value;
   }
  }


  private static int calcEntryValue(int created, int accessed, int hits){//the higher the better, each hit gives ~1.25seconds extra time
    return created + (hits*5 >> 2) + accessed*2;//time + 5*hits/4 + access*2
  }

  Comparator<Object[]> newExpirationComparator(final int time){//expired 1st
    return new Comparator<Object[]>() {          
      @Override
      public int compare(Object[] o1, Object[] o2) {
        boolean e1 = isExpired(o1, time);
        boolean e2 = isExpired(o2, time);
        
        return e1==e2?0: (e1?-1:1);
      }   
    };
  }

  boolean isExpired(Object[] o1, int time){//keep it package private, avoid bridge methods
    ClassMeta meta = getMeta(ArrayUtil.getClass(o1));
    int accessed =  ArrayUtil.getAccessTime(o1);
    if (time-accessed > meta.expiration)
      return true;
    
    int created =  ArrayUtil.getCreationTime(o1);    
    return time-created > meta.expiration*4;
  }


  private ClassMeta addMeta(Class<?> clazz, ClassMeta classMeta, Class<?> objectIdClass){
    for(;;){//copy on write
      final IdentityHashMap<Class<?>, ClassMeta> map = metaMap.get();    
      final ClassMeta existing  = map.get(clazz);
      final int existingLength = existing!=null? existing.length : -1;      

      if (existingLength >= classMeta.length){
        return existing;
      }

      IdentityHashMap<Class<?>, ClassMeta> update = new IdentityHashMap<>(map);
      update.put(clazz, classMeta);
      if (objectIdClass!=null){
        if (objectIdClass != getClass()){//getClass() is a special case, where the objectIdClass is unavailable and the step should be skipped
          update.put(objectIdClass, classMeta);
        }
      } else{
        datastoreCacheable.add(clazz.getName());
      }
    
      if (metaMap.compareAndSet(map, update)){
        return classMeta;
      }
    }
  }


  private Class<?> getObjectIdClass(Class<?> pcClass, AbstractClassMetaData meta) {
    final String objectidClass = meta.getObjectidClass();
    if (objectidClass==null){
      return null;
    }
    
    try{
      Class type =  Class.forName(objectidClass, false, pcClass.getClassLoader());
      if (DatastoreId.class.isAssignableFrom(type)){
        return null;
      }
      if (SingleFieldId.class.isAssignableFrom(type)){
        return null;
      }      
      return type;
    }catch(ClassNotFoundException _skip){      
    }
    return null;
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
    if (this.getClass()!=null)//always true
      return;
    
    evictImpl(oid);
  }

  @Override
  public void evictAll() {
    table.clear();
    sharedExpirationIterator.set(null);
  }

  @Override
  public void evictAll(Object[] oids) {
    for (Object key : oids){
      evictImpl(key);
    }
  }
  private boolean evictImpl(Object key) {
    Object removed =table.put(key, null); 
    stats.recordRemoval(removed);
    return removed!=null;
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
    if (pc==null && !isCacheableForGet(oid)){
      return null;
    }
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

  
  @Override
  public CacheStatistics getCacheStatistics() {
    return stats;
  }

  public CacheStatistics getStats(){
    return stats;
  }

  @Override
  public CachedPC put(Object oid, CachedPC pc) {
    putImpl(oid, pc);//the return value is never used, so we can ignore it
    evictOrExpire();
    return null;
  }

  private void putImpl(Object oid, CachedPC<?> pc) {
    if (!getMeta(pc.getObjectClass()).cacheable){
      return;
    }	

    /*Object existing = */
    table.put(oid, toArray(pc));
    stats.recordPut(pc);
  }
  
  private void evictOrExpire() {
    final int time = time(); 
    final EvictionInfo evictionInfo = evictionDone.get();
    if (time - evictionInfo.evictedAt > MAX_EVICTION ){
      int delta = table.size() - maxElements;
      if (delta > 64 || sharedExpirationIterator.get()!=null){
        sharedExpire();
        delta = table.size() - maxElements;
      }
      if (delta > 0){
        performEviction(delta);
        evictionInfo.expiredAt = time() + ThreadLocalRandom.current().nextInt(MAX_EVICTION/2);//randomize
        return;
      }
    } 
    if (time - evictionInfo.expiredAt > MAX_EXPIRATION ){
      performExpiration();
      evictionInfo.expiredAt = time() + ThreadLocalRandom.current().nextInt(MAX_EXPIRATION/3);//random expirations
    }
  }

  private void performEviction(int delta) {
    //expire 1/2048 at a time or at least 8
    final long statsTime = stats.time(); 
    int entries = Math.max(delta, Math.max(8, maxElements>>>11));
    Collection<Object> keys = table.getExpirable(entries, EvictionComparator.instance);
    int evicted = 0;
    for (Object key:keys){
      if (evictImpl(key))
        evicted++;
    }
    stats.recordEviction(stats.time() - statsTime, evicted);    
  }

  private void performExpiration() {
    final int delta = table.size() - maxElements;

    final long statsTime = stats.time();
    final int entries = Math.max(Math.min(128, delta), Math.max(16, maxElements>>>10));
    final int time = time();

    final Collection<Object> keys = table.getExpirable(entries, newExpirationComparator(time));
    int expired = 0;
    for (Object key:keys){
      Object v = table.get(key);
      if (!(v instanceof Object[]))
        continue;
      if (!isExpired((Object[]) v, time))
        break;
      if (evictImpl(key)){
        expired++;
      }
    }
    stats.recordExpiration(stats.time() - statsTime, expired);
    if (delta - expired > 0 || sharedExpirationIterator.get()!=null){//doesn't matter if the size changes, we have reached the cap already
      sharedExpire();
    }
  }
  private static final java.util.Iterator<Object[]> BUSY_ITERATOR = Collections.<Object[]>emptyList().iterator();
  //state:: "null" - no active expiration, BUSY_ITERATOR - some thread is performing shared expiration, valid iterator - free to grab and help
  //--perhaps should use split iterator with removed impl-- (need to have at least 4 CPUs for such operation) 
  private final AtomicReference<java.util.Iterator<Object[]>> sharedExpirationIterator=new AtomicReference<>();
  
  /**
   * @return true if the expiration needs to continue (i.e. sharedExpire is likely to perform more work), false is the expiration has completed
   */
  protected boolean sharedExpire(){//available for calls 
    java.util.Iterator<Object[]> i;
    final AtomicReference<Iterator<Object[]>> sharedExpirationIterator = this.sharedExpirationIterator;
    for(;;){
      i = sharedExpirationIterator.get();
      if (i==BUSY_ITERATOR)
        return false;
      
      java.util.Iterator<Object[]> expect = i;  
      if (i==null)
        i = ((ConcurrentHashMapV8<Object, Object[]>)table).values().iterator();
      
      if (sharedExpirationIterator.compareAndSet(expect, BUSY_ITERATOR)){//Locked state
        break;
      }           
    }    
    long start = stats.time();
    final int time = time();
    int expired = 0;
    for (int loops=table.size()>>4;i.hasNext() && loops-->0;){// 1/16 a time
      Object[] o = i.next();
      if (isExpired(o, time)){
        i.remove();
        expired++;
      }
    }
    boolean result;
    sharedExpirationIterator.compareAndSet(BUSY_ITERATOR, (result=i.hasNext())?i:null);
    stats.recordExpiration(stats.time()-start, expired);
    return result;
  }
  
  private Object[] toArray(CachedPC<?> pc) {
    final ClassMeta meta = getMeta(pc.getObjectClass());
    final Object[] fields;
    
    if (pc instanceof CachedX<?>){//already exists, fix time() and access like it's newly placed
      fields = ((CachedX<?>) pc).getArray().clone();//clone it early, so no changes to CachedX would be reflected straight into the cache
      ArrayUtil.setTimeAndAccess(fields, time());//apply time, zero the hitCount - the object has not been in use in its current state (say changing entity's state to 'deleted', 'removed' should not be kept)
      
      for (int i=0; i<fields.length - ArrayUtil.RESERVED; i++){
        if (fields[i] instanceof Map){
          fields[i] = MapReplacement.replacement((Map<?, ?>) fields[i]);
        }
      }

    } else{
      final boolean[] loaded = pc.getLoadedFields();
      final int length = Math.max(meta.length, loaded.length);
      if (length>meta.length){
        addMeta(meta.clazz, meta.newLength(length), getClass());
      }
  
      fields = ArrayUtil.newArray(pc, length, time());
      for (int i=0; i<loaded.length; i++){
        if (!loaded[i])
          continue;
        Object o = pc.getFieldValue(i);
        if (o instanceof Map){
          o = MapReplacement.replacement((Map<?,?>)o);
        }
        fields[i] = o;
      }
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
      putImpl(e.getKey(), e.getValue());
    }
    evictOrExpire();
  }

  @Override
  public boolean isEmpty() {
    return table.isEmpty();
  }
  
  @Override
  public boolean containsOid(Object oid) {
    return touch(table.get(oid))!=null;
  }


  ////////////////////
  private Object touch(Object object) {
    if (!(object instanceof Object[]))      
      return null;
    Object[] array = (Object[]) object;
    ArrayUtil.setTimeAndHitCount(array, time());
    return object;
  }
  
  private CachedPC<?> assembleCachedPC(Object object, Object key) {
    if (!(object instanceof Object[]))      
      return null;
    
    final int time = time();
    
    Object[] array = (Object[]) object;
    int len = array.length;
    if (len < ArrayUtil.RESERVED){
      throwWrongArray(array);//separate method to reduce code size
    }    
           
    @SuppressWarnings("unchecked")
    Class<Object> clazz = (Class<Object>) array[--len];    
    if (isExpired(array, time)){//the array ref to cache will be in L1 (so here it's the best place to call isExpired)
      evictImpl(key);
      stats.recordObsolete();
      return null;
    }

    Object version = array[--len];

    //touch the original array    
    ArrayUtil.setTimeAndHitCount(array, time);//racy but meh, we can live with some races
    
    //the caller is not expected to modify the contents of the cached instance;
    //cloning would be rather cheap as the the array is small should go in the TLAB and die trivially within young gen
    CachedX<Object> result = new CachedX<Object>(clazz, array, array.length - ArrayUtil.RESERVED, version);        
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
    if (!classMeta.cacheable){
      meta.setCacheable(false);//force metadata not to cache the class any longer, there are no proper read barriers... but it will do
      //overall it hacks a little as the metadata should not be mutable
    }
    return addMeta(clazz, classMeta, getObjectIdClass(clazz, meta));    

  }

  private ArrayList<InternEntry> resolveInterns(AbstractClassMetaData meta) {
    ArrayList<InternEntry> list = new ArrayList<>();
    for (AbstractMemberMetaData fieldMeta : getAllFields(meta)){
      String intern = fieldMeta.getValueForExtension(JdoExtensions.INTERN);
      if (intern==null || fieldMeta.getFieldId()<0)
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
  
  /**
   * @param meta target class metadata to return fields for
   * @return all MemberMetaData for passed ClassMetaData, including the ones from inherited classes
   */
  private static AbstractMemberMetaData[] getAllFields(AbstractClassMetaData meta){
    if (!(meta.getParent() instanceof AbstractClassMetaData)){
      return meta.getManagedMembers();
    }
    ArrayList<AbstractMemberMetaData> list =new ArrayList<AbstractMemberMetaData>(Arrays.asList(meta.getManagedMembers()));
    for (MetaData parent = meta.getParent(); parent instanceof AbstractClassMetaData; parent=parent.getParent()){
      AbstractClassMetaData clazz = (AbstractClassMetaData) parent;
      if (clazz.getNoOfManagedMembers()==0){
        continue;
      }
      list.addAll(0, Arrays.asList(clazz.getManagedMembers()));//parent classes at start
    }
    return list.toArray(new AbstractMemberMetaData[list.size()]);
  }
  int time(){ 
    //we use System.currentTimeMillis/1024 (i.e. >>>10)
    long elapsed = System.currentTimeMillis() - created;
    elapsed >>>= 10;// shift is way faster than div 1000 to get the seconds and it's 2% off, which is ok
    return (int) elapsed;    
  }  

  protected static int millisToTime(long millis){//for subclasses 
    return (int)(millis>>>10);
  }
  
  protected boolean isCacheableForGet(Object oid){
    if (oid==null){
      return false;
    }
    //if the class is not effectively cacheable, don't count the miss in the get operation. counting missing on classes
    //that would never be cached is pointless and destroy the most important stat: hitRatio
    IdentityHashMap<Class<?>, ClassMeta> map = metaMap.get();
    ClassMeta meta  = map.get(oid.getClass());
    if (meta!=null){
      return meta.cacheable;
    }
    
    final String className = IdentityUtils.getTargetClassNameForIdentitySimple(oid);    
    return className!=null && datastoreCacheable.contains(className);     
  }
}
