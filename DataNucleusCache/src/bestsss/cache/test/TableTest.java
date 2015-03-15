package bestsss.cache.test;


import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Random;

import org.junit.Assert;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import bestsss.cache.ClosedHashTable;
/**
 * Written by Stanimir Simeonoff and released as public domain as described at
 * http://creativecommons.org/publicdomain/zero/1.0/
 * @author Stanimir Simeonoff
 */
public class TableTest {
  private static final Long[] keys = new Long[1<<20];
  @BeforeClass public static void init(){
    Random r = new Random(19);
    for (int i = 0; i < keys.length; i++) {
      keys[i] = r.nextLong();
    }
  }
  long baseline;
  @Before
  public void memNothingTest(){
    baseline = printUserMemory("", 0);
  }
  @Test
  public void memTest(){
    System.gc();
    ClosedHashTable<Long, Long> table = new ClosedHashTable<>();
    for (Long key: keys){
      table.put(key, key);
    }
    Assert.assertEquals(table.size(), keys.length);
    printUserMemory(table, baseline); 
    
    for (Long key: keys){
      Assert.assertEquals(key, table.get(key));
    }
    testDelete(table);
  }
  
  private void testDelete(ClosedHashTable<Long, Long> table) {
//    Random r = new Random(19);
    Long[] k = keys.clone();
    int deletions = k.length/3*2;
    for (int i = 0; i < deletions; i++) {
      Assert.assertEquals(k[i], table.put(k[i], null));
      k[i] = null;
    }
    System.out.printf("Tombstones: %d, deletions: %d, factor: %f", table.tombstones(), deletions, table.tombstones()/(double)deletions);
    
    Assert.assertEquals(k[deletions], table.put(k[deletions++], null));
    
    for (int i = deletions; i < k.length; i++) {
      Long key = k[i];
      Assert.assertEquals(key, table.get(key));
    }
  }
  
  @Test
  public void memHashMapTest(){
    System.gc();
    HashMap<Long, Long> table = new HashMap<>();
    for (Long key: keys){
      table.put(key, key);
    }
    printUserMemory(table, baseline);      
  }
  
  private static long printUserMemory(Object ref, long baseline) {
    long result;
    System.gc();
    System.out.println(result=ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
    System.out.println("Used: "+(result-baseline));
    System.out.println(ref.getClass() +"   "+ System.identityHashCode(ref));
    System.out.println("================");
    return result;
  }
}
