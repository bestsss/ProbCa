package bestsss.cache.test;

import java.math.BigDecimal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import jsr166e.ConcurrentHashMapV8;

import org.junit.Assert;
import org.junit.Test;

import bestsss.cache.Table;
import bestsss.cache.sort.Smoothsort;
/*
 * Written by Stanimir Simeonoff and released as public domain as described at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

 /**
  * @author Stanimir Simeonoff
 */
public class EvictionTest {
  @Test
  public void eviction(){
    //bestsss.cache.ClosedHashTable;

    Table<Integer, Integer> m = new ConcurrentHashMapV8<Integer, Integer>();
    
    int loops = (int)1e6;
//    java.util.Random r =new java.util.Random(119);
    int count = 0;
    for (int i=0;i<loops; i++){
      Integer v = i;
      if (m.put(v,v)==null){
        count++;
      }
    }
    final int max = loops;
    int expired=0;
    List<Integer> overThreshold=new ArrayList<>();
    List<Integer> all=new ArrayList<Integer>();
    long nanos = -System.nanoTime();
    for (int i=0;i<17;i++){
      List<Integer> c = m.getExpirable(16, Smoothsort.<Integer>naturalOrder());
      all.addAll(c);
      checkExp(c, max-expired, overThreshold);
      for (Integer n : c){
        if (m.remove(n)!=null){
          expired++;
        }
      }      
    }
    nanos+=System.nanoTime();
    
    int expected = count - expired;
    Collections.sort(overThreshold);
    Collections.sort(all);
    System.out.println("Done in "+BigDecimal.valueOf(nanos, 6));
    System.out.println(overThreshold);
    System.out.println("All: "+all);
    Assert.assertEquals(expected, m.size());
    Assert.assertTrue( overThreshold.size()<expired/3);
  }

  private void  checkExp(List<Integer> expirable, int max, List<Integer> over) {
    int threshold = (int) (0.2*max);
    for (Integer n : expirable){
      if (n>threshold){
        over.add(n);        
      }
    }    
  }
}
