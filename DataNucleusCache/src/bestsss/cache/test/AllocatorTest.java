package bestsss.cache.test;

import java.util.Collections;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import bestsss.cache.Allocator;
import bestsss.cache.CachedX;

public class AllocatorTest {
  public void testPoll(Allocator allocator, int len){          
    Object[] o;
    Assert.assertTrue((o=allocator.get(len)).length>=len);
    Assert.assertTrue(allocator.offer(o));
    Assert.assertSame(o, allocator.get(len));
    for(int i=0;i<o.length;i++){
      Assert.assertSame(o[i], CachedX.NOT_PRESENT);
    }
  }

  @Test
  public void test(){
    Allocator allocator = new Allocator(32);
    int[] len = new int[252];
    for (int i=0;i<len.length;i++)
      len[i]=i+1;
    shuffle(len);
    
    testPoll(allocator, 4);
    testPoll(allocator, 1);
    testPoll(allocator, 13);
    for (int i=0;i<len.length;i++)
      testPoll(allocator, len[i]);
    
  }

  private static void shuffle(int[] len) {
    Random rnd=new Random();
    for (int i=len.length; i>1; i--)
      swap(len, i-1, rnd.nextInt(i));
  }


  private static void swap(int[] x, int a, int b) {
    int t = x[a];
    x[a] = x[b];
    x[b] = t;
  }
}
