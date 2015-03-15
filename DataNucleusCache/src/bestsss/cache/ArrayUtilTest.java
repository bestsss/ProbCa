package bestsss.cache;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

/*
 * Written by Stanimir Simeonoff and released as public domain as described at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

 /**
  * @author Stanimir Simeonoff
 */
public class ArrayUtilTest {
  @Test
  public void testWithRandom(){
	Allocator allocator=new Allocator(0);
	
	testRandom(allocator);
	
	//2nd pass
	testRandom(allocator);
  }
  
  @Test
  public void testLength(){
	for (int i=0; i< 2222; i++){
	  Assert.assertFalse( Allocator.length(i)<i);
	}
  }

  private void testRandom(Allocator allocator) {
	Random r = new Random(21);	
	for (int i=0; i<33333; i++){
	  testLength(allocator,r.nextInt(997));
	}
  }

  private void testLength(Allocator allocator, int len) {
	final int time = len & 7;
	Object[] a = newArray(this, len, allocator, time);
	for (int i=0; i<len; i++){
	  a[i]=(long) i;
	}

	Assert.assertEquals(ArrayUtil.getClass(a), getClass());

	Assert.assertEquals(ArrayUtil.getHits(a), 0);
	Assert.assertEquals(ArrayUtil.getTime(a), time);
	for (int i=0; i<len; i++){
	  Assert.assertEquals(a[i], (long) i);;
	}

	allocator.offer(a);
  }

  private Object[] newArray(Object object, int length, Allocator allocator, int time) {
    length += ArrayUtil.RESERVED;
    Object[] fields = allocator.get(length);
    length = fields.length;
    
    fields[length-CLASS] = object.getClass();
    fields[length-VERSION] = this;
    fields[length-HITS] = IntegerProvider.ZERO;
    fields[length-TIME] = IntegerProvider.get(time);    
    return fields;

  }
  private static final int CLASS = 1;
  private static final int VERSION = 2;
  private static final int HITS = 3;
  private static final int TIME = 4;
  static final int RESERVED = TIME;
  
}
