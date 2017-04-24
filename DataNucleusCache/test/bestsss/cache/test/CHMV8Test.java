package bestsss.cache.test;

/*
 * Written by Stanimir Simeonoff and released as public domain as described at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
* @author Stanimir Simeonoff
*/
import org.junit.Test;
import org.junit.Assert;
import jsr166e.ConcurrentHashMapV8;


public class CHMV8Test {
  
  @Test
  public void nullPut(){
    ConcurrentHashMapV8<Long, String> map = new ConcurrentHashMapV8<>();
    map.put(100L, "xxx");
    Assert.assertEquals(map.get(100L), "xxx");
    Assert.assertEquals(map.put(100L, null), "xxx");
    Assert.assertEquals(map.get(100L), null);
  }
   
}
