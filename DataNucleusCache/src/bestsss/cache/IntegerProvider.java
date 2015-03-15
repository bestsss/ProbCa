package bestsss.cache;
/*
 * Written by Stanimir Simeonoff and released as public domain as described at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

 /**
  * @author Stanimir Simeonoff
 */
//extends base functionality to provide more ints
//generational, so L2Cache.time() can operate with precached values
public class IntegerProvider {
  private static volatile IntegerProvider current=new IntegerProvider(0);
  
  private final int MIN;
  private final int MAX;
  
  private final Integer[] table;
  private int tableLen;

  private IntegerProvider(int min){
    MIN = min;
    MAX=MIN+(1<<15);    
    table=initTable(MIN,MAX);
    tableLen=table.length;
  }
  
  public synchronized static void next(int max){
    IntegerProvider p = current; 
    if (p.MAX != max){
      return;
    }
    current = new IntegerProvider(p.MAX);    
  }
  

  public final static Integer MINUS_1=get(-1);
  public final static Integer ZERO=get(0);

  public final static Integer MAX_VALUE=get(Integer.MAX_VALUE);


  private static Integer[] initTable(int start, int max){
    Integer[] res=new Integer[max-start];
    for (int i=0;i<res.length;i++) res[i]=Integer.valueOf(start++);
    return res;
  }

  public static Integer get(int i){
    return current.getImpl(i);
  }
  
  public Integer getImpl(int i){
    i-=MIN;
    return i<0 || i>=tableLen?Integer.valueOf(i+MIN):table[i];
  }

  public static Integer get(String s){
    return get(Integer.parseInt(s));
  }
  public static int min(){
    return current.MIN;
  }
}
