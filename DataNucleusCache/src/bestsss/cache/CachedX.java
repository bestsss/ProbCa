package bestsss.cache;

import java.lang.reflect.Field;
import java.util.Arrays;

import org.datanucleus.cache.CachedPC;

public class CachedX<T> extends CachedPC<T>{
  private static final long serialVersionUID = 0L;
  
  public static final Object NOT_PRESENT=Consts.NOT_PRESENT;//Using a designated value for lack of presence as null is a valid loaded value
  private static final boolean[] EMPTY={};
  
  static enum Consts{
    NOT_PRESENT;
  }

  
  private static final Field loadFieldsSetter=resolveloadedFields();

  private static Field resolveloadedFields() {
    try {
      Field f = CachedPC.class.getDeclaredField("loadedFields");
      f.setAccessible(true);
      return f;    
    } catch (Exception _ex) {
      _ex.printStackTrace();
      return null;
    }
  } 
  private int length;
  private Object[] allFields;
   
  public CachedX(Class<T> cls, Object[] allFields, int length, Object vers) {
    super(cls, EMPTY, vers);
    this.length = length;
    this.allFields = allFields;
    zapSuperLoadedFields();
  }

  Object[] getArray(){
    return allFields;
  }

  private void zapSuperLoadedFields() {
    if (loadFieldsSetter!=null){//remove the useless empty boolean array
      try{
        loadFieldsSetter.set(this, EMPTY);
      }catch (Exception _ex) {//skip
      }
    }
  }


  @Override
  public Object getFieldValue(Integer fieldNumber) {
//	System.out.println(getObjectClass()+ " : "+fieldNumber);
    int f = fieldNumber;
    if (f<0 || f>=length){
      return null;
    }
    return unmaskPresence(allFields[f]);
    
  }

  private static Object unmaskPresence(Object value) {
    return value!=NOT_PRESENT?value:null;
  }

  @Override
  public boolean[] getLoadedFields() {
    boolean[] result = new boolean[length];
    for (int i=0;i<result.length;i++){
      result[i] = allFields[i]!=NOT_PRESENT;
    }
    return result;
  }

  
  @Override
  public void setFieldValue(Integer fieldNumber, Object value) {
    int f = fieldNumber;
    set(f, value);
  }

   void set(int f, Object value) {
    if (f<0)
      throw new IndexOutOfBoundsException();
    
    if (f>length){
      if (f>=ArrayUtil.maxLength(allFields)){//extend if doesn't fit the current array
        allFields = ArrayUtil.extend(allFields, length, f);
      }
      length=f+1;
    }
    allFields[f] = value;
  }

  static int[] getFlagsSetTo(Object[] flags, boolean state){
      int[] temp = new int[flags.length];
      int j = 0;
      for (int i = 0; i < flags.length; i++){
          if ((flags[i]!=NOT_PRESENT) == state){
              temp[j++] = i;
          }
      }
      if (j==temp.length)
        return temp;
      
      if (j==0)
        return null;
      
      return Arrays.copyOf(temp, j);
  }
  
  @Override
  public int[] getLoadedFieldNumbers() {
    return getFlagsSetTo(allFields, true);
  }
  

  @Override
  public void setLoadedField(int fieldNumber, boolean loaded) {
    if (loaded)//don't deal with loaded, the mechanism to deal with craptastic loaded map is just a bad design
      return;
    
    //consider not loaded as deleted
    int f = fieldNumber;
    if (f<0)
      throw new IndexOutOfBoundsException();
    
    if (f<length){
      allFields[f] = NOT_PRESENT;
    }
    //ignore fields about length
  }

  
  @Override
  public CachedX<T> getCopy() {
    Object[] copy = allFields.clone();
    for (int i = 0; i < copy.length; i++) {
      Object v = copy[i];
      if (v instanceof CachedPC<?>){
        v = ((CachedPC<?>) v).getCopy();
        copy[i]=v;
      }
    }
    CachedX<T> result = new CachedX<T>(getObjectClass(), copy, length, getVersion());   
    return result;
  }

  int getLength() {
     return length;
  } 
}