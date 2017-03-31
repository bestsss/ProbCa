package bestsss.cache;

import java.lang.reflect.Field;
import java.util.Arrays;

import org.datanucleus.cache.CachedPC;

/*
 * Written by Stanimir Simeonoff and released as public domain as described at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

/**
 * Replacement of the vanilla org.datanucleus.cache.CachedPC.
 * The class doesn't rely on a HashMap that's very memory intense as it requires a HashMap.Entry per each loaded field. 
 * Instead it utilizes a plain Object[] that serves as a replacement of the "boolean[] loadedFields".
 * Unless there are very few loaded fields the class is a clear win, also the access to the Object[] offers a lot better spacial caching properties.
 * 
 * @author Stanimir Simeonoff
 */
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

  private void zapSuperLoadedFields() {//this object is supposed to have short life span, so zapping the field is likely superflous here
    if (loadFieldsSetter!=null){//remove the useless empty boolean array
      try{
        loadFieldsSetter.set(this, EMPTY);
      }catch (Exception _ex) {//skip
      }
    }
  }


  @Override
  public Object getFieldValue(Integer fieldNumber) {
    int f = fieldNumber;
    if (f<0 || f>=length){
      return null;
    }
    return unwrap(allFields[f]);
    
  }

  private static Object unwrap(Object value) {
    if (NOT_PRESENT==value || null==value){
      return null;
    }
    if (value.getClass()==MapReplacement.class){
      return ((MapReplacement)value).toMap();
    }
      
    return value;
  }

  @Override
  public boolean[] getLoadedFields() {
    boolean[] result = new boolean[Math.min(length, allFields.length)];//reduce the lattice, remove bound checks
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

   private static int[] getFlagsSetTo(Object[] fields, int length){
     length = Math.min(fields.length, length);
     final int[] result = new int[length];
     int j = 0;
     for (int i = 0; i < result.length; i++){
       if (fields[i]!=NOT_PRESENT){
         result[j++] = i;
       }
     }
     if (j==result.length)
       return result;

     if (j==0)
       return null;//returning null is bad, yet mimic ClassUtil.getFlagsSetTo

     return Arrays.copyOf(result, j);
   }

   @Override
   public int[] getLoadedFieldNumbers() {
     return getFlagsSetTo(allFields, this.length);
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
    //ignore fields above length
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
