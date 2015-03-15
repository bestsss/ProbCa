package bestsss.cache;
/*
 * Written by Stanimir Simeonoff and released as public domain as described at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

 /**
  * @author Stanimir Simeonoff
 */
class FastIntSet {
  private final int[] table;//no rehash

  public FastIntSet(int maxElements){
    this.table = new int[Integer.highestOneBit(Math.max(maxElements, 4) - 1)<<2];//ceil to pow2
  }

  /**
   * @param n, any integer but -1
   * @return true if added to the set, false if already existed
   */
  public boolean add(int n){
    n+=1;//
    for (int idx = idx(n); ; idx=idx==table.length-1?0:idx+1){
      int v = table[idx];
      if (v==n)
        return false;
      
      if (v==0){
        table[idx] = n;
        return true;
      }
    }
  }
  public boolean contains(int n){
    n+=1;
    for (int idx = idx(n); ; idx=idx==table.length-1?0:idx+1){
      int v = table[idx];
      if (v==n)
        return true;
      if (v==0)
        return false;//empty
    }
  }
  private int idx(int n) {
    return n & table.length-1;
  }

}
