package bestsss.cache.sort;

import java.util.Comparator;


/**
 * modified Smoothsort from http://en.wikipedia.org/wiki/Smoothsort
 * to use comparator. Used in favor of the std merge sort to remove the extra allocation elements during sort
 * More Info: http://www.keithschwarz.com/interesting/code/?dir=smoothsort*/
public class Smoothsort {
	private Smoothsort(){
	}
	
	private static enum NaturalOrderComparator implements Comparator<Comparable<Object>> {
		instance;
		@Override
		public int compare(Comparable<Object> o1, Comparable<Object> o2) {
			return o1.compareTo(o2);
		}
	}
	
	@SuppressWarnings("unchecked")
	public static <T> Comparator<T> naturalOrder() {
    return (Comparator<T>) NaturalOrderComparator.instance;
	}
  // by keeping these constants, we can avoid the tiresome business
  // of keeping track of Dijkstra's b and c. Instead of keeping
  // b and c, I will keep an index into this array.
 
  static final int LP[] = { 1, 1, 3, 5, 9, 15, 25, 41, 67, 109,
      177, 287, 465, 753, 1219, 1973, 3193, 5167, 8361, 13529, 21891,
      35421, 57313, 92735, 150049, 242785, 392835, 635621, 1028457,
      1664079, 2692537, 4356617, 7049155, 11405773, 18454929, 29860703,
      48315633, 78176337, 126491971, 204668309, 331160281, 535828591,
      866988873 // the next number is > 31 bits.
  };
  public static <T  extends Comparable<T>> void  sort(T[] m){
  	sort(m, naturalOrder());  
  }
  public static <T> void sort(T[] m, Comparator<? super T> c){ 
  	sort(m, 0, m.length, c);
  }
  public static <T> void sort(T[] m, int fromIndex, int toIndex, Comparator<? super T> c) {
  	toIndex--;//reduce  to comply w/ the impl 
    int head = fromIndex; // the offset of the first element of the prefix into m
 
    // These variables need a little explaining. If our string of heaps
    // is of length 38, then the heaps will be of size 25+9+3+1, which are
    // Leonardo numbers 6, 4, 2, 1. 
    // Turning this into a binary number, we get b01010110 = 0x56. We represent
    // this number as a pair of numbers by right-shifting all the zeros and 
    // storing the mantissa and exponent as "p" and "pshift".
    // This is handy, because the exponent is the index into L[] giving the
    // size of the rightmost heap, and because we can instantly find out if
    // the rightmost two heaps are consecutive Leonardo numbers by checking
    // (p&3)==3
 
    int p = 1; // the bitmap of the current standard concatenation >> pshift
    int pshift = 1;
 
    while (head < toIndex) {
      if ((p & 3) == 3) {
        // Add 1 by merging the first two blocks into a larger one.
        // The next Leonardo number is one bigger.
        sift(m, pshift, head, c);
        p >>>= 2;
        pshift += 2;
      } else {
        // adding a new block of length 1
        if (LP[pshift - 1] >= toIndex - head) {
          // this block is its final size.
          trinkle(m, p, pshift, head, false, c);
        } else {
          // this block will get merged. Just make it trusty.
          sift(m, pshift, head, c);
        }
 
        if (pshift == 1) {
          // LP[1] is being used, so we add use LP[0]
          p <<= 1;
          pshift--;
        } else {
          // shift out to position 1, add LP[1]
          p <<= (pshift - 1);
          pshift = 1;
        }
      }
      p |= 1;
      head++;
    }
 
    trinkle(m, p, pshift, head, false, c);
 
    while (pshift != 1 || p != 1) {
      if (pshift <= 1) {
        // block of length 1. No fiddling needed
        int trail = Integer.numberOfTrailingZeros(p & ~1);
        p >>>= trail;
        pshift += trail;
      } else {
        p <<= 2;
        p ^= 7;
        pshift -= 2;
 
        // This block gets broken into three bits. The rightmost
        // bit is a block of length 1. The left hand part is split into
        // two, a block of length LP[pshift+1] and one of LP[pshift].
        // Both these two are appropriately heapified, but the root
        // nodes are not necessarily in order. We therefore semitrinkle
        // both of them
 
        trinkle(m, p >>> 1, pshift + 1, head - LP[pshift] - 1, true, c);
        trinkle(m, p, pshift, head - 1, true, c);
      }
 
      head--;
    }
  }
 
  private static <C> void sift(C[] m, int pshift,
      int head, Comparator<? super C> c) {
    // we do not use Floyd's improvements to the heapsort sift, because we
    // are not doing what heapsort does - always moving nodes from near
    // the bottom of the tree to the root.
 
    C val = m[head];
 
    while (pshift > 1) {
      int rt = head - 1;
      int lf = head - 1 - LP[pshift - 2];
 
      if (c.compare(val, m[lf]) >= 0 && c.compare(val, m[rt]) >= 0)
        break;
      if (c.compare( m[lf], m[rt]) >= 0) {
        m[head] = m[lf];
        head = lf;
        pshift -= 1;
      } else {
        m[head] = m[rt];
        head = rt;
        pshift -= 2;
      }
    }
 
    m[head] = val;
  }
 
  private static <C> void trinkle(C[] m, int p,
      int pshift, int head, boolean isTrusty, Comparator<? super C> c) {
 
    C val = m[head];
 
    while (p != 1) {
      final int stepson = head - LP[pshift];
 
      final C stepsonValue = m[stepson];
			if (c.compare(stepsonValue, val) <= 0)
        break; // current node is greater than head. Sift.
 
      // no need to check this if we know the current node is trusty,
      // because we just checked the head (which is val, in the first
      // iteration)
      if (!isTrusty && pshift > 1) {
        int rt = head - 1;
        int lf = head - 1 - LP[pshift - 2];
        if (c.compare(m[rt], stepsonValue) >= 0
            || c.compare(m[lf], stepsonValue) >= 0)
          break;
      }
 
      m[head] = stepsonValue;
 
      head = stepson;
      int trail = Integer.numberOfTrailingZeros(p & ~1);
      p >>>= trail;
      pshift += trail;
      isTrusty = false;
    }
 
    if (!isTrusty) {
      m[head] = val;
      sift(m, pshift, head, c);
    }
  }
  /*
  public static void main(String[] args) {
  	String[] words = "This example demonstrate how you can run a program as a specified user (uid) and with a specified group (gid). Many daemon programs will do the uid and gid switch by them self, but for those programs that does not (e.g. Java programs), monit's ability to start a program as a certain user can be very useful. In this example we start the Tomcat Java Servlet Engine as the standard nobody user and group. Please note that Monit will only switch uid and gid for a program if the super-user is running monit, otherwise Monit will simply ignore the request to change uid and gid.".split(" ");
  	sort(words, String.CASE_INSENSITIVE_ORDER);
  	for(String w:words) System.out.println(w);
  	java.util.Random r = new java.util.Random (129);
  	Long[] longs = new Long[331]; for (int i=0;i<longs.length;i++) longs[i]= r.nextLong();
  	sort(longs); for(Long l : longs)System.out.println(l);  	
  }
  */
}
