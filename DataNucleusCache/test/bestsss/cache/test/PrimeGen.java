package bestsss.cache.test;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Random;

public class PrimeGen {
  public static void main(String[] args) {
    Random r = new SecureRandom();
    for (int i=0;i<64;i++){
     System.out.print("0x"+Integer.toHexString(BigInteger.probablePrime(32, r).intValue())+", "); 
    }
  }
}
