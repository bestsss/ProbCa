package bestsss.cache.test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import bestsss.cache.InternMap;

/**
 * @copyright Playtech 2014
 * @author Stanimir Simeonoff
 */
public class InternTest {
  static long ops = (long) 1e7;
  static int threads = Runtime.getRuntime().availableProcessors();
  static int maxValue = 512;
  static final int[] spreads = {9, maxValue/8, 9, maxValue/4, 10, maxValue/2, 22, 24, 64, 33, 128, maxValue, 11};
  
  final ExecutorService executor = Executors.newCachedThreadPool();
  final InternMap<Long> map =new InternMap<>();
  long task(long loops){	
	InternMap<Long> map = this.map;
	if (map==null)
	  return -1L;//avoid null traps
	final ThreadLocalRandom r = ThreadLocalRandom.current();

	long hits = 0;
	while (loops-->0){	  
	  for (int n : spreads){
		if (intern(map, r, n)){
		  hits++;
		}
	  }	 	 
	}
	return hits;
  }
  
  private static boolean intern(InternMap<Long> map, ThreadLocalRandom r, int max){
	Long v = new Long(r.nextInt(max));//MUST have new
	return map.intern(v)!=v;
  }
  
  public void go(final long ops,int taskCount) throws Exception{
	
	ArrayList<Future<Long>> tasks = new ArrayList<>();
	for (int i =0;i<taskCount;i++){	  
    	Future<Long> f = executor.submit(new Callable<Long>() {
    	  @Override
    	  public Long call() throws Exception {
    		return task(ops);
    	  }	  
    	});
    	tasks.add(f);
	}
	long time = System.nanoTime(); 
	executor.shutdown();
	executor.awaitTermination(1, TimeUnit.HOURS);
	long hits = 0;
	for (Future<Long> f : tasks){
	  hits+=f.get();
	}
	long elapsed = System.nanoTime() - time;
	long totalCount = ops*spreads.length*tasks.size();
	log("Hit ratio: %.4f, %.1f nanos/ops, elapsed: %.2f",  div(hits, totalCount),  div(elapsed, totalCount), BigDecimal.valueOf(elapsed, 6));
	log("Map: %s", this.map);
  }
  
  private void log(String format, Object... args) {
	System.out.printf(format+"%n", args);
  }
  private static Object div(long x, long y){
	return (double)x/y;
  }

  
  public static void main(String[] args) throws Exception {
	new InternTest().go((long)1e5, 1);//warm up
	new InternTest().go(ops, threads);//real stuff
	new InternTest().go((long)1e7, 1);//real stuff
  }
}
