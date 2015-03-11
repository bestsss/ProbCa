package bestsss.cache.sort;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class CacheComparator<C> implements Comparator<C>{
	public <E extends C> List<E> leastOf(Iterator<E> elements, int k) {

		if (k == 0 || !elements.hasNext()) {
			return Collections.emptyList();
		} else if (k >= Integer.MAX_VALUE / 2) {
			// k is really large; just do a straightforward sorted-copy-and-sublist
			ArrayList<E> list = newArrayList(elements);
			Collections.sort(list, this);
			if (list.size() > k) {
				list.subList(k, list.size()).clear();
			}
			list.trimToSize();
			return Collections.unmodifiableList(list);
		}

		/*
		 * Our goal is an O(n) algorithm using only one pass and O(k) additional
		 * memory.
		 *
		 * We use the following algorithm: maintain a buffer of size 2*k. Every time
		 * the buffer gets full, find the median and partition around it, keeping
		 * only the lowest k elements.  This requires n/k find-median-and-partition
		 * steps, each of which take O(k) time with a traditional quickselect.
		 *
		 * After sorting the output, the whole algorithm is O(n + k log k). It
		 * degrades gracefully for worst-case input (descending order), performs
		 * competitively or wins outright for randomly ordered input, and doesn't
		 * require the whole collection to fit into memory.
		 */
		int bufferCap = k * 2;
		@SuppressWarnings("unchecked") // we'll only put E's in
		E[] buffer = (E[]) new Object[bufferCap];
		E threshold = elements.next();
		buffer[0] = threshold;
		int bufferSize = 1;
		// threshold is the kth smallest element seen so far.  Once bufferSize >= k,
		// anything larger than threshold can be ignored immediately.

		while (bufferSize < k && elements.hasNext()) {
			E e = elements.next();
			buffer[bufferSize++] = e;
			threshold = max(threshold, e);
		}

		while (elements.hasNext()) {
			E e = elements.next();
			if (compare(e, threshold) >= 0) {
				continue;
			}

			buffer[bufferSize++] = e;
			if (bufferSize == bufferCap) {
				// We apply the quickselect algorithm to partition about the median,
				// and then ignore the last k elements.
				int left = 0;
				int right = bufferCap - 1;

				int minThresholdPosition = 0;
				// The leftmost position at which the greatest of the k lower elements
				// -- the new value of threshold -- might be found.

				while (left < right) {
					int pivotIndex = (left + right + 1) >>> 1;
					int pivotNewIndex = partition(buffer, left, right, pivotIndex);
					if (pivotNewIndex > k) {
						right = pivotNewIndex - 1;
					} else if (pivotNewIndex < k) {
						left = Math.max(pivotNewIndex, left + 1);
						minThresholdPosition = pivotNewIndex;
					} else {
						break;
					}
				}
				bufferSize = k;

				threshold = buffer[minThresholdPosition];
				for (int i = minThresholdPosition + 1; i < bufferSize; i++) {
					threshold = max(threshold, buffer[i]);
				}
			}
		}
		Smoothsort.sort(buffer, 0, bufferSize, this);

		bufferSize = Math.min(bufferSize, k);
		return Arrays.asList(buffer).subList(0, bufferSize);
	}
	private <E extends C> int partition(E[] values, int left, int right, int pivotIndex) {
		E pivotValue = values[pivotIndex];

		values[pivotIndex] = values[right];
		values[right] = pivotValue;

		int storeIndex = left;
		for (int i = left; i < right; i++) {
			if (compare(values[i], pivotValue) < 0) {
				swap(values, storeIndex, i);
				storeIndex++;
			}
		}
		swap(values, right, storeIndex);
		return storeIndex;
	}
	public static void swap(Object[] a, int x, int y){
		Object t =a[x];
		a[x]=a[y];
		a[y]=t;
	}

	@Override 	@SuppressWarnings("unchecked")
	public int compare(C o1, C o2) {
		return ((Comparable<C>)o1).compareTo(o2);
	}

	public <E extends C>  E max(E x, E y){
		return compare(x,y)<=0?x:y;
	}

	private static <E> ArrayList<E> newArrayList(Iterator<E> i){
		ArrayList<E> result = new ArrayList<E>();
		for(;i.hasNext();) result.add(i.next());
		return result;
	}
}
