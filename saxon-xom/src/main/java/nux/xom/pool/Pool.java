/*
 * Copyright (c) 2005, The Regents of the University of California, through
 * Lawrence Berkeley National Laboratory (subject to receipt of any required
 * approvals from the U.S. Dept. of Energy). All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * (1) Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 
 * (2) Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * 
 * (3) Neither the name of the University of California, Lawrence Berkeley
 * National Laboratory, U.S. Dept. of Energy nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * You are under no obligation whatsoever to provide any bug fixes, patches, or
 * upgrades to the features, functionality or performance of the source code
 * ("Enhancements") to anyone; however, if you choose to make your Enhancements
 * available either publicly, or directly to Lawrence Berkeley National
 * Laboratory, without imposing a separate written license agreement for such
 * Enhancements, then you hereby grant the following license: a non-exclusive,
 * royalty-free perpetual license to install, use, modify, prepare derivative
 * works, incorporate into other computer software, distribute, and sublicense
 * such enhancements or derivative works thereof, in binary and source code
 * form.
 */
package nux.xom.pool;

import java.io.File;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import nu.xom.Node;

/**
 * Thread-safe memory sensitive cache/pool using a LRUHashMap; maintains a
 * {@link SoftReference} for each map value; Automatically and safely evicts
 * stale entries of values that have been garbage collected by Java's soft
 * reference mechanism.
 * <p>
 * Null keys are permitted. Null values are permitted but their mappings are
 * silently removed (a cache with null values is rather meaningless).
 * <p>
 * For a discussion of atomic updates, visibility and ordering in the Java
 * memory model see http://gee.cs.oswego.edu/dl/cpj/jmm.html
 * 
 * @see nux.xom.sandbox.DocumentMapTest
 * 
 * @author whoschek.AT.lbl.DOT.gov
 * @author $Author: hoschek3 $
 * @version $Revision: 1.31 $, $Date: 2006/05/04 06:03:52 $
 */
final class Pool implements Map { // not a public class!

	/** the underlying map holding the real entries */
	private final SoftLRUHashMap child;
//	private final Map child;

	/** gc notifies us of soft ref collections by enqueuing onto this queue */
	private final ReferenceQueue queue;
	
	/**
	 * the current amount of memory [bytes] this map occupies
	 */
	private long totalSize;

	private final long maxIdleTime; // copied from config
	private final long maxLifeTime; // copied from config
	private final long capacity;    // copied from config
	private final boolean fileMonitoring; // copied from config

	/**
	 * daemon thread running periodically to evict invalid entries.
	 */
	private static final Timer SWEEPER = new Timer(true);

	/** enable Nux debug output on System.err? */
	private static final boolean DEBUG = 
		XOMUtil.getSystemProperty("nux.xom.pool.Pool.debug", false);

	
	/** Returns a wrapper around an array to be used as a key in a HashMap */
	static Object createHashKeys(Object[] keys) {
		return new HashKeys(keys);
	}
	
	/**
	 * Constructs a new instance with the given parameters.
	 * 
	 * @param config
	 *            the configuration to use
	 */
	static Map createPool(PoolConfig config) {
		if (config == null) 
			throw new IllegalArgumentException("config must not be null");
		
		int maxEntries = config.getMaxEntries();
		if (config.getCapacity() <= 0 || config.getMaxIdleTime() <= 0 || config.getMaxLifeTime() <= 0) {
			maxEntries = 0; // avoid unnecessary overhead
		}
		if (maxEntries > 0)
			return new Pool(config);
		else
			return Collections.synchronizedMap(new LRUHashMap(Math.abs(maxEntries)));
	}
	
	private Pool(PoolConfig config) {		
		this.totalSize = 0;
		this.child = new SoftLRUHashMap(this, config.getMaxEntries());
		this.capacity = config.getCapacity();
		this.fileMonitoring = config.getFileMonitoring();

		// fixup terribly inefficient parameters
		this.maxIdleTime = Math.max(100, config.getMaxIdleTime());
		this.maxLifeTime = Math.max(100, config.getMaxLifeTime());
		long t = Math.min(maxIdleTime, maxLifeTime);

		t = Math.min(config.getInvalidationPeriod(), t);
		if (t == Long.MAX_VALUE) t = -1; // never evict
		if (t >= 0) t = Math.max(100, t); // fixup inefficient parameters
		this.queue = t > 0 ? new ReferenceQueue() : null;
		if (t > 0) SWEEPER.schedule(new SweepTask(this), t, t);
	}

	public synchronized void clear() {
		evictStaleEntries();
		child.clear();
		totalSize = 0;
	}

	public synchronized Object get(Object key) {
		evictStaleEntries();
		SoftValue ref = (SoftValue) child.get(key);
		return SoftValue.unwrap(ref, false);
	}

	public Object put(Object key, Object value) {
		synchronized (this) {
			evictStaleEntries();
		}
		
		int size = 0;
		if (value != null && capacity != Long.MAX_VALUE) {
			int valueSize = getMemorySize(value); // need not hold lock (potentially expensive)
			int keySize = getMemorySize(key);
			size += valueSize + keySize;
//			if (DEBUG) System.err.println("vsize=" + valueSize + ", ksize=" + keySize + ", MB=" + (totalSize / (1024.0f * 1024.0f)));
			if (size > capacity) value = null; // i.e. remove entry, if any
		}
		
		synchronized (this) {
			SoftValue old;
			if (value != null) {
				SoftValue ref = new SoftValue(key, value, queue, size);
				old = (SoftValue) child.put(key, ref);
				totalSize += size;
			} else {
				old = (SoftValue) child.remove(key);
			}
	
			Object result = SoftValue.unwrap(old, true);
			if (old != null) evictEntry(key, old, "PUT");
			if (value != null && totalSize > capacity) evictExcessMemoryEntries();
			return result;
		}
	}

	public Object remove(Object key) {
		return put(key, null);
	}

	// methods that are not really needed for our purposes:
	public synchronized boolean containsKey(Object key) { return get(key) != null; }
	public synchronized int size() { return child.size(); }
	public synchronized boolean isEmpty() { return child.isEmpty(); }
	public synchronized Set keySet() { return child.keySet(); }
	public void putAll(Map src) {
		Iterator iter = src.entrySet().iterator();
		while (iter.hasNext()) {
			Entry entry = (Entry) iter.next();
			put(entry.getKey(), entry.getValue());
		}
	}

	// methods that could be implemented; so far not needed at all for our purposes:
	public boolean containsValue(Object value) { throw new UnsupportedOperationException(); }
	public Collection values() { throw new UnsupportedOperationException(); }
	public Set entrySet() { throw new UnsupportedOperationException(); }

	/** mark entry as removed; the actual removal is done elsewhere */
	private void evictEntry(Object key, SoftValue ref, String msg) {
		ref.remove();
		totalSize -= ref.size;
		if (DEBUG) {
			String str = String.valueOf(key);
			if (str.length() > 35) str = str.substring(0, 32) + "...";
			System.err.println("*******" + msg + " EVICTED=" + str
					+ ", size()=" + child.size() + ", MB=" + (totalSize / (1024.0f * 1024.0f)));
		}
//		if (key instanceof PoolValidatingKey) { // TODO: consider adding feature?
//			((PoolValidatingKey) key).onRemoval(); // notify for potential dependency chain invalidation
//		}
	}
	
	/** removes all entries that have been collected and enqueued by the VM gc */
	private void evictStaleEntries() {
		if (queue == null) return; // nothing to do
		SoftValue ref;
		while ((ref = (SoftValue) queue.poll()) != null) {
			if (!ref.isRemoved()) {
				child.remove(ref.key);
				evictEntry(ref.key, ref, "GC");
			}
		}
	}

	/** removes all entries that turn out to be nomore valid (to be run periodically) */
	private void evictInvalidEntries() {
		Iterator iter = child.entrySet().iterator();
		long now = System.currentTimeMillis();
		long idle = maxIdleTime - now;
		long life = maxLifeTime - now;
		
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			Object key = entry.getKey();
			SoftValue ref = (SoftValue) entry.getValue();
			String msg = null;
			
			boolean isValid = ref.lastAccessTime + idle > 0;
			if (!isValid) msg = "INVALID (maxIdleTime)";
			
			if (isValid) {
				isValid = ref.insertionTime + life > 0;
				if (!isValid) msg = "INVALID (maxLifeTime)";
			}
			
			if (isValid && key instanceof PoolValidatingKey) {
				isValid = ((PoolValidatingKey) key).isValid();
				if (!isValid) msg = "INVALID (PoolValidatingKey)";
			}
			
			if (isValid && fileMonitoring) {
				File file = null;
				if (key instanceof File) { 
					file = (File) key;
				} else if (key instanceof HashKeys) {
					file = ((HashKeys) key).getFile();
				}
				
				if (file != null) { // invalidate entry if it's file has changed
					long lastModified = file.lastModified();
					isValid = lastModified != 0 && lastModified <= ref.insertionTime;
					if (!isValid) msg = "INVALID (fileChange)";
				}
			}
			
			if (!isValid) {
				iter.remove();
				evictEntry(key, ref, msg);
			}			
		}		
	}

	/** removes LRU entries until the max memory limit invariant holds again */
	private void evictExcessMemoryEntries() {		
		Iterator iter = child.entrySet().iterator();
		while (totalSize > capacity && iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			iter.remove();
			evictEntry(entry.getKey(), (SoftValue) entry.getValue(), "MEMORY");
		}
	}

	/** calculates approximate memory consumption of the given value object */
	private static int getMemorySize(Object value) {
		if (value == null)
			return 0;
		if (value instanceof byte[]) 
			return ((byte[]) value).length;
		if (value instanceof Node) 
			return XOMUtil.getMemorySize((Node) value);
		if (value instanceof CharSequence) 
			return 2 * ((CharSequence) value).length();
		
		if (value instanceof HashKeys) {
			value = ((HashKeys) value).keys;
		}
		else if (value instanceof Collection) {
			value = ((Collection) value).toArray();
		}
		
		if (value instanceof Object[]) {
			Object[] arr = (Object[]) value;
			int size = 12 + 4 + 4 * arr.length;
			for (int i=arr.length; --i >= 0; ) {
				size += getMemorySize(arr[i]); // assumes no graph cycles
			}
			return size;
		}		
		return 0;
	}
	

	///////////////////////////////////////////////////////////////////////////////
	// Nested classes:
	///////////////////////////////////////////////////////////////////////////////

	/**
	 * Bounded LinkedHashMap with least-recently-used (LRU) eviction policy.
	 */
	private static class LRUHashMap extends LinkedHashMap {
		
		private final int maxSize;

		private LRUHashMap(int maxSize) {
			super(1, 0.75f, true);
			this.maxSize = maxSize;
		}
		
		protected boolean removeEldestEntry(Map.Entry eldest) {
			return size() > maxSize;
		}
			
	}	
	
	/**
	 * Notifies the Pool of removals that occur as a result of exceeding the
	 * maxEntries limit. This helps to accurately keep track of the current
	 * total memory size occupied by the entries.
	 */
	private static final class SoftLRUHashMap extends LRUHashMap {
		
		private final Pool pool;
		
		private SoftLRUHashMap(Pool pool, int maxEntries) {
			super(maxEntries);
			this.pool = pool;
		}
		
		protected boolean removeEldestEntry(Map.Entry eldest) {
			if (super.removeEldestEntry(eldest)) {
				remove(eldest.getKey());
				pool.evictEntry(eldest.getKey(), (SoftValue) eldest.getValue(), "MAXENTRIES");
			}
			return false;
		}
	}
	
	/**
	 * A SoftReference that remembers the entry's key for safe and efficient
	 * removal on evictStaleEntries()
	 */
	private static final class SoftValue extends SoftReference {

		private Object key; // hard ref to the key this value is associated with
		private final long insertionTime; // timestamp at put()
		private long lastAccessTime; // timestamp on get()
		private final int size; // memory consumed by the value [bytes]
		private static final Object REMOVED = new Object(); // marker

		private SoftValue(Object key, Object value, ReferenceQueue queue, int size) {
			super(value, queue);
			this.key = key;
			this.insertionTime = System.currentTimeMillis();
			this.lastAccessTime = this.insertionTime;
			this.size = size;
		}

		private static Object unwrap(SoftValue ref, boolean remove) {
			if (ref == null) return null;
			Object value = ref.get();
			if (remove) {
				ref.remove();
			} else {
				ref.lastAccessTime = System.currentTimeMillis();
			}
			return value;
		}

		private boolean isRemoved() {
			return key == REMOVED;
		}

		private void remove() {
			clear();
			key = REMOVED;
		}

		public String toString() { // for debug only
			return "key=" + key + ", val=" + get();
		}
	}

	/**
	 * Removes stale entries from the given Pool while making sure not to
	 * prevent the Pool itself from being garbage collected.
	 */
	private static final class SweepTask extends TimerTask {

		private final WeakReference poolRef;

		private SweepTask(Pool pool) {
			this.poolRef = new WeakReference(pool);
		}

		public void run() {
			try {
				Pool pool = (Pool) poolRef.get();
				if (pool != null) {
					if (DEBUG) {
						System.err.println("############### Pool.SweepTask running...");
					}
					long now = DEBUG ? System.currentTimeMillis() : 0;
					synchronized (pool) { // must hold lock
						pool.evictStaleEntries();
						pool.evictInvalidEntries();
					}
					if (DEBUG) System.err.println("Pool.SweepTask took ms=" + 
							(System.currentTimeMillis() - now));
				} else {
					cancel(); // pool has been gc'd; remove task from timer
				}
			} catch (Throwable t) { // keep sweeper operational no matter what
				t.printStackTrace();
				if (DEBUG) System.exit(-1); // TODO: remove this when building a release?
			}
		}
	}

	/**
	 * Small efficient helper wrapping an array to be used as a key in a HashMap;
	 * intended for caches/pools. 
	 * <p>
	 * The key array can contain null elements.
	 * Avoids slow iteration for equals() and hashCode()
	 * in AbstractList and hence java.util.Arrays.asList().
	 */
	private static final class HashKeys {

		private final Object[] keys;
		
		private HashKeys(Object[] keys) {
			if (keys == null) throw new IllegalArgumentException("keys must not be null");
			this.keys = keys;
		}
		
		public final boolean equals(Object other) {
			if (other instanceof HashKeys) { 
				return eq(keys, ((HashKeys) other).keys);
			}
			return false;
		}
		
		public final int hashCode() { // see java.util.Arrays.asList().hashCode()
			int hash = 1;
			Object[] k = keys;
			for (int i = k.length; --i >= 0; ) {
				hash *= 31;
				if (k[i] != null) hash += k[i].hashCode();
			}
			return hash;
		}
		
		private static boolean eq(Object[] k1, Object[] k2) {
			int len = k1.length;
			if (len != k2.length) return false;
			for (int i = 0; i < len; i++) {
				Object x = k1[i];
				Object y = k2[i];
				if (x != y && (x == null || y == null || !x.equals(y))) {
					return false;
				}
			}
			return true;			
		}
		
		/** see evictInvalidEntries() and XQueryPool.getXQuery(File, URI) */ 
		public File getFile() {
			if (keys.length > 0 && keys[0] instanceof File) {
				return (File) keys[0];
			}
			return null;
		}
		
		public String toString() { // for debug only
			return Arrays.asList(keys).toString();
		}

	}
}
