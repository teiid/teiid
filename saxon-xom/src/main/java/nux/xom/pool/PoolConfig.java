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

/**
 * Set of tunable configuration parameters for {@link DocumentMap} and
 * cousins.
 * <p>
 * Setters return <code>this</code> for convenient method chaining.
 * 
 * @author whoschek.AT.lbl.DOT.gov
 * @author $Author: hoschek3 $
 * @version $Revision: 1.12 $, $Date: 2005/12/10 01:05:49 $
 */
public class PoolConfig {

	private int compressionLevel = DEFAULT_COMPRESSION_LEVEL;
	private long capacity = DEFAULT_CAPACITY;
	private int maxEntries = DEFAULT_MAX_ENTRIES;
	private long maxIdleTime = DEFAULT_MAX_IDLE_TIME;
	private long maxLifeTime = DEFAULT_MAX_LIFE_TIME;
	private boolean fileMonitoring = DEFAULT_FILE_MONITORING;

	private static final int DEFAULT_COMPRESSION_LEVEL = 
		XOMUtil.getSystemProperty("nux.xom.pool.PoolConfig.compressionLevel", 0);

	private static final int DEFAULT_MAX_ENTRIES = 
		XOMUtil.getSystemProperty("nux.xom.pool.PoolConfig.maxEntries", 10000);

	private static final long DEFAULT_MAX_IDLE_TIME = 
		XOMUtil.getSystemProperty("nux.xom.pool.PoolConfig.maxIdleTime", 5L * 60 * 1000);

	private static final long DEFAULT_MAX_LIFE_TIME = 
		XOMUtil.getSystemProperty("nux.xom.pool.PoolConfig.maxLifeTime", Long.MAX_VALUE);

	private static final boolean DEFAULT_FILE_MONITORING = 
		XOMUtil.getSystemProperty("nux.xom.pool.PoolConfig.fileMonitoring", false);

	private static final long DEFAULT_INVALIDATION_PERIOD = 
		XOMUtil.getSystemProperty("nux.xom.pool.PoolConfig.invalidationPeriod", 10000L);

	private static final long DEFAULT_CAPACITY = getDefaultCapacity();

	private static long getDefaultCapacity() {
		long def = Runtime.getRuntime().maxMemory();
//		System.err.println("maxMem=" + (def / (1024 * 1024.0f)));
//		System.err.println("maxMem is Long.MAX_VALUE=" + (def == Long.MAX_VALUE));
		if (def != Long.MAX_VALUE) def = def / 2;
		return XOMUtil.getSystemProperty("nux.xom.pool.PoolConfig.capacity", def);
	}
	
	/**
	 * Constructs an instance with default parameters.
	 */
	public PoolConfig() {}
	
	/**
	 * Constructs a deep copy of the given source configuration.
	 * Helps to implement polymorphic copy().
	 * 
	 * @param src the source configuration to copy
	 */
	protected PoolConfig(PoolConfig src) {
		setCompressionLevel(src.getCompressionLevel());
		setCapacity(src.getCapacity());
		setMaxEntries(src.getMaxEntries());
		setMaxIdleTime(src.getMaxIdleTime());
		setMaxLifeTime(src.getMaxLifeTime());
		setFileMonitoring(src.getFileMonitoring());
	}
	
	/**
	 * Returns a deep copy of this configuration.
	 * 
	 * @return a deep copy
	 */
	public PoolConfig copy() {
		return new PoolConfig(this);
	}
	
	/**
	 * Returns the pool compression level.
	 * 
	 * @return the pool compression level
	 */
	public int getCompressionLevel() {
		return compressionLevel;
	}
	
	/**
	 * Sets the pool compression level (defaults to 0).
	 * 
	 * @param compressionLevel
	 *            a number in the range -1..9
	 * @return this
	 */
	public PoolConfig setCompressionLevel(int compressionLevel) {
		if (compressionLevel < -1 || compressionLevel > 9)
			throw new IllegalArgumentException("Compression level must be -1..9");
		this.compressionLevel = compressionLevel;
		return this;
	}
	
	/**
	 * Returns the maximum number of entries this pool can hold before starting
	 * to evict old entries.
	 * 
	 * @return the maximum number of entries
	 */
	public int getMaxEntries() {
		return maxEntries;
	}
	
	/**
	 * Sets the maximum number of entries this pool can hold before starting to
	 * evict old entries. A value of zero effectively disables pooling. A value
	 * of <code>Integer.MAX_VALUE</code> effectively disables this constraint.
	 * The default is 10000.
	 * 
	 * @param maxEntries
	 *            the new maxEntries
	 * @return this
	 */
	public PoolConfig setMaxEntries(int maxEntries) {
		this.maxEntries = maxEntries;
		return this;
	}
	
	/**
	 * Returns the maximum amount of memory [bytes] the sum of all contained pool
	 * values may occupy.
	 * 
	 * @return the maximum amount of memory
	 */
	public long getCapacity() {
		return capacity;
	}
	
	/**
	 * Sets the maximum amount of memory [bytes] the sum of all contained pool
	 * values may occupy. A value of <code>Long.MAX_VALUE</code> effectively
	 * disables this constraint. The default is
	 * <code>Runtime.maxMemory() / 2</code>.
	 * 
	 * @param capacity
	 *            the new capacity
	 * @return this
	 * @see Runtime#maxMemory()
	 */
	public PoolConfig setCapacity(long capacity) {
		this.capacity = capacity;
		return this;
	}
	
	/**
	 * Returns the (approximate) maximum time [ms] a pool entry is retained
	 * since its last access on get().
	 * 
	 * @return the maximum time
	 */
	public long getMaxIdleTime() {
		return maxIdleTime;
	}
	
	/**
	 * Sets the (approximate) maximum time [ms] a pool entry is retained since
	 * its last access on get(). A value of <code>Long.MAX_VALUE</code>
	 * effectively disables this constraint. The default is <code>5L * 60 * 1000</code>,
	 * i.e. 5 minutes.
	 * 
	 * @param maxIdleTime
	 *            the new maxIdleTime
	 * @return this
	 */
	public PoolConfig setMaxIdleTime(long maxIdleTime) {
		this.maxIdleTime = maxIdleTime;
		return this;
	}
	
	/**
	 * Returns the (approximate) maximum time [ms] a pool entry is retained
	 * since its creation/insertion on put().
	 * 
	 * @return the maximum time
	 */
	public long getMaxLifeTime() {
		return maxLifeTime;
	}
	
	/**
	 * Sets the (approximate) maximum time [ms] a pool entry is retained since
	 * its creation/insertion on put(). A value of <code>Long.MAX_VALUE</code>
	 * effectively disables this constraint (this is the default).
	 * 
	 * @param maxLifeTime
	 *            the new maxLifeTime
	 * @return this
	 */
	public PoolConfig setMaxLifeTime(long maxLifeTime) {
		this.maxLifeTime = maxLifeTime;
		return this;
	}

	/**
	 * Sets whether or not a pool should periodically monitor and automatically
	 * remove an entry if it's key is a {@link java.io.File} and that file has
	 * been modified or deleted since the entry has been inserted into the pool.
	 * The default is <code>false</code>.
	 * 
	 * @param fileMonitoring
	 *            true to enable auto-removal of changed files, false otherwise
	 * @return this
	 */
	public PoolConfig setFileMonitoring(boolean fileMonitoring) {
		this.fileMonitoring = fileMonitoring;
		return this;
	}
	
	/**
	 * Returns the file change invalidation policy.
	 * 
	 * @return the file change invalidation policy
	 */
	public boolean getFileMonitoring() {
		return fileMonitoring;
	}
	
	/** time [ms] the sweeper thread should sleep between runs */
	long getInvalidationPeriod() {
		return DEFAULT_INVALIDATION_PERIOD;
	}
	
//	public PoolConfig setEvictionRatio(double ratio) { 
//	// memSize[MB] * idleTime[s] > x --> evict
//	return this;
//}	

}
