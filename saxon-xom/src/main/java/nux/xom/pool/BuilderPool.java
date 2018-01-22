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

import java.net.URI;
import java.util.Map;

import nu.xom.Builder;
import nu.xom.ParsingException;
import nu.xom.ValidityException;
import nu.xom.XMLException;

import org.xml.sax.EntityResolver;

/**
 * Efficient thread-safe pool/cache of XOM {@link Builder} objects, creating and
 * holding zero or more Builders per thread. On cache miss, a new Builder is
 * created via a factory, cached for future reuse, and then returned. On cache
 * hit a Builder is returned <em>instantly</em>.
 * <p>
 * Recognizing that Builders are not thread-safe but can be reused serially,
 * this class helps to avoid the large overhead involved in creating a Builder
 * instance (more precisely: an underlying XMLReader instance), in particular for
 * Builders that parse small XML documents and/or validate against W3C XML
 * schemas or RELAX NG schemas. Most useful in high throughput server container
 * environments (e.g. large-scale Peer-to-Peer messaging network infrastructures
 * over high-bandwidth networks, scalable MOMs, etc).
 * <p>
 * Internally, each thread has its own local pool entries, and each pool can
 * hold at most a given number of builders, evicting old builders beyond that
 * point via a LRU (least recently used) policy, or if the JVM runs low on free
 * memory.
 * <p>
 * Thread-safe implementation (internally uses a {@link ThreadLocal}).
 * <p>
 * Example usage (in any arbitrary thread and any arbitrary object):
 * 
 * <pre>
 * public void foo() {
 *   // non-validating parser
 *   Document doc = BuilderPool.GLOBAL_POOL.getBuilder(false).build(new File("/tmp/test.xml"));
 *   //Document doc = new Builder(false).build(new File("/tmp/test.xml")); // would be inefficient
 *   System.out.println(doc.toXML());
 * 
 *   // W3C XML Schema parser
 *   Map schemaLocations = new HashMap(1);
 *   schemaLocations.put(new File("/tmp/p2pio.xsd"), "http://dsd.lbl.gov/p2pio-1.0"); 
 *   Builder builder = BuilderPool.GLOBAL_POOL.getW3CBuilder(schemaLocations);
 *   Document doc = builder.build(new File("/tmp/test.xml"));
 *   System.out.println(doc.toXML());
 * 
 *   // RELAX NG validation for DOCBOOK publishing system
 *   Builder builder = BuilderPool.GLOBAL_POOL.getMSVBuilder(new URI("http://www.docbook.org/docbook-ng/ipa/docbook.rng"));
 *   //Builder builder = BuilderPool.GLOBAL_POOL.getMSVBuilder(new File("/tmp/docbook/docbook.rng").toURI());
 *   Document doc = builder.build(new File("/tmp/mybook.xml"));
 *   System.out.println(doc.toXML());
 * }
 * </pre>
 * 
 * @author whoschek.AT.lbl.DOT.gov
 * @author $Author: hoschek3 $
 * @version $Revision: 1.42 $, $Date: 2005/12/05 06:53:05 $
 */
public class BuilderPool {
	
	/**
	 * A default pool (can be shared freely across threads without harm); global per class loader.
	 */
	public static final BuilderPool GLOBAL_POOL = new BuilderPool();
	
	/** 
	 * Current pool entries.
	 * One local map per thread; When a thread terminates and is no more
	 * referenced its copy of "threadlocal" (with contained builders) is subject to
	 * garbage collection.
	 * Contrary to claims at http://www.javalobby.org/thread.jspa?forumID=61&threadID=13090
	 * usage of such a non-static ThreadLocal does *not* leak memory.
	 */
	private final ThreadLocal threadlocal;
	
	/**
	 * The factory used to create new Builders from scratch.
	 */
	private final BuilderFactory factory;
	
	/**
	 * Creates a new pool with default parameters.
	 */
	public BuilderPool() {
		this(new PoolConfig(), new BuilderFactory());
	}
	
	/**
	 * Creates a new pool with the given configuration that uses the given
	 * factory on cache misses.
	 * 
	 * @param config
	 *            the configuration to use
	 * @param factory
	 *            the factory creating new Builder instances on cache misses
	 */
	public BuilderPool(PoolConfig config, BuilderFactory factory) {
		if (config == null) 
			throw new IllegalArgumentException("config must not be null");
		if (factory == null) 
			throw new IllegalArgumentException("factory must not be null");
		this.factory = factory;		
		this.threadlocal = createThreadLocal(config.copy());
	}
	
	/**
	 * Returns a validating or non-validating <code>Builder</code>. In
	 * validating mode, a {@link ValidityException} will be thrown when
	 * encountering an XML validation error upon parsing.
	 * 
	 * @param validate
	 *            true if XML validation should be performed.
	 * 
	 * @throws XMLException
	 *             if an appropriate parser cannot be found or created.
	 * @return a new Builder
	 */
	public Builder getBuilder(boolean validate) {
		Boolean key = validate ? Boolean.TRUE : Boolean.FALSE;
		Map entries =  (Map) threadlocal.get();
		Builder builder = (Builder) entries.get(key);
		if (builder == null) {
			builder = factory.createBuilder(validate);
			entries.put(key, builder);
		}
		return builder;
	}

	/**
	 * Returns a <code>Builder</code> that validates against the
	 * DTD obtained by the given entity resolver.
	 * 
	 * @param resolver 
	 * 			the entity resolver obtaining the DTD
	 * 
	 * @throws XMLException
	 *             if an appropriate parser cannot be found or created.
	 * @return a new Builder
	 */
	public Builder getDTDBuilder(EntityResolver resolver) {
		Object key = resolver;
		if (key == null) key = Pool.createHashKeys(new Object[] {resolver});
		
		Map entries =  (Map) threadlocal.get();
		Builder builder = (Builder) entries.get(key);
		if (builder == null) {
			builder = factory.createDTDBuilder(resolver); 
			entries.put(key, builder);
		}
		return builder;
	}

	/**
	 * Returns a <code>Builder</code> that validates against W3C XML Schemas.
	 * <p>
	 * For a detailed description of the parameters,
	 * see {@link BuilderFactory#createW3CBuilder(Map)}.
	 * 
	 * @param schemaLocations 
	 * 			the <code>schemaLocation --> namespace</code> associations 
	 * 			(may be <code>null</code>).
	 * 
	 * @throws XMLException
	 *             if an appropriate parser cannot be found or created.
	 * @return a new Builder
	 */
	public Builder getW3CBuilder(Map schemaLocations) {
		Object key = Pool.createHashKeys(new Object[] {schemaLocations, null});
		Map entries =  (Map) threadlocal.get();
		Builder builder = (Builder) entries.get(key);
		if (builder == null) {
			builder = factory.createW3CBuilder(schemaLocations); 
			entries.put(key, builder);
		}
		return builder;
	}

	/**
	 * Returns a <code>Builder</code> that validates against the given MSV
	 * (Multi-Schema Validator) schema. A {@link ParsingException} will be
	 * thrown when encountering an XML validation error upon parsing.
	 * <p>
	 * The type of all schemas written in XML-syntax (RELAX NG, W3C XML Schema,
	 * etc) will be auto-detected correctly by MSV no matter what the format is.
	 * 
	 * @param schema
	 *            the URL of the schema to validate against.
	 * @throws XMLException
	 *             if the schema contains a syntax or semantic error or an appropriate
	 *             parser cannot be found or created.
	 * @return a new Builder
	 */
	public Builder getMSVBuilder(URI schema) {
		if (schema == null) 
			throw new IllegalArgumentException("schema must not be null");
		Object key = schema;
		Map entries =  (Map) threadlocal.get();
		Builder builder = (Builder) entries.get(key);
		if (builder == null) {
			builder = factory.createMSVBuilder(null, schema); 
			entries.put(key, builder);
		}
		return builder;
	}
	
	private static ThreadLocal createThreadLocal(final PoolConfig config) {
		return new ThreadLocal() {
			protected Object initialValue() { // lazy init
				return Pool.createPool(config);
			}
		};
	}
	
}