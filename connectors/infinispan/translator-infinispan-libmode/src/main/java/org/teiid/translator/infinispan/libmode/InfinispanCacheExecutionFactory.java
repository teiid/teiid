/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.translator.infinispan.libmode;

import org.teiid.translator.Translator;

/**
 * InfinispanExecutionFactory is the "infinispan-cache" translator that is used to access an Infinispan cache.
 * <p>
 * The optional setting is:
 * <li>{@link #supportsDSLSearching DSL Searching} - will default to <code>false</code>, supporting only Key searching.
 * Set to <code>true</code> will use the Infinispan DSL query language to search the cache for objects</li> 
 * </li>
 * 
 * @author vhalbert
 *
 */
@Deprecated
@Translator(name = "infinispan-cache", description = "The Infinispan Cache Library Mode Translator (Deprecated)")
public class InfinispanCacheExecutionFactory extends InfinispanLibModeExecutionFactory {


	public InfinispanCacheExecutionFactory() {
		super();

	}
	
}
