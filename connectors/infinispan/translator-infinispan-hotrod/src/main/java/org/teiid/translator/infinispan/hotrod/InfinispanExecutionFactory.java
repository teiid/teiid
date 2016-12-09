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

package org.teiid.translator.infinispan.hotrod;

import org.teiid.translator.Translator;



/**
 * InfinispanExecutionFactory (deprecated, @see InfinispanExecutionFactory) is the translator that will be use to translate  a remote Infinispan cache and issue queries
 * using DSL to query the cache.  
 * 
 * @author vhalbert
 * 
 * @since 8.7
 *
 */
@Deprecated
@Translator(name = "infinispan-cache-dsl", description = "(Deprecated) The Infinispan Translator Using DSL to Query Cache")
public class InfinispanExecutionFactory extends InfinispanHotRodExecutionFactory {
	
	public InfinispanExecutionFactory() {
		super();
	}

}
