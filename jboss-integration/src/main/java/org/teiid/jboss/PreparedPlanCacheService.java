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
package org.teiid.jboss;

import org.jboss.msc.value.InjectedValue;
import org.jboss.msc.value.Value;
import org.teiid.cache.CacheConfiguration;
import org.teiid.cache.CacheFactory;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.dqp.internal.process.PreparedPlan;
import org.teiid.dqp.internal.process.SessionAwareCache;


class PreparedPlanCacheService extends CacheService<PreparedPlan> {
	public PreparedPlanCacheService(SessionAwareCache.Type type, CacheConfiguration config) {
		super(type, config);
		this.cacheFactoryInjector = new InjectedValue<CacheFactory>();
		this.cacheFactoryInjector.setValue(new Value() {
			@Override
			public CacheFactory getValue() throws IllegalStateException, IllegalArgumentException {
				return new DefaultCacheFactory();
			}
		});
	}
	
}
