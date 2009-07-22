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

package com.metamatrix.jdbc;

import java.util.List;
import java.util.Properties;

import org.jboss.cache.Cache;
import org.jboss.cache.DefaultCacheFactory;
import org.jboss.cache.config.CacheLoaderConfig.IndividualCacheLoaderConfig;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;

@Singleton
class CacheProvider implements Provider<org.jboss.cache.Cache> {
	
	@Inject @Named("DQPProperties") Properties props;

	public Cache get() {
		Cache cache = new DefaultCacheFactory().createCache("jboss-cache-configuration.xml", false); //$NON-NLS-1$
		List<IndividualCacheLoaderConfig> configs = cache.getConfiguration().getCacheLoaderConfig().getIndividualCacheLoaderConfigs();
		Properties p = configs.get(0).getProperties();
		String workspaceDir = this.props.getProperty(DQPEmbeddedProperties.DQP_WORKDIR);
		p.setProperty("location", workspaceDir + "/cache"); //$NON-NLS-1$ //$NON-NLS-2$
		configs.get(0).setProperties(p);
		cache.create();
		cache.start();
		return cache;
	}
}
