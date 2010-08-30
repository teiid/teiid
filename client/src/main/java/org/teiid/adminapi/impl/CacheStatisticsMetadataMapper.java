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
package org.teiid.adminapi.impl;

import java.lang.reflect.Type;

import org.jboss.metatype.api.types.CompositeMetaType;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.types.SimpleMetaType;
import org.jboss.metatype.api.values.CompositeValue;
import org.jboss.metatype.api.values.CompositeValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.MetaValueFactory;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.jboss.metatype.plugins.types.MutableCompositeMetaType;
import org.jboss.metatype.spi.values.MetaMapper;

public class CacheStatisticsMetadataMapper extends MetaMapper<CacheStatisticsMetadata> {
	private static final String HITRATIO = "hitRatio"; //$NON-NLS-1$
	private static final String TOTAL_ENTRIES = "totalEntries"; //$NON-NLS-1$
	private static final String REQUEST_COUNT = "requestCount"; //$NON-NLS-1$
	private static final MetaValueFactory metaValueFactory = MetaValueFactory.getInstance();
	private static final MutableCompositeMetaType metaType;
	
	static {
		metaType = new MutableCompositeMetaType(CacheStatisticsMetadata.class.getName(), "The Cache statistics"); //$NON-NLS-1$
		metaType.addItem(TOTAL_ENTRIES, TOTAL_ENTRIES, SimpleMetaType.INTEGER_PRIMITIVE);
		metaType.addItem(HITRATIO, HITRATIO, SimpleMetaType.DOUBLE_PRIMITIVE);
		metaType.addItem(REQUEST_COUNT, REQUEST_COUNT, SimpleMetaType.INTEGER_PRIMITIVE);
		metaType.freeze();
	}
	
	@Override
	public Type mapToType() {
		return CacheStatisticsMetadata.class;
	}
	
	@Override
	public MetaType getMetaType() {
		return metaType;
	}
	
	@Override
	public MetaValue createMetaValue(MetaType metaType, CacheStatisticsMetadata object) {
		if (object == null)
			return null;
		if (metaType instanceof CompositeMetaType) {
			CompositeMetaType composite = (CompositeMetaType) metaType;
			CompositeValueSupport cache = new CompositeValueSupport(composite);
			
			cache.set(TOTAL_ENTRIES, SimpleValueSupport.wrap(object.getTotalEntries()));
			cache.set(HITRATIO, SimpleValueSupport.wrap(object.getHitRatio()));
			cache.set(REQUEST_COUNT, SimpleValueSupport.wrap(object.getRequestCount()));
			
			return cache;
		}
		throw new IllegalArgumentException("Cannot convert cache statistics " + object); //$NON-NLS-1$
	}

	@Override
	public CacheStatisticsMetadata unwrapMetaValue(MetaValue metaValue) {
		if (metaValue == null)
			return null;

		if (metaValue instanceof CompositeValue) {
			CompositeValue compositeValue = (CompositeValue) metaValue;
			
			CacheStatisticsMetadata cache = new CacheStatisticsMetadata();
			cache.setTotalEntries((Integer) metaValueFactory.unwrap(compositeValue.get(TOTAL_ENTRIES)));
			cache.setHitRatio((Double) metaValueFactory.unwrap(compositeValue.get(HITRATIO)));
			cache.setRequestCount((Integer) metaValueFactory.unwrap(compositeValue.get(REQUEST_COUNT)));
			return cache;
		}
		throw new IllegalStateException("Unable to unwrap cache statistics " + metaValue); //$NON-NLS-1$
	}

}
