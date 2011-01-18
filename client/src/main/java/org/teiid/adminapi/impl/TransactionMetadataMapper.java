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

public class TransactionMetadataMapper extends MetaMapper<TransactionMetadata> {
	private static final String ID = "id"; //$NON-NLS-1$
	private static final String SCOPE = "scope"; //$NON-NLS-1$
	private static final String CREATED_TIME = "createdTime"; //$NON-NLS-1$
	private static final String ASSOCIATED_SESSION = "associatedSession"; //$NON-NLS-1$
	private static final MutableCompositeMetaType metaType;
	private static final MetaValueFactory metaValueFactory = MetaValueFactory.getInstance();
	
	static {
		metaType = new MutableCompositeMetaType(TransactionMetadata.class.getName(), "The Transaction domain meta data"); //$NON-NLS-1$
		metaType.addItem(ASSOCIATED_SESSION, ASSOCIATED_SESSION, SimpleMetaType.STRING);
		metaType.addItem(CREATED_TIME, CREATED_TIME, SimpleMetaType.LONG_PRIMITIVE);
		metaType.addItem(SCOPE, SCOPE, SimpleMetaType.STRING);
		metaType.addItem(ID, ID, SimpleMetaType.STRING);
		metaType.freeze();
	}
	
	@Override
	public Type mapToType() {
		return TransactionMetadata.class;
	}
	
	@Override
	public MetaType getMetaType() {
		return metaType;
	}
	
	@Override
	public MetaValue createMetaValue(MetaType metaType, TransactionMetadata object) {
		if (object == null)
			return null;
		if (metaType instanceof CompositeMetaType) {
			CompositeMetaType composite = (CompositeMetaType) metaType;
			CompositeValueSupport transaction = new CompositeValueSupport(composite);
			
			transaction.set(ASSOCIATED_SESSION, SimpleValueSupport.wrap(object.getAssociatedSession()));
			transaction.set(CREATED_TIME, SimpleValueSupport.wrap(object.getCreatedTime()));
			transaction.set(SCOPE, SimpleValueSupport.wrap(object.getScope()));
			transaction.set(ID, SimpleValueSupport.wrap(object.getId()));
			
			return transaction;
		}
		throw new IllegalArgumentException("Cannot convert TransactionMetadata " + object); //$NON-NLS-1$
	}

	@Override
	public TransactionMetadata unwrapMetaValue(MetaValue metaValue) {
		if (metaValue == null)
			return null;

		if (metaValue instanceof CompositeValue) {
			CompositeValue compositeValue = (CompositeValue) metaValue;
			
			TransactionMetadata transaction = new TransactionMetadata();
			transaction.setAssociatedSession((String) metaValueFactory.unwrap(compositeValue.get(ASSOCIATED_SESSION)));
			transaction.setCreatedTime((Long) metaValueFactory.unwrap(compositeValue.get(CREATED_TIME)));
			transaction.setScope((String) metaValueFactory.unwrap(compositeValue.get(SCOPE)));
			transaction.setId((String) metaValueFactory.unwrap(compositeValue.get(ID)));
			return transaction;
		}
		throw new IllegalStateException("Unable to unwrap TransactionMetadata " + metaValue); //$NON-NLS-1$
	}
}
