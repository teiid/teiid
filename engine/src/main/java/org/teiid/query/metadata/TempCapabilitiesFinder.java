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

package org.teiid.query.metadata;

import org.teiid.core.TeiidComponentException;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.translator.ExecutionFactory.NullOrder;
import org.teiid.translator.ExecutionFactory.TransactionSupport;

public class TempCapabilitiesFinder implements CapabilitiesFinder {

	private final static BasicSourceCapabilities cachedTempCaps = defaultCapabilities();
	static BasicSourceCapabilities defaultCapabilities() {
		BasicSourceCapabilities tempCaps = new BasicSourceCapabilities();
		tempCaps.setCapabilitySupport(Capability.INSERT_WITH_ITERATOR, true);
		tempCaps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
		tempCaps.setCapabilitySupport(Capability.CRITERIA_IN, true);
		tempCaps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
		tempCaps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
		tempCaps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED_EXCLUSIVE, true);
		tempCaps.setCapabilitySupport(Capability.CRITERIA_ONLY_LITERAL_COMPARE, true);
		tempCaps.setCapabilitySupport(Capability.CRITERIA_ISNULL, true);
		tempCaps.setCapabilitySupport(Capability.CRITERIA_LIKE, true);
		tempCaps.setCapabilitySupport(Capability.CRITERIA_LIKE_ESCAPE, true);
		tempCaps.setCapabilitySupport(Capability.CRITERIA_LIKE_REGEX, true);
		tempCaps.setCapabilitySupport(Capability.CRITERIA_SIMILAR, true);
		tempCaps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);
		tempCaps.setCapabilitySupport(Capability.ARRAY_TYPE, true);
		tempCaps.setCapabilitySupport(Capability.UPSERT, true);
		tempCaps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, 100000);
		tempCaps.setSourceProperty(Capability.MAX_DEPENDENT_PREDICATES, 1);
		tempCaps.setSourceProperty(Capability.TRANSACTION_SUPPORT, TransactionSupport.XA);
		tempCaps.setSourceProperty(Capability.QUERY_ORDERBY_DEFAULT_NULL_ORDER, NullOrder.LOW);
		return tempCaps;
	}
	
	private final CapabilitiesFinder delegate;
	private BasicSourceCapabilities tempCaps = cachedTempCaps;
	
	public TempCapabilitiesFinder(CapabilitiesFinder delegate) {
		this(delegate, NullOrder.LOW);
	}
	
	public TempCapabilitiesFinder(CapabilitiesFinder delegate, NullOrder nullOrder) {
		this.delegate = delegate;
		if (nullOrder != NullOrder.LOW) {
			tempCaps = defaultCapabilities();
			tempCaps.setSourceProperty(Capability.QUERY_ORDERBY_DEFAULT_NULL_ORDER, nullOrder);
		}
	}

	@Override
	public SourceCapabilities findCapabilities(String modelName)
			throws TeiidComponentException {
		if (TempMetadataAdapter.TEMP_MODEL.getID().equals(modelName)) {
    		return tempCaps;
    	}
		return delegate.findCapabilities(modelName);
	}
	
	@Override
	public boolean isValid(String modelName) {
		if (TempMetadataAdapter.TEMP_MODEL.getID().equals(modelName)) {
			return true;
		}
		return delegate.isValid(modelName);
	}

}
