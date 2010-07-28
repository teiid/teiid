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

package org.teiid.common.buffer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.Streamable;
import org.teiid.dqp.DQPPlugin;
import org.teiid.query.sql.symbol.Expression;

/**
 * Tracks lob references so they are not lost during serialization.
 * TODO: for temp tables we may need to have a copy by value management strategy
 */
public class LobManager {

	private Map<String, Streamable<?>> lobReferences = new ConcurrentHashMap<String, Streamable<?>>(); 

	public void updateReferences(int[] lobIndexes, List<?> tuple)
			throws TeiidComponentException {
		for (int i = 0; i < lobIndexes.length; i++) {
			Object anObj = tuple.get(lobIndexes[i]);
			if (!(anObj instanceof Streamable<?>)) {
				continue;
			}
			Streamable lob = (Streamable) anObj;
			if (lob.getReference() == null) {
				lob.setReference(getLobReference(lob.getReferenceStreamId()).getReference());
			} else {
				String id = lob.getReferenceStreamId();
				this.lobReferences.put(id, lob);
			}
		}
	}
	
    public Streamable<?> getLobReference(String id) throws TeiidComponentException {
    	Streamable<?> lob = null;
    	if (this.lobReferences != null) {
    		lob = this.lobReferences.get(id);
    	}
    	if (lob == null) {
    		throw new TeiidComponentException(DQPPlugin.Util.getString("ProcessWorker.wrongdata")); //$NON-NLS-1$
    	}
    	return lob;
    }
    
    public void clear() {
    	this.lobReferences.clear();
    }
    
    public static int[] getLobIndexes(List expressions) {
    	if (expressions == null) {
    		return null;
    	}
		int[] result = new int[expressions.size()];
		int resultIndex = 0;
	    for (int i = 0; i < expressions.size(); i++) {
	    	Expression expr = (Expression) expressions.get(i);
	        if (DataTypeManager.isLOB(expr.getType()) || expr.getType() == DataTypeManager.DefaultDataClasses.OBJECT) {
	        	result[resultIndex++] = i;
	        }
	    }
	    if (resultIndex == 0) {
	    	return null;
	    }
	    return Arrays.copyOf(result, resultIndex);
    }

}
