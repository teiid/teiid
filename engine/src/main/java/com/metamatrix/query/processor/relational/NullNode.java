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

package com.metamatrix.query.processor.relational;

import java.util.Collections;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.TupleBatch;

public class NullNode extends RelationalNode {

    public NullNode(int nodeID) {
        super(nodeID);
    }

    public TupleBatch nextBatchDirect()
        throws MetaMatrixComponentException {

        TupleBatch batch = new TupleBatch(1, Collections.EMPTY_LIST);
        batch.setTerminationFlag(true);
        return batch;
    }
        
    protected void getNodeString(StringBuffer str) {
        super.getNodeString(str);
    }

	public Object clone(){
		NullNode clonedNode = new NullNode(super.getID());
		super.copy(this, clonedNode);
		return clonedNode;
	}
    
    /* 
     * @see com.metamatrix.query.processor.Describable#getDescriptionProperties()
     */
    public Map getDescriptionProperties() {   
        // Default implementation - should be overridden     
        Map props = super.getDescriptionProperties();
        props.put(PROP_TYPE, "Null");         //$NON-NLS-1$
        return props;
    }
    
}
