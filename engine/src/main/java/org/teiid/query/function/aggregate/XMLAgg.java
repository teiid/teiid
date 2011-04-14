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

package org.teiid.query.function.aggregate;

import java.util.List;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.XMLType;
import org.teiid.query.function.source.XMLSystemFunctions.XmlConcat;
import org.teiid.query.util.CommandContext;

/**
 * Aggregates XML entries
 */
public class XMLAgg extends AggregateFunction {

	private XMLType result;
	private XmlConcat concat;
    private CommandContext context;
    
    public XMLAgg(CommandContext context) {
    	this.context = context;
	}

    public void reset() {
    	concat = null;
    	result = null;
    }

    /**
     * @throws TeiidProcessingException 
     * @throws TeiidComponentException 
     * @see org.teiid.query.function.aggregate.AggregateFunction#addInputDirect(Object, List)
     */
    public void addInputDirect(Object input, List<?> tuple) throws TeiidComponentException, TeiidProcessingException {
    	if (concat == null) {
    		concat = new XmlConcat(context.getBufferManager());
    	}
    	concat.addValue(input);
    }

    /**
     * @throws TeiidProcessingException 
     * @throws TeiidComponentException 
     * @see org.teiid.query.function.aggregate.AggregateFunction#getResult()
     */
    public Object getResult() throws TeiidComponentException, TeiidProcessingException {
    	if (result == null) {
    		if (concat == null) {
        		return null;
    		}
    		result = concat.close();
    		concat = null;
    	}
        return result;
    }

}
