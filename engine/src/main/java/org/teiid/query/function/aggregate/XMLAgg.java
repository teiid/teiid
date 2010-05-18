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
import org.teiid.query.function.source.XMLSystemFunctions;
import org.teiid.query.util.CommandContext;

/**
 * Aggregates XML entries
 */
public class XMLAgg extends AggregateFunction {

    private XMLType result;
    private CommandContext context;
    
    public XMLAgg(CommandContext context) {
    	this.context = context;
	}

    public void reset() {
        result = null;
    }

    /**
     * @throws TeiidProcessingException 
     * @throws TeiidComponentException 
     * @see org.teiid.query.function.aggregate.AggregateFunction#addInputDirect(Object, List)
     */
    public void addInputDirect(Object input, List<?> tuple) throws TeiidComponentException, TeiidProcessingException {
    	result = XMLSystemFunctions.xmlConcat(context, result, input);
    }

    /**
     * @see org.teiid.query.function.aggregate.AggregateFunction#getResult()
     */
    public Object getResult() {
        return result;
    }

}
