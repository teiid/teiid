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

package org.teiid.query.processor.xml;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;


/**
 * This condition always evaluates to true, basically is a holder for a
 * {@link Program sub Program}, and can therefore be used as the
 * default choice in an {@link IfInstruction IfInstruction}.
 */
public class DefaultCondition extends Condition {


    public DefaultCondition(Program thenProgram){
        super( thenProgram );
    }

    
   
	/**
     * Always returns true
	 * @see org.teiid.query.processor.xml.Condition#evaluate(Map, List, ProcessorEnvironment)
	 */
	public boolean evaluate(XMLProcessorEnvironment env, XMLContext context) 
        throws TeiidComponentException, TeiidProcessingException{
		return true;
	}

    
    public String toString(){
        return "DefaultCondition"; //$NON-NLS-1$
    }



    /** 
     * @see org.teiid.query.processor.xml.Condition#getResultSetNames()    
     */
    public List getResultSetNames() {
        return Collections.EMPTY_LIST;
    }
}
