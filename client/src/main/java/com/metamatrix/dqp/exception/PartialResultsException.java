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

package com.metamatrix.dqp.exception;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.api.exception.MetaMatrixProcessingException;

/**
 * <p>This exception is returned as a warning by <code>getWarning()</code>
 * method on the <code>Results</code> object. This object holds a list of
 * <code>SourceFailureDetails</code> objects. It constructs a warning message
 * from the failure details for each atomic query detailing the data sources
 * against which the query failed.</p>
 */

public class PartialResultsException extends MetaMatrixProcessingException {
	
    private static final String NEW_LINE = System.getProperty("line.separator"); //$NON-NLS-1$
    private static final String EMPTY_STR = ""; //$NON-NLS-1$
    private Collection failureList; // Collection of source failure details
	
   /**
    * <p>Construct a default instance of this class.</p>
    */
    public PartialResultsException() {
        super(EMPTY_STR);
        failureList = new ArrayList();
    }
    
   /**
    * <p>This method is used to add <code>SourceFailureDetails</code> objects
    *  for each atomic query failure.</p>
    * @param details A <code>SourceFailureDetails</code> object
    */
    public void addSourceFailure(SourceFailureDetails details) {
   		failureList.add(details);   	
    }
   
   /**
    * <p>Get's a collection of <code>SourceFailureDetails</code> objects</p>
    * @return A List containing <code>SourceFailureDetails</code> objects
    */
    public Collection getSourceFailureDetails() {
		return failureList;
	}   
   
   /**
    * <p>This method returns a warning message detailing the data sources
    * against which the query failed. The message details the connector binding
    * name, model name and the actual exception message for each atomic query
    * failure.</p>
    * @return A Warning message detailing atomic query failures.
    */
    public String getMessage() {
		
		Iterator failureIter = failureList.iterator();
		StringBuffer warningBuf = new StringBuffer(AdminPlugin.Util.getString("PartialResultsException.WARNING__There_were_failures_while_processing_the_query")); //$NON-NLS-1$
        warningBuf.append(NEW_LINE);
		while(failureIter.hasNext()) {
			SourceFailureDetails details = (SourceFailureDetails) failureIter.next();
			warningBuf.append(details);
			warningBuf.append(NEW_LINE);
		}
			
   		return warningBuf.toString();
    }

    /**
     * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
     */
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        failureList = (Collection)in.readObject();
    }

    /**
     * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
     */
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(failureList);
    }

} // END CLASS
