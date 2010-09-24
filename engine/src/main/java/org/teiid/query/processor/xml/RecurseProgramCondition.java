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

import java.util.List;
import java.util.Map;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.lang.Criteria;


/**
 * This special condition handles recursive XML processing.  If it evaluates
 * to true, the Program it returns is a Program which is already on the program
 * stack of the Processor Environment.
 */
public class RecurseProgramCondition extends CriteriaCondition {

    private static final int NO_LIMIT = -1;
    private static final boolean NO_EXCEPTION = false;

    private int recursionLimit;
    private boolean exceptionOnRecursionLimit;

    public RecurseProgramCondition(Program subProgramToRecurse,  Criteria terminationCriteria){
        this(subProgramToRecurse, terminationCriteria, NO_LIMIT, NO_EXCEPTION);
    }

    public RecurseProgramCondition(Program subProgramToRecurse, Criteria terminationCriteria, int recursionLimit, boolean exceptionOnRecursionLimit){
        super(terminationCriteria, subProgramToRecurse);
        
        this.recursionLimit = recursionLimit;
        this.exceptionOnRecursionLimit = exceptionOnRecursionLimit;
    }

    
    /** 
     * @see org.teiid.query.processor.xml.Condition#isProgramRecursive()
     */
    public boolean isProgramRecursive() {
        return true;
    }    

	/**
     * Evaluates to true, unless the termination criteria is true, or unless the recursion
     * count limit has been reached
     * @throws TeiidComponentException if it was indicated in the constructor that an
     * exception should be thrown because the recursion count limit was reached, OR if there
     * was a problem evaluating the termination condition relational criteria
	 * @see org.teiid.query.processor.xml.Condition#evaluate(Map, List, ProcessorEnvironment)
	 */
	public boolean evaluate(XMLProcessorEnvironment env, XMLContext context)
        throws TeiidComponentException, TeiidProcessingException{

        boolean terminate = false;

        if (criteria != null) {
            terminate = super.evaluate(env, context);
        }

        if (!terminate && this.recursionLimit != NO_LIMIT){
            // Recursion limit (from model) is 1-based. 
            // The recursion count is also 1-based, with zero meaning the program is not
            // recursive, 1 meaning the program is on it's first recursive iteration, etc. 
            // At this point, the condition is about to check the recursion count of the 
            // program on the PREVIOUS iteration.  If that count is GREATER THAN or even
            // EQUAL TO the recursion limit, we don't want to push the program onto the 
            // stack another time, so terminate.
            terminate = env.getProgramRecursionCount(this.getThenProgram()) >= this.recursionLimit;

            //handle the case of exception on recursion limit reached
            if (terminate && this.exceptionOnRecursionLimit){
                throw new TeiidComponentException("ERR.015.006.0039", QueryPlugin.Util.getString("ERR.015.006.0039")); //$NON-NLS-1$ //$NON-NLS-2$
            }
        }

        return !terminate;
	}

    public String toString(){
        StringBuffer s = new StringBuffer("RECURSE "); //$NON-NLS-1$
        s.append("termination Criteria "); //$NON-NLS-1$
        s.append(criteria);
        s.append(", recursion limit "); //$NON-NLS-1$
        if (this.recursionLimit == NO_LIMIT){
            s.append("none"); //$NON-NLS-1$
        } else {
            s.append(this.recursionLimit);
        }
        s.append(", exception on limit "); //$NON-NLS-1$
        s.append(this.exceptionOnRecursionLimit);

        return s.toString();
    }

}
