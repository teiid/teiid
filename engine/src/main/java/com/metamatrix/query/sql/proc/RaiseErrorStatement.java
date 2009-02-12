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

package com.metamatrix.query.sql.proc;

import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.ReservedWords;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.GroupSymbol;

/**
 * <p> This class represents a error assignment statement in the storedprocedure language.
 * It extends the <code>Statement</code> that could part of a <code>Block</code>.  This
 * this object holds and error message.</p>
 */
public class RaiseErrorStatement extends AssignmentStatement {

	/**
	 * Constructor for RaiseErrorStatement.
	 */
	public RaiseErrorStatement() {
		super();
	}
	
	/**
	 * Constructor for RaiseErrorStatement.
	 * @param message The error message
	 */
	public RaiseErrorStatement(Expression message) {
        super(createElementSymbol(), message);
	}
        
    private static ElementSymbol createElementSymbol() {
        /*
         * The element symbol created here is just a placeholder for reusing
         * the logic in AssignmentStatement/AssignmentInstruction.  It should not
         * matter that it has an invalid ID or GroupSymbol.  Setting the type to
         * String allows for the expression to be converted to String as necessary.
         */
        ElementSymbol result = new ElementSymbol(ReservedWords.ERROR);
        result.setMetadataID(ReservedWords.ERROR);
        result.setType(String.class);
        result.setGroupSymbol(new GroupSymbol(ReservedWords.ERROR));
        return result;
    }
    
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }
    
    
    /** 
     * @see com.metamatrix.query.sql.proc.AssignmentStatement#getType()
     */
    public int getType() {
        return TYPE_ERROR;
    }
    
} // END CLASS