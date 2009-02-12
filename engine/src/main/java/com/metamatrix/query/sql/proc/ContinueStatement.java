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

/*
 */
package com.metamatrix.query.sql.proc;

import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;

/**
 * <p> This class represents a continue statement in the storedprocedure language.
 * It extends the <code>Statement</code> that could part of a block.</p>
 */
public class ContinueStatement extends Statement {
    
    /**
     * Return the type for this statement, this is one of the types
     * defined on the statement object.
     */
    public int getType() {
        return Statement.TYPE_CONTINUE;
    }       

    // =========================================================================
    //                  P R O C E S S I N G     M E T H O D S
    // =========================================================================
    
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }
    
    /**
     * Deep clone statement to produce a new identical statement.
     * @return Deep clone 
     */
    public Object clone() {     
        return new ContinueStatement();
    }
    
    /**
     * Compare two ContinueStatements for equality.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        // Quick same object test
        if(this == obj) {
            return true;
        }

        // Quick fail tests     
        if(!(obj instanceof ContinueStatement)) {
            return false;
        }

        return true;
    } 
      
    public int hashCode() {
        //the continue statements are always equal
        return 0;
    }
      
    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }   
}
