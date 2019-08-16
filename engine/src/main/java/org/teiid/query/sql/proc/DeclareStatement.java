/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.sql.proc;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * <p> This class represents a statement used to declare variables in the
 * storedprocedure language.
 */
public class DeclareStatement extends AssignmentStatement {

    // type of the variable
    private String varType;

    /**
     * Constructor for DeclareStatement.
     */
    public DeclareStatement() {
        super();
    }

    /**
     * Constructor for DeclareStatement.
     * @param variable The <code>ElementSymbol</code> object that is the variable
     * @param varType The type of this variable
     */
    public DeclareStatement(ElementSymbol variable, String varType) {
        super(variable, (Expression)null);
        this.varType = varType;
    }

    /**
     * Constructor for DeclareStatement.
     * @param variable The <code>ElementSymbol</code> object that is the variable
     * @param varType The type of this variable
     */
    public DeclareStatement(ElementSymbol variable, String varType, Expression value) {
        super(variable, value);
        this.varType = varType;
    }

    @Deprecated public DeclareStatement(ElementSymbol variable, String varType, Command value) {
        super(variable, value);
        this.varType = varType;
    }

    /**
     * Get the type of this variable declared in this statement.
     * @return A string giving the variable type
     */
    public String getVariableType() {
        return varType;
    }

    /**
     * Set the type of this variable declared in this statement.
     * @param varType A string giving the variable type
     */
    public void setVariableType(String varType) {
        this.varType = varType;
    }

    /**
     * Return the type for this statement, this is one of the types
     * defined on the statement object.
     * @return The type of this statement
     */
    public int getType() {
        return Statement.TYPE_DECLARE;
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
        if (getExpression() == null) {
            return new DeclareStatement(this.getVariable().clone(), this.varType);
        }
        return new DeclareStatement(this.getVariable().clone(), this.varType, (Expression)getExpression().clone());
    }

    /**
     * Compare two DeclareStatements for equality.  They will only evaluate to equal if
     * they are IDENTICAL: variable and the its type are equal.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        // Quick same object test
        if(this == obj) {
            return true;
        }

        // Quick fail tests
        if(obj == null || !(obj instanceof DeclareStatement) || !super.equals(obj)) {
            return false;
        }

        DeclareStatement other = (DeclareStatement) obj;

        return
            EquivalenceUtil.areEqual(getVariableType(), other.getVariableType());
    }

    /**
     * Get hashcode for TableAssignmentStatement.  WARNING: This hash code relies on the hash codes of the
     * statements present in the block.  If statements are added to the block or if
     * statements on the block change the hash code will change. Hash code is only valid
     * after the block has been completely constructed.
     * @return Hash code
     */
    public int hashCode() {
        // This hash code relies on the variable and its value for this statement
        // and criteria clauses, not on the from, order by, or option clauses
        int myHash = super.hashCode();
        myHash = HashCodeUtil.hashCode(myHash, this.getVariableType());
        return myHash;
    }

    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

} // END CLASS
