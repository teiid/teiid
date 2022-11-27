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
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ScalarSubquery;


/**
 * <p> This class represents an assignment statement in the storedprocedure language.
 * It extends the <code>Statement</code> that could part of a <code>Block</code>.  This
 * statement holds references to the variable and it's value which could be an
 * <code>Expression</code> or a <code>Command</code>.
 */
public class AssignmentStatement extends Statement implements ExpressionStatement {

    // the variable to which a value is assigned
    private ElementSymbol variable;
    private Expression value;
    private Command command;

    /**
     * Constructor for AssignmentStatement.
     */
    public AssignmentStatement() {
        super();
    }

    public AssignmentStatement(ElementSymbol variable, QueryCommand value) {
        this.variable = variable;
        this.value = new ScalarSubquery(value);
    }

    @Deprecated
    public AssignmentStatement(ElementSymbol variable, Command value) {
        this.variable = variable;
        if (value instanceof QueryCommand) {
            this.value = new ScalarSubquery((QueryCommand)value);
        } else {
            this.command = value;
        }
    }

    public AssignmentStatement(ElementSymbol variable, Expression value) {
        this.variable = variable;
        this.value = value;
    }

    @Deprecated
    public Command getCommand() {
        if (command != null) {
            return command;
        }
        if (value instanceof ScalarSubquery && ((ScalarSubquery)value).getCommand() instanceof Query) {
            Query query = (Query)((ScalarSubquery)value).getCommand();
            if (query.getInto() != null) {
                return query;
            }
        }
        return null;
    }

    public void setCommand(Command command) {
        this.command = command;
    }

    public Expression getExpression() {
        return this.value;
    }

    public void setExpression(Expression expression) {
        this.value = expression;
    }

    /**
     * Get the expression giving the value that is assigned to the variable.
     * @return An <code>Expression</code> with the value
     */
    public ElementSymbol getVariable() {
        return this.variable;
    }

    /**
     * Set the variable that is assigned to the value
     * @param variable <code>ElementSymbol</code> that is being assigned
     */
    public void setVariable(ElementSymbol variable) {
        this.variable = variable;
    }

    /**
     * Return the type for this statement, this is one of the types
     * defined on the statement object.
     * @return The type of this statement
     */
    public int getType() {
        return Statement.TYPE_ASSIGNMENT;
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
        AssignmentStatement clone = new AssignmentStatement(this.variable.clone(), (Expression) this.value.clone());
        return clone;
    }

    /**
     * Compare two AssignmentStatements for equality.  They will only evaluate to equal if
     * they are IDENTICAL: variable and its value which could be a command or an expression
     * objects are equal.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        // Quick same object test
        if(this == obj) {
            return true;
        }

        // Quick fail tests
        if(!(obj instanceof AssignmentStatement)) {
            return false;
        }

        AssignmentStatement other = (AssignmentStatement) obj;

        return
            // Compare the variables
            EquivalenceUtil.areEqual(this.getVariable(), other.getVariable()) &&
            // Compare the values
            EquivalenceUtil.areEqual(this.getExpression(), other.getExpression());
            // Compare the values
    }

    /**
     * Get hashcode for AssignmentStatement.  WARNING: This hash code relies on the hash codes
     * of the variable and its value which could be a command or an expression.
     * @return Hash code
     */
    public int hashCode() {
        // This hash code relies on the variable and its value for this statement
        // and criteria clauses, not on the from, order by, or option clauses
        int myHash = 0;
        myHash = HashCodeUtil.hashCode(myHash, this.getVariable());
        myHash = HashCodeUtil.hashCode(myHash, this.getExpression());
        return myHash;
    }

    @Override
    public Class<?> getExpectedType() {
        if (this.variable == null) {
            return null;
        }
        return this.variable.getType();
    }


} // END CLASS
