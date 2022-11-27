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

import java.util.ArrayList;
import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.ProcedureReservedWords;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.proc.Statement.Labeled;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * <p> This class represents a group of <code>Statement</code> objects. The
 * statements are stored on this object in the order in which they are added.
 */
public class Block extends Statement implements Labeled {

    // list of statements on this block
    private List<Statement> statements;
    private boolean atomic;
    private String label;

    private String exceptionGroup;
    private List<Statement> exceptionStatements;

    /**
     * Constructor for Block.
     */
    public Block() {
        statements = new ArrayList<Statement>();
    }

    /**
     * Constructor for Block with a single <code>Statement</code>.
     * @param statement The <code>Statement</code> to be added to the block
     */
    public Block(Statement statement) {
        this();
        statements.add(statement);
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    /**
     * Get all the statements contained on this block.
     * @return A list of <code>Statement</code>s contained in this block
     */
    public List<Statement> getStatements() {
        return statements;
    }

    /**
     * Set the statements contained on this block.
     * @param statements A list of <code>Statement</code>s contained in this block
     */
    public void setStatements(List<Statement> statements) {
        this.statements = statements;
    }

    /**
     * Add a <code>Statement</code> to this block.
     * @param statement The <code>Statement</code> to be added to the block
     */
    public void addStatement(Statement statement) {
        addStatement(statement, false);
    }

    public void addStatement(Statement statement, boolean exception) {
        if (statement instanceof AssignmentStatement) {
            AssignmentStatement stmt = (AssignmentStatement)statement;
            Command cmd = stmt.getCommand();
            if (cmd != null) {
                internalAddStatement(new CommandStatement(cmd), exception);
                stmt.setCommand(null);
                stmt.setExpression(null);
                if (stmt.getVariable().getShortName().equalsIgnoreCase(ProcedureReservedWords.ROWCOUNT)
                        && stmt.getVariable().getGroupSymbol() != null && stmt.getVariable().getGroupSymbol().getName().equalsIgnoreCase(ProcedureReservedWords.VARIABLES)) {
                    return;
                }
                String fullName = ProcedureReservedWords.VARIABLES+Symbol.SEPARATOR+ProcedureReservedWords.ROWCOUNT;
                stmt.setExpression(new ElementSymbol(fullName));
            }
        }
        internalAddStatement(statement, exception);
    }

    private void internalAddStatement(Statement statement, boolean exception) {
        if (exception) {
            if (this.exceptionStatements == null) {
                exceptionStatements = new ArrayList<Statement>();
            }
            exceptionStatements.add(statement);
        } else {
            statements.add(statement);
        }
    }

    // =========================================================================
    //                  P R O C E S S I N G     M E T H O D S
    // =========================================================================

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Deep clone statement to produce a new identical block.
     * @return Deep clone
     */
    public Block clone() {
        Block copy = new Block();
        copy.setAtomic(atomic);
        copy.statements = LanguageObject.Util.deepClone(statements, Statement.class);
        if (exceptionStatements != null) {
            copy.exceptionStatements = LanguageObject.Util.deepClone(exceptionStatements, Statement.class);
        }
        copy.exceptionGroup = this.exceptionGroup;
        copy.setLabel(label);
        return copy;
    }

    /**
     * Compare two queries for equality.  Blocks will only evaluate to equal if
     * they are IDENTICAL: statements in the block are equal and are in the same order.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        // Quick same object test
        if(this == obj) {
            return true;
        }

        // Quick fail tests
        if(!(obj instanceof Block)) {
            return false;
        }

        Block other = (Block)obj;

        // Compare the statements on the block
        return this.atomic == other.atomic
        && StringUtil.equalsIgnoreCase(label, other.label)
        && EquivalenceUtil.areEqual(getStatements(), other.getStatements())
        && EquivalenceUtil.areEqual(exceptionGroup, other.exceptionGroup)
        && EquivalenceUtil.areEqual(exceptionStatements, exceptionStatements);
    }

    /**
     * Get hashcode for block.  WARNING: This hash code relies on the hash codes of the
     * statements present in the block.  If statements are added to the block or if
     * statements on the block change the hash code will change. Hash code is only valid
     * after the block has been completely constructed.
     * @return Hash code
     */
    public int hashCode() {
        return statements.hashCode();
    }

    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    public boolean isAtomic() {
        return atomic;
    }

    public void setAtomic(boolean atomic) {
        this.atomic = atomic;
    }

    @Override
    public int getType() {
        return Statement.TYPE_COMPOUND;
    }

    public String getExceptionGroup() {
        return exceptionGroup;
    }

    public void setExceptionGroup(String exceptionGroup) {
        this.exceptionGroup = exceptionGroup;
    }

    public List<Statement> getExceptionStatements() {
        return exceptionStatements;
    }

    public void setExceptionStatements(List<Statement> exceptionStatements) {
        this.exceptionStatements = exceptionStatements;
    }

}// END CLASS
