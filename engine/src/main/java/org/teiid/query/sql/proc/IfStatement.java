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
import org.teiid.query.sql.lang.Criteria;


/**
 * <p> This class represents an if-else statement in the storedprocedure language.
 * It extends the <code>Statement</code> that could part of a block.  This statement has
 * an IF block and an optional ELSE block, it also holds reference to the criteria that
 * determines which block should be executed..
 */
public class IfStatement extends Statement  {

    // the IF block
    private Block ifBlock;

    // the ELSE block
    private Block elseBlock;

    // criteria on the if block
    private Criteria condition;

    /**
     * Constructor for IfStatement.
     */
    public IfStatement() {
        super();
    }

    /**
     * Constructor for IfStatement.
     * @param criteria The criteria determining which block should be executed
     * @param ifBlock The IF <code>Block</code> object.
     * @param elseBlock The ELSE <code>Block</code> object.
     */
    public IfStatement(Criteria criteria, Block ifBlock, Block elseBlock) {
        this.ifBlock = ifBlock;
        this.elseBlock = elseBlock;
        this.condition = criteria;
    }

    /**
     * Constructor for IfStatement.
     * @param criteria The criteria determining which block should be executed
     * @param ifBlock The IF <code>Block</code> object.
     */
    public IfStatement(Criteria criteria, Block ifBlock) {
        this(criteria, ifBlock, null);
    }

    /**
     * Get the statement's IF block.
     * @return The IF <code>Block</code> object.
     */
    public Block getIfBlock() {
        return ifBlock;
    }

    /**
     * Set the statement's IF block.
     * @param block The IF <code>Block</code> object.
     */
    public void setIfBlock(Block block) {
        this.ifBlock = block;
    }

    /**
     * Get the statement's ELSE block.
     * @return The ELSE <code>Block</code> object.
     */
    public Block getElseBlock() {
        return elseBlock;
    }

    /**
     * Set the statement's ELSE block.
     * @param block The ELSE <code>Block</code> object.
     */
    public void setElseBlock(Block block) {
        elseBlock = block;
    }

    /**
     * Return a boolean indicating if the statement has an else block.
     * @return A boolean indicating if the statement has an else block
     */
    public boolean hasElseBlock() {
        return (elseBlock != null);
    }

    /**
     * Get the condition that determines which block needs to be executed.
     * @return The <code>Criteria</code> to determine block execution
     */
    public Criteria getCondition() {
        return condition;
    }

    /**
     * Set the condition that determines which block needs to be executed.
     * @param criteria The <code>Criteria</code> to determine block execution
     */
    public void setCondition(Criteria criteria) {
        this.condition = criteria;
    }

    /**
     * Return the type for this statement, this is one of the types
     * defined on the statement object.
     * @return The statement type
     */
    public int getType() {
        return Statement.TYPE_IF;
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
        Block otherIf = this.ifBlock.clone();
        Criteria otherCrit = (Criteria) this.condition.clone();
        Block otherElse = null;
        if(this.hasElseBlock()) {
            otherElse = this.elseBlock.clone();
        }

        return new IfStatement(otherCrit, otherIf, otherElse);
    }

    /**
     * Compare two IfStatements for equality.  They will only evaluate to equal if
     * they are IDENTICAL: their if, else blocks are same and the condition on the
     * ifBlock is same.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        // Quick same object test
        if(this == obj) {
            return true;
        }

        // Quick fail tests
        if(!(obj instanceof IfStatement)) {
            return false;
        }

        IfStatement other = (IfStatement) obj;

        return
            // Compare the condition
            EquivalenceUtil.areEqual(getCondition(), other.getCondition()) &&
            // Compare the if block
            EquivalenceUtil.areEqual(getIfBlock(), other.getIfBlock()) &&
            // Compare the else block
            EquivalenceUtil.areEqual(this.getElseBlock(), other.getElseBlock());
    }

    /**
     * Get hashcode for IfStatement.  WARNING: This hash code relies on the
     * hash codes of the if-else blocks anf the criteria determining the block
     * on this statement. Hash code is only valid after the block has been
     * completely constructed.
     * @return Hash code
     */
    public int hashCode() {
        // This hash code relies on the blocks and criteria for this statement
        int myHash = 0;
        myHash = HashCodeUtil.hashCode(myHash, this.getCondition());
        myHash = HashCodeUtil.hashCode(myHash, this.getIfBlock());
        myHash = HashCodeUtil.hashCode(myHash, this.getElseBlock());
        return myHash;
    }

} // END CLASS
