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

import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;

/**
 * <p> This class represents a while statement in the storedprocedure language.
 * It extends the <code>Statement</code> that could part of a block.  This statement has
 * a block and a criteria that
 * determines when to exit the while loop.</p>
 */
public class WhileStatement extends Statement {

    private Block whileBlock;
    
    // criteria on the if block
    private Criteria condition;

    /**
     * Constructor for IfStatement.
     * @param criteria The criteria determining which bleck should be executed
     * @param ifBlock The IF <code>Block</code> object.
     * @param ifBlock The ELSE <code>Block</code> object.
     */
    public WhileStatement(Criteria criteria, Block block) {
        this.whileBlock = block;
        this.condition = criteria;
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
     * @return
     */
    public Block getBlock() {
        return whileBlock;
    }

    /**
     * @param block
     */
    public void setBlock(Block block) {
        whileBlock = block;
    }
    
    /**
     * Return the type for this statement, this is one of the types
     * defined on the statement object.
     * @return The statement type
     */
    public int getType() {
        return Statement.TYPE_WHILE;
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
        Block otherBlock = (Block)this.whileBlock.clone();
        Criteria otherCrit = (Criteria) this.condition.clone();     

        return new WhileStatement(otherCrit, otherBlock);
    }
    
    /**
     * Compare two WhileStatements for equality.  They will only evaluate to equal if
     * they are IDENTICAL: the block is same and the condition on  is same.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        // Quick same object test
        if(this == obj) {
            return true;
        }

        // Quick fail tests     
        if(!(obj instanceof WhileStatement)) {
            return false;
        }

        WhileStatement other = (WhileStatement) obj;
        
        return 
            // Compare the condition
            EquivalenceUtil.areEqual(getCondition(), other.getCondition()) &&
            // Compare the if block
            EquivalenceUtil.areEqual(whileBlock, other.whileBlock);
    } 

    /**
     * Get hashcode for WhileStatement. WARNING: This hash code relies on the
     * hash codes of the block and the criteria. Hash code is only valid after the block has been
     * completely constructed.
     * @return Hash code
     */
    public int hashCode() {
        // This hash code relies on the blocks and criteria for this statement
        int myHash = 0;
        myHash = HashCodeUtil.hashCode(myHash, this.getCondition());
        myHash = HashCodeUtil.hashCode(myHash, this.getBlock());

        return myHash;
    }

    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

}
