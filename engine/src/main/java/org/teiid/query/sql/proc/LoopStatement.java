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
package org.teiid.query.sql.proc;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.proc.Statement.Labeled;


/**
 * <p> This class represents a loop statement in the storedprocedure language 
 * to cursor through a result set.
 * It extends the <code>Statement</code> that could part of a block.  This statement has
 * a block, a select statement and a cursor. 
 * determines which block should be executed..</p>
 */
public class LoopStatement extends Statement implements SubqueryContainer, Labeled {
    private String cursorName;
    private Block loopBlock;
    private Command query;
    private String label;
    
    public LoopStatement(Block block, Command query, String cursorName){
        this.loopBlock = block;
        this.query = query;
        this.cursorName = cursorName;
    }    
    
    public String getLabel() {
		return label;
	}
    
    public void setLabel(String label) {
		this.label = label;
	}
    
    /**
     * @return
     */
    public String getCursorName() {
        return cursorName;
    }

    /**
     * @return
     */
    public Block getBlock() {
        return loopBlock;
    }

    /**
     * @return
     */
    public Command getCommand() {
        return query;
    }

    /**
     * Sets the command. 
     */
    public void setCommand(Command command){
        this.query = command;
    }

    /**
     * @param string
     */
    public void setCursorName(String cursorName) {
        this.cursorName = cursorName;
    }

    /**
     * @param block
     */
    public void setBlock(Block block) {
        loopBlock = block;
    }

    /**
     * @param query
     */
    public void setCommand(Query query) {
        this.query = query;
    }
    
    /**
     * Return the type for this statement, this is one of the types
     * defined on the statement object.
     * @return The statement type
     */
    public int getType() {
        return Statement.TYPE_LOOP;
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
        Block otherBlock = this.loopBlock.clone();    
        Query otherQuery = (Query)this.query.clone();

        LoopStatement ls = new LoopStatement(otherBlock, otherQuery, this.cursorName);
        ls.setLabel(label);
        return ls;
    }
    
    /**
     * Compare two LoopStatements for equality.  They will only evaluate to equal if
     * they are IDENTICAL: the blocks is same, the query is same, and the cursor name is same.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        // Quick same object test
        if(this == obj) {
            return true;
        }

        // Quick fail tests     
        if(!(obj instanceof LoopStatement)) {
            return false;
        }

        LoopStatement other = (LoopStatement) obj;
        
        return 
            // Compare the query
            EquivalenceUtil.areEqual(query, other.query) &&
            // Compare the if block
            EquivalenceUtil.areEqual(loopBlock, other.loopBlock) &&
            // Compare the else block
            EquivalenceUtil.areEqual(cursorName, other.cursorName)
            && StringUtil.equalsIgnoreCase(this.label, other.label);
    } 

    /**
     * Get hashcode for LoopStatement.  WARNING: This hash code relies on the
     * hash codes of the block and the query
     * on this statement. Hash code is only valid after the block has been
     * completely constructed.
     * @return Hash code
     */
    public int hashCode() {
        // This hash code relies on the blocks and criteria for this statement
        int myHash = 0;
        myHash = HashCodeUtil.hashCode(myHash, this.loopBlock);
        myHash = HashCodeUtil.hashCode(myHash, this.query);
        myHash = HashCodeUtil.hashCode(myHash, this.cursorName);
        return myHash;
    }

}
