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

/*
 */
package org.teiid.query.sql.proc;

import org.teiid.core.util.HashCodeUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.query.sql.LanguageVisitor;

/**
 * <p> This class represents a break statement in the storedprocedure language.
 * It extends the <code>Statement</code> that could part of a block.
 */
public class BranchingStatement extends Statement {

    public enum BranchingMode {
        /**
         * Teiid specific - only allowed to target loops
         */
        BREAK,
        /**
         * Teiid specific - only allowed to target loops
         */
        CONTINUE,
        /**
         * ANSI - allowed to leave any block
         */
        LEAVE
    }

    private String label;
    private BranchingMode mode;

    public BranchingStatement() {
        this(BranchingMode.BREAK);
    }

    public BranchingStatement(BranchingMode mode) {
        this.mode = mode;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setMode(BranchingMode mode) {
        this.mode = mode;
    }

    public BranchingMode getMode() {
        return mode;
    }

    /**
     * Return the type for this statement, this is one of the types
     * defined on the statement object.
     */
    public int getType() {
        switch (mode) {
        case BREAK:
            return Statement.TYPE_BREAK;
        case CONTINUE:
            return Statement.TYPE_CONTINUE;
        case LEAVE:
            return Statement.TYPE_LEAVE;
        }
        throw new AssertionError();
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
    public BranchingStatement clone() {
        BranchingStatement clone = new BranchingStatement();
        clone.mode = mode;
        clone.label = label;
        return clone;
    }

    /**
     * Compare two BreakStatements for equality.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        // Quick same object test
        if(this == obj) {
            return true;
        }
        if (!(obj instanceof BranchingStatement)) {
            return false;
        }
        BranchingStatement other = (BranchingStatement)obj;
        return StringUtil.equalsIgnoreCase(label, other.label)
        && mode == other.mode;
    }

    public int hashCode() {
        return HashCodeUtil.hashCode(mode.hashCode());
    }

}
