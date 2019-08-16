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

import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.Expression;


/**
 * <p> This class represents a return statement
 */
public class ReturnStatement extends AssignmentStatement {

    public ReturnStatement(Expression value) {
        super(null, value);
    }

    /**
     * Return the type for this statement, this is one of the types
     * defined on the statement object.
     * @return The type of this statement
     */
    public int getType() {
        return Statement.TYPE_RETURN;
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
        ReturnStatement clone = new ReturnStatement(null);
        if (this.getExpression() != null) {
            clone.setExpression((Expression) this.getExpression().clone());
        }
        if (this.getVariable() != null) {
            clone.setVariable(this.getVariable().clone());
        }
        return clone;
    }

    public boolean equals(Object obj) {
        // Quick same object test
        if(this == obj) {
            return true;
        }

        // Quick fail tests
        if(!(obj instanceof ReturnStatement)) {
            return false;
        }

        ReturnStatement other = (ReturnStatement) obj;
        return super.equals(other);
    }

} // END CLASS
