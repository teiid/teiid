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
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.SubqueryContainer;


/**
 * <p> This class represents a variable assignment statement in the storedprocedure language.
 * It extends the <code>Statement</code> that could part of a block.  This statement has
 * a command that should be executed as part of the procedure.
 */
public class CommandStatement extends Statement implements SubqueryContainer {

    // the command this statement represents
    Command command;
    private boolean returnable = true;

    /**
     * Constructor for CommandStatement.
     */
    public CommandStatement() {
        super();
    }

    /**
     * Constructor for CommandStatement.
     * @param value The <code>Command</code> on this statement
     */
    public CommandStatement(Command value) {
        this.command = value;
    }

    /**
     * Get the command on this statement.
     * @return The <code>Command</code> on this statement
     */
    public Command getCommand() {
        return command;
    }

    public void setCommand(Command command){
        this.command = command;
    }

    /**
     * Return the type for this statement, this is one of the types
     * defined on the statement object.
     * @return The type of this statement
     */
    public int getType() {
        return Statement.TYPE_COMMAND;
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
        CommandStatement cs = new CommandStatement((Command)this.command.clone());
        cs.returnable = this.returnable;
        return cs;
    }

    /**
     * Compare two CommandStatements for equality.  They will only evaluate to equal if
     * they are IDENTICAL: the command objects are equal.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        // Quick same object test
        if(this == obj) {
            return true;
        }

        // Quick fail tests
        if(!(obj instanceof CommandStatement)) {
            return false;
        }

        return EquivalenceUtil.areEqual(getCommand(), ((CommandStatement)obj).getCommand());
    }

    /**
     * Get hashcode for CommandStatement.  WARNING: This hash code relies on the
     * hash code of the command on this statement.
     * @return Hash code
     */
    public int hashCode() {
        // This hash code relies on the commands hash code
        return this.getCommand().hashCode();
    }

    public boolean isReturnable() {
        return returnable;
    }

    public void setReturnable(boolean returnable) {
        this.returnable = returnable;
    }

} // END CLASS
