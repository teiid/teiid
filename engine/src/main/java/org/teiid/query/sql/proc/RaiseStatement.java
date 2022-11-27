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

import org.teiid.core.types.DataTypeManager;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.Expression;


/**
 * <p> This class represents a error assignment statement in the storedprocedure language.
 * It extends the <code>Statement</code> that could part of a <code>Block</code>.  This
 * this object holds and error message.
 */
public class RaiseStatement extends Statement implements ExpressionStatement {

    private Expression expression;
    private boolean warning;

    public RaiseStatement() {
    }

    /**
     * Constructor for RaiseErrorStatement.
     * @param message The error message
     */
    public RaiseStatement(Expression message) {
        expression = message;
    }

    public RaiseStatement(Expression message, boolean warning) {
        expression = message;
        this.warning = warning;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    public Expression getExpression() {
        return expression;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    public int getType() {
        return TYPE_ERROR;
    }

    @Override
    public RaiseStatement clone() {
        return new RaiseStatement((Expression) this.expression.clone(), warning);
    }

    @Override
    public int hashCode() {
        return expression.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof RaiseStatement)) {
            return false;
        }

        RaiseStatement other = (RaiseStatement)obj;

        return other.expression.equals(this.expression) && this.warning == other.warning;
    }

    @Override
    public Class<?> getExpectedType() {
        return DataTypeManager.DefaultDataClasses.OBJECT;
    }

    public boolean isWarning() {
        return warning;
    }

    public void setWarning(boolean warning) {
        this.warning = warning;
    }

} // END CLASS