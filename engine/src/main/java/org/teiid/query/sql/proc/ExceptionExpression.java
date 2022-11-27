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
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class ExceptionExpression implements Expression, LanguageObject {

    private Expression message;
    private Expression sqlState;
    private Expression errorCode;
    private Expression parent;

    @Override
    public Class<?> getType() {
        return DataTypeManager.DefaultDataClasses.OBJECT;
    }

    public ExceptionExpression() {
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof ExceptionExpression)) {
            return false;
        }
        ExceptionExpression other = (ExceptionExpression)obj;
        return EquivalenceUtil.areEqual(message, other.message)
        && EquivalenceUtil.areEqual(sqlState, other.sqlState)
        && EquivalenceUtil.areEqual(errorCode, other.errorCode)
        && EquivalenceUtil.areEqual(parent, other.parent);
    }

    @Override
    public int hashCode() {
        return HashCodeUtil.hashCode(0, message, sqlState, errorCode);
    }

    @Override
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    @Override
    public ExceptionExpression clone() {
        ExceptionExpression clone = new ExceptionExpression();
        if (this.message != null) {
            clone.message = (Expression) this.message.clone();
        }
        if (this.sqlState != null) {
            clone.sqlState = (Expression) this.sqlState.clone();
        }
        if (this.errorCode != null) {
            clone.errorCode = (Expression) this.errorCode.clone();
        }
        if (this.parent != null) {
            clone.parent = (Expression) this.parent.clone();
        }
        return clone;
    }

    public Expression getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(Expression errCode) {
        this.errorCode = errCode;
    }

    public Expression getSqlState() {
        return sqlState;
    }

    public void setSqlState(Expression sqlState) {
        this.sqlState = sqlState;
    }

    public Expression getMessage() {
        return message;
    }

    public void setMessage(Expression message) {
        this.message = message;
    }

    public Expression getParent() {
        return parent;
    }

    public void setParent(Expression parent) {
        this.parent = parent;
    }

    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    public String getDefaultSQLState() {
        return "50001";
    }

}
