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

package org.teiid.query.sql.lang;

import org.teiid.language.SortSpecification.NullOrdering;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class OrderByItem implements LanguageObject {

    private static final long serialVersionUID = 6937561370697819126L;

    private Integer expressionPosition; //set during resolving to the select clause position
    private boolean ascending = true;
    private Expression symbol;
    private NullOrdering nullOrdering;

    public OrderByItem(Expression symbol, boolean ascending) {
        setSymbol(symbol);
        this.ascending = ascending;
    }

    public int getExpressionPosition() {
        return expressionPosition == null?-1:expressionPosition;
    }

    public void setExpressionPosition(int expressionPosition) {
        this.expressionPosition = expressionPosition;
    }

    public boolean isAscending() {
        return ascending;
    }

    public void setAscending(boolean ascending) {
        this.ascending = ascending;
    }

    public Expression getSymbol() {
        return symbol;
    }

    public void setSymbol(Expression symbol) {
        if (symbol != null && !(symbol instanceof Symbol) && !(symbol instanceof Constant)) {
            symbol = new ExpressionSymbol("expr", symbol); //$NON-NLS-1$
        }
        this.symbol = symbol;
    }

    public NullOrdering getNullOrdering() {
        return nullOrdering;
    }

    public void setNullOrdering(NullOrdering nullOrdering) {
        this.nullOrdering = nullOrdering;
    }

    /**
     *
     * @return true if the expression does not appear in the select clause
     */
    public boolean isUnrelated() {
        return getExpressionPosition() == -1;
    }

    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public OrderByItem clone() {
        OrderByItem clone = new OrderByItem((Expression)this.symbol.clone(), ascending);
        clone.expressionPosition = this.expressionPosition;
        clone.nullOrdering = this.nullOrdering;
        return clone;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof OrderByItem)) {
            return false;
        }
        OrderByItem o = (OrderByItem)obj;
        return o.symbol.equals(symbol) && o.ascending == this.ascending && o.nullOrdering == this.nullOrdering;
    }

    @Override
    public int hashCode() {
        return symbol.hashCode();
    }

    @Override
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

}
