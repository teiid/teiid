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

package org.teiid.query.sql.symbol;

import org.teiid.core.util.Assertion;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.LanguageVisitor;


/**
 * An AliasSymbol wraps a SingleElementSymbol and changes it's name.  AliasSymbols
 * should be used to perform the aliasing of elements in a SELECT clause.  They
 * should typically NOT be used elsewhere in a query.  The alias symbol takes on
 * the type of it's underlying SingleElementSymbol.  AliasSymbols are typically
 * applied to ElementSymbol, ExpressionSymbol, and AggregateSymbol.
 */
public class AliasSymbol extends Symbol implements DerivedExpression {

    private Expression symbol;

    /**
     * Constructor used for cloning
     * @param name
     * @param canonicalName
     * @since 4.3
     */
    private AliasSymbol(String name, String canonicalName, Expression symbol) {
        super(name);
        setSymbol(symbol);
    }

    /**
     * Construct an AliasSymbol given the alias name and the underlying symbol.
     * @param name Name of the alias
     * @param symbol Underlying symbol
     */
    public AliasSymbol(String name, Expression symbol) {
        super(name);
        setSymbol(symbol);
    }

    /**
     * Get the underlying symbol
     * @return Underlying symbol
     */
    public Expression getSymbol() {
        return this.symbol;
    }

    /**
     * Set the underlying symbol
     * @param symbol New symbol
     */
    public void setSymbol(Expression symbol) {
        if(symbol instanceof AliasSymbol || symbol == null){
            Assertion.failed(QueryPlugin.Util.getString("ERR.015.010.0029")); //$NON-NLS-1$
        }
        this.symbol = symbol;
    }

    /**
     * Get the type of the symbol
     * @return Type of the symbol
     */
    public Class<?> getType() {
        return this.symbol.getType();
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Return a copy of this object.
     */
    public Object clone() {
        Expression symbolCopy = (Expression) this.symbol.clone();
        AliasSymbol result = new AliasSymbol(getName(), getShortName(), symbolCopy);
        result.outputName = this.outputName;
        return result;
    }

   /**
     * @see org.teiid.query.sql.symbol.Symbol#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof AliasSymbol)) {
            return false;
        }
        AliasSymbol other = (AliasSymbol)obj;
        return this.getName().equals(other.getName()) && this.symbol.equals(other.symbol);
    }

}
