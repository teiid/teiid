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

package org.teiid.language;

import org.teiid.language.visitor.LanguageObjectVisitor;

/**
 * Represents a literal value that is used in
 * an expression.  The value can be obtained and should match
 * the type specified by {@link #getType()}
 */
public class Literal extends BaseLanguageObject implements Expression {
    
    private Object value;
    private Class<?> type;
    private boolean bindValue;
    private boolean multiValued;
    private boolean isBindEligible;
    
    public Literal(Object value, Class<?> type) {
        this.value = value;
        this.type = type;
    }
    
    public Object getValue() {
        return this.value;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public Class<?> getType() {
        return this.type;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public boolean isBindValue() {
        return bindValue;
    }

    public void setBindValue(boolean bindValue) {
        this.bindValue = bindValue;
    }

	public boolean isMultiValued() {
		return multiValued;
	}

	public void setMultiValued(boolean multiValued) {
		this.multiValued = multiValued;
	}

	public void setType(Class<?> type) {
		this.type = type;
	}
	
	/**
	 * Set by the optimizer if the literal was created by the evaluation of another expression.
	 * Setting to true will not always result in the value being handled as a bind value.
	 * That can be forced {@link #isBindValue()}
	 * @return
	 */
	public boolean isBindEligible() {
		return isBindEligible;
	}
	
	public void setBindEligible(boolean isBindEligible) {
		this.isBindEligible = isBindEligible;
	}

}
