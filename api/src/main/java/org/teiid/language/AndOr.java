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
 * Represents a logical criteria such as AND, OR, or NOT.
 */
public class AndOr extends Condition {
    
	public enum Operator {
		AND,
		OR,
	}

    private Condition leftCondition;
    private Condition rightCondition;
    private Operator operator = Operator.AND;
    	
    public AndOr(Condition left, Condition right, Operator operator) {
    	this.leftCondition = left;
    	this.rightCondition = right;
        this.operator = operator; 
    }

    /**
     * Get operator used to connect these criteria.
     * @return Operator constant
     */
    public Operator getOperator() {
        return this.operator;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Set operator used to connect these criteria.
     * @param operator Operator constant
     */
    public void setOperator(Operator operator) {
        this.operator = operator;
    }
    
    public Condition getLeftCondition() {
		return leftCondition;
	}
    
    public Condition getRightCondition() {
		return rightCondition;
	}
    
    public void setLeftCondition(Condition left) {
		this.leftCondition = left;
	}
    
    public void setRightCondition(Condition right) {
		this.rightCondition = right;
	}
    
}
