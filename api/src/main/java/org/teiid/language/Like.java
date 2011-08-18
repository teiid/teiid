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

public class Like extends Condition implements Predicate {
	
	public enum MatchMode {
		LIKE,
		SIMILAR,
		/**
		 * The escape char is typically not used in regex mode.
		 */
		REGEX
	}
        
    private Expression leftExpression;
    private Expression rightExpression;
    private Character escapeCharacter;
    private boolean isNegated;
    private MatchMode mode = MatchMode.LIKE;
    
    public Like(Expression left, Expression right, Character escapeCharacter, boolean negated) {
        leftExpression = left;
        rightExpression = right;
        this.escapeCharacter = escapeCharacter;
        this.isNegated = negated;
        
    }

    public Expression getLeftExpression() {
        return leftExpression;
    }

    public Expression getRightExpression() {
        return rightExpression;
    }

    public Character getEscapeCharacter() {
        return this.escapeCharacter;
    }

    public boolean isNegated() {
        return this.isNegated;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public void setLeftExpression(Expression expression) {
        this.leftExpression = expression;        
    }

    public void setRightExpression(Expression expression) {
        this.rightExpression = expression;
    }
    
    public void setEscapeCharacter(Character character) {
        this.escapeCharacter = character;
    }

    public void setNegated(boolean negated) {
        this.isNegated = negated;
    }
    
    public MatchMode getMode() {
		return mode;
	}
    
    public void setMode(MatchMode mode) {
		this.mode = mode;
	}

}
