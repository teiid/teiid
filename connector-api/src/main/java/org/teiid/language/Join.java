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

public class Join extends BaseLanguageObject implements TableReference {
	
	public enum JoinType {
		INNER_JOIN,
		CROSS_JOIN,
		LEFT_OUTER_JOIN,
		RIGHT_OUTER_JOIN,
		FULL_OUTER_JOIN
	}

    private TableReference leftItem;
    private TableReference rightItem;
    private JoinType joinType;
    private Condition condition;
    
    public Join(TableReference left, TableReference right, JoinType joinType, Condition criteria) {
        this.leftItem = left;
        this.rightItem = right;
        this.joinType = joinType;
        this.condition = criteria;
    }

    public TableReference getLeftItem() {
        return leftItem;
    }

    public TableReference getRightItem() {
        return rightItem;
    }

    public JoinType getJoinType() {
        return this.joinType;
    }

    public Condition getCondition() {
        return condition;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }
    
    public void setLeftItem(TableReference item) {
        this.leftItem = item;
    }

    public void setRightItem(TableReference item) {
        this.rightItem = item;
    }
    
    public void setJoinType(JoinType type) {
        this.joinType = type;
    }

    public void setCondition(Condition criteria) {
        this.condition = criteria;
    }

}
