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

package org.teiid.query.sql.lang;

import org.teiid.metadata.Table;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.proc.TriggerAction;

public class AlterTrigger extends Alter<TriggerAction> {
	
	private Table.TriggerOperation operation;
	
	public Table.TriggerOperation getOperation() {
		return operation;
	}
	
	public void setOperation(Table.TriggerOperation operation) {
		this.operation = operation;
	}
	
	@Override
	public void acceptVisitor(LanguageVisitor visitor) {
		visitor.visit(this);
	}
	
	@Override
	public AlterTrigger clone() {
		AlterTrigger clone = new AlterTrigger();
		cloneOnTo(clone);
		clone.operation = operation;
		return clone;
	}
	
	@Override
	public int getType() {
		return TYPE_ALTER_TRIGGER;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!super.equals(obj)) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		AlterTrigger other = (AlterTrigger)obj;
		return other.operation == this.operation;
	}
}
