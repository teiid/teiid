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

import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;

public abstract class Alter<T extends Command> extends Command {
	
	private GroupSymbol target;
	private T definition;
	
	public GroupSymbol getTarget() {
		return target;
	}
	
	public void setTarget(GroupSymbol target) {
		this.target = target;
	}
	
	public T getDefinition() {
		return definition;
	}
	
	public void setDefinition(T definition) {
		this.definition = definition;
	}
	
	@Override
	public boolean areResultsCachable() {
		return false;
	}
	
	@Override
	public List<SingleElementSymbol> getProjectedSymbols() {
		return Command.getUpdateCommandSymbol();
	}
	
	public void cloneOnTo(Alter<T> clone) {
		copyMetadataState(clone);
		clone.setDefinition((T)getDefinition().clone());
		clone.setTarget(getTarget().clone());
	}
	
	@Override
	public int hashCode() {
		return HashCodeUtil.hashCode(this.target.hashCode(), this.definition);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj.getClass() != this.getClass()) {
			return false;
		}
		Alter<?> other = (Alter<?>)obj;
		return EquivalenceUtil.areEqual(this.target, other.target)
		&& EquivalenceUtil.areEqual(this.definition, other.definition);
	}
	
}
