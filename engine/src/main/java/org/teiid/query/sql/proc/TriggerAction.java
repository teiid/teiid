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

package org.teiid.query.sql.proc;

import java.util.List;

import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class TriggerAction extends Command {
	
	private GroupSymbol view;
	private Block block;
	
	public TriggerAction(Block b) {
		this.setBlock(b);
	}
	
	public Block getBlock() {
		return block;
	}
	
	public void setBlock(Block block) {
		block.setAtomic(true);
		this.block = block;
	}
	
	public GroupSymbol getView() {
		return view;
	}
	
	public void setView(GroupSymbol view) {
		this.view = view;
	}
		
	@Override
	public int hashCode() {
		return block.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof TriggerAction)) {
			return false;
		}
		TriggerAction other = (TriggerAction) obj;
		return block.equals(other.block);
	}

	@Override
	public String toString() {
		return SQLStringVisitor.getSQLString(this);
	}
	
	@Override
	public void acceptVisitor(LanguageVisitor visitor) {
		visitor.visit(this);
	}
	
	@Override
	public TriggerAction clone() {
		TriggerAction clone = new TriggerAction(this.block.clone());
		if (this.view != null) {
			clone.setView(view.clone());
		}
		return clone;
	}

	@Override
	public boolean areResultsCachable() {
		return false;
	}

	@Override
	public List<SingleElementSymbol> getProjectedSymbols() {
		return Command.getUpdateCommandSymbol();
	}

	@Override
	public int getType() {
		return Command.TYPE_TRIGGER_ACTION;
	}

}
