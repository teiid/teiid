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

public class Parameter extends BaseLanguageObject implements Expression {

    private Class<?> type;
    private int valueIndex;
    private String dependentValueId;
    
	@Override
	public Class<?> getType() {
		return type;
	}
	
	public void setType(Class<?> type) {
		this.type = type;
	}

	@Override
	public void acceptVisitor(LanguageObjectVisitor visitor) {
		visitor.visit(this);
	}

	public void setValueIndex(int valueIndex) {
		this.valueIndex = valueIndex;
	}

	/**
	 * 0-based index of the parameter values in the {@link BatchedCommand#getParameterValues()} row value
	 * @return
	 */
	public int getValueIndex() {
		return valueIndex;
	}
	
	/**
	 * The id of the dependent values this parameter references.  Dependent values are available via {@link Select#getDependentValues()}
	 * Will only be set for dependent join pushdown.
	 * @return
	 */
	public String getDependentValueId() {
		return dependentValueId;
	}
	
	public void setDependentValueId(String dependentValueId) {
		this.dependentValueId = dependentValueId;
	}
	
}
