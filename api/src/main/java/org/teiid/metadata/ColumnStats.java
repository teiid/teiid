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

package org.teiid.metadata;

import java.io.Serializable;

public class ColumnStats implements Serializable {

	private static final long serialVersionUID = 7827734836519486538L;
	
	private Integer distinctValues;
    private Integer nullValues;
    private String minimumValue;
    private String maximumValue;
	
	public String getMinimumValue() {
		return minimumValue;
	}
	
	public void setMinimumValue(String min) {
		this.minimumValue = min;
	}
	
	public String getMaximumValue() {
		return maximumValue;
	}
	
	public void setMaximumValue(String max) {
		this.maximumValue = max;
	}

	public Integer getDistinctValues() {
		return distinctValues;
	}

	public void setDistinctValues(Integer numDistinctValues) {
		this.distinctValues = numDistinctValues;
	}

	public Integer getNullValues() {
		return nullValues;
	}

	public void setNullValues(Integer numNullValues) {
		this.nullValues = numNullValues;
	}
    
}
