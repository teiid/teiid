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

package com.metamatrix.metadata.runtime.impl;

import com.metamatrix.modeler.core.index.IndexConstants;

public class MetadataRecordDelegate {

    private String uuid;
    private String parentUUID;
    private String nameInSource;
    private String fullName;
	private String name;
	
	public String getUUID() {
		return uuid;
	}
	public void setUUID(String uuid) {
		this.uuid = uuid;
	}
	public String getParentUUID() {
		return parentUUID;
	}
	public void setParentUUID(String parentUUID) {
		this.parentUUID = parentUUID;
	}
	public String getNameInSource() {
		return nameInSource;
	}
	public void setNameInSource(String nameInSource) {
		this.nameInSource = nameInSource;
	}
	public String getFullName() {
        return this.fullName == null ? this.name : this.fullName;
	}
	public void setFullName(String fullName) {
		this.fullName = fullName;
	}
	public String getName() {
    	if(this.name == null || this.name.trim().length() == 0) {
			int nmIdx = this.fullName != null ? this.fullName.lastIndexOf(IndexConstants.NAME_DELIM_CHAR) : -1;
			if (nmIdx == -1) {
				this.name = this.fullName;
			} else {
				this.name = this.fullName != null ? this.fullName.substring(nmIdx+1) : null;
			}
    	}
		return name;
	}	
	public void setName(String name) {
		this.name = name;
	}
	
}
