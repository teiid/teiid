/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.console.ui.views.users;

/***
* Data class to store a role's name, description, and display name.
*/
public class RoleDisplay  {
	private String name;
	private String description;
	private String displayName;
	
	public RoleDisplay(String name, String desc) {
		this.name = name;
		this.description = desc;
		setDisplayName(this.name);
	}
	
	private void setDisplayName(String name) {
//        this.displayName = name.substring(name.indexOf(".") + 1, name.length());    
        this.displayName = name;    
	}
	
	public String getName() {
		return this.name;
	}
	
	public String getDescription() {
		return this.description;
	}
	
	public String getDisplayName() {
		return this.displayName;
	}
	
	public boolean equals(Object obj) {
		boolean same = false;
		if (obj != null) {
			if (obj == this) {
				same = true;
			} else if (obj instanceof RoleDisplay) {
				RoleDisplay rd = (RoleDisplay)obj;
				same = this.name.equals(rd.getName());
			}
		}
		return same;
	}
}
