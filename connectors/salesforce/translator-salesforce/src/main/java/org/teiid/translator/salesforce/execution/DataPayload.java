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
package org.teiid.translator.salesforce.execution;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * A bucket to pass data to the Salesforce connection.
 *
 */
public class DataPayload {

	public static class Field {
		public String name;
		public Object value;
		
		public Field(String name, Object value) {
			this.name = name;
			this.value = value;
		}
	}
	
	private String type;
	private List<Field> messageElements = new ArrayList<DataPayload.Field>();
	private String id;
	
	public void setType(String typeName) {
		type = typeName;
	}

	public String getType() {
		return type;
	}

	public void setID(String id) {
		this.id = id;
	}
	
	public String getID() {
		return id;
	}
	
	public List<Field> getMessageElements() {
		return messageElements;
	}
	
	public void addField(String name, Object value) {
		this.messageElements.add(new Field(name, value));
	}
	
}