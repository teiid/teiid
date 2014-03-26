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
package org.teiid.deployers;

import java.util.ArrayList;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.runtime.RuntimePlugin;


/**
 * This is used with ra.xml properties file to extend the metadata on the properties.
 */
public class ExtendedPropertyMetadata {
	String displayName;
	String description;
	boolean advanced;
	boolean masked;
	boolean editable = true;
	boolean required;
	ArrayList<String> allowed;
	String name;
	String dataType;
	String defaultValue;
	String category;
	String owner; 
	
	public ExtendedPropertyMetadata() {
	}
	
	public ExtendedPropertyMetadata(String name, String type, String encodedData, String defaultValue) {
		this.name = name;
		this.dataType = type;
		this.defaultValue = defaultValue;
		
		encodedData = encodedData.trim();
		
		// if not begins with { then treat as if just a simple description field.
		if (!encodedData.startsWith("{")) { //$NON-NLS-1$
			this.displayName = encodedData;
			return;
		}
		
		if (!encodedData.endsWith("}")) { //$NON-NLS-1$
			 throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40034, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40034, encodedData));
		}
		encodedData = encodedData.substring(1, encodedData.length()-1);
		
		int index = 0;
		int start = -1;
		boolean inQuotes = false;
		int inQuotesStart = -1;
		boolean inArray = false;
		
		String propertyName = null;
		ArrayList<String> values = new ArrayList<String>();
		for (char c:encodedData.toCharArray()) {
			if (c == '$' && start == -1) {
				start = index;
			}
			else if (c == '"') {
				inQuotes = !inQuotes;
				if (inQuotes && inQuotesStart == -1) {
					inQuotesStart = index;
				}
				else if (!inQuotes && inQuotesStart != -1) {
					if (inQuotesStart+1 != index) {
						values.add(encodedData.substring(inQuotesStart+1, index));
					}
					else {
						values.add(""); //$NON-NLS-1$
					}
					inQuotesStart = -1;
				}
			}
			else if (c == '[') {
				inArray = true;
			}
			else if (c == ']') {
				inArray = false;
			}
			else if (c == ':' && !inQuotes && !inArray && start != -1) {
				propertyName = encodedData.substring(start, index);
			}
			else if (c == ',' && !inQuotes && !inArray && start != -1) {
				addProperty(propertyName, values);
				propertyName = null;
				values = new ArrayList<String>();
				start = -1;
			}
			index++;
		}
		// add last property
		if (propertyName != null) {
			addProperty(propertyName, values);
		}
	}
	
	private void addProperty(String name, ArrayList<String> values) {
		if (name.equals("$display")) { //$NON-NLS-1$
			this.displayName = values.get(0);
		}
		else if (name.equals("$description")) { //$NON-NLS-1$
			this.description = values.get(0);
		}
		else if (name.equals("$advanced")) { //$NON-NLS-1$
			this.advanced = Boolean.parseBoolean(values.get(0));
		}
		else if (name.equals("$masked")) { //$NON-NLS-1$
			this.masked = Boolean.parseBoolean(values.get(0));
		}
		else if (name.equals("$editable")) { //$NON-NLS-1$
			this.editable = Boolean.parseBoolean(values.get(0));
		}
		else if (name.equals("$allowed")) { //$NON-NLS-1$
			this.allowed = new ArrayList<String>(values);
		}
		else if (name.equals("$required")) { //$NON-NLS-1$
			this.required = Boolean.parseBoolean(values.get(0));
		}
	}
	
	public String name() {
		return this.name;
	}
	public String description() {
		return description;
	}
	public String display() {
		return displayName;
	}
	public boolean advanced() {
		return advanced;
	}
	public boolean masked() {
		return masked;
	}
	public boolean readOnly() {
		return !editable;
	}
	public boolean required() {
		return required;
	}
	public String[] allowed() {
		if (allowed != null) {
			return allowed.toArray(new String[allowed.size()]);
		}
		return new String[] {};
	}
	public String datatype() {
		return this.dataType;
	}
	public String defaultValue() {
		return this.defaultValue;
	}
	public String category() {
	    return this.category;
	}
	public String owner() {
	    return this.owner;
	}
}
