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
package org.teiid.templates.connector;

import java.util.ArrayList;

import com.metamatrix.core.MetaMatrixRuntimeException;

public class ExtendedPropertyMetadata {
	private String displayName;
	private String description;
	private boolean advanced;
	private boolean masked;
	private boolean editable = true;
	private boolean required;
	private ArrayList<String> allowed;
	

	public ExtendedPropertyMetadata(String encodedData) {
		encodedData = encodedData.trim();
		
		// if not begins with { then treat as if just a simple description field.
		if (!encodedData.startsWith("{")) {
			this.displayName = encodedData;
			return;
		}
		
		if (!encodedData.endsWith("}")) {
			throw new MetaMatrixRuntimeException("The description field = "+encodedData+" does not end with \"}\"");
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
						values.add("");
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
		addProperty(propertyName, values);
	}
	
	private void addProperty(String name, ArrayList<String> values) {
		if (name.equals("$display")) {
			this.displayName = values.get(0);
		}
		else if (name.equals("$description")) {
			this.description = values.get(0);
		}
		else if (name.equals("$advanced")) {
			this.advanced = Boolean.parseBoolean(values.get(0));
		}
		else if (name.equals("$masked")) {
			this.masked = Boolean.parseBoolean(values.get(0));
		}
		else if (name.equals("$editable")) {
			this.editable = Boolean.parseBoolean(values.get(0));
		}
		else if (name.equals("$allowed")) {
			this.allowed = new ArrayList<String>(values);
		}
		else if (name.equals("$required")) {
			this.required = Boolean.parseBoolean(values.get(0));
		}
	}

	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getDisplayName() {
		return displayName;
	}
	public boolean isAdvanced() {
		return advanced;
	}
	public boolean isMasked() {
		return masked;
	}
	public boolean isEditable() {
		return editable;
	}
	public boolean isRequired() {
		return required;
	}
	public ArrayList<String> getAllowed() {
		return allowed;
	}
}
