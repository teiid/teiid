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
package com.metamatrix.connector.salesforce.execution.visitors;

import java.util.ArrayList;
import java.util.List;

import javax.xml.namespace.QName;

import org.apache.axis.message.MessageElement;

import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.language.IElement;
import com.metamatrix.connector.language.IInsert;
import com.metamatrix.connector.language.ILiteral;
import com.metamatrix.connector.metadata.runtime.Element;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;

public class InsertVisitor extends CriteriaVisitor {

	List<ConnectorException> exceptions = new ArrayList<ConnectorException>();
	List<MessageElement> elements = new ArrayList<MessageElement>();
	
	public InsertVisitor(RuntimeMetadata metadata) {
		super(metadata);
	}
	
	@Override
	public void visit(IInsert insert) {
		super.visit(insert);
		try {
			loadColumnMetadata(insert.getGroup());
			
			List<IElement> columns = insert.getElements();
			List<Object> values = insert.getValues();
			if(columns.size() != values.size()) {
				throw new ConnectorException("Error:  columns.size and values.size are not the same.");
			}
			
			for(int i = 0; i < columns.size(); i++) {
				IElement element = columns.get(i);
				Element column = (Element)metadata.getObject(element.getMetadataID());
				Object value = values.get(i);
				String val;
				if(value instanceof ILiteral) {
					ILiteral literalValue = (ILiteral)value;
					val = this.stripQutes((String)literalValue.getValue());
				} else {
					val = value.toString();
				}
				
				MessageElement messageElem = new MessageElement(
						new QName(column.getNameInSource()), val);
				elements.add(messageElem);
			}
			
		} catch (ConnectorException ce) {
			exceptions.add(ce);
		}
	}

	public MessageElement[] getMessageElements() {
		return elements.toArray(new MessageElement[0]);
	}
	
	private String stripQutes(String id) {
		if((id.startsWith("'") && id.endsWith("'"))) {
			id = id.substring(1,id.length()-1);
		} else if ((id.startsWith("\"") && id.endsWith("\""))) {
			id = id.substring(1,id.length()-1);
		}
		return id;
	}
}
