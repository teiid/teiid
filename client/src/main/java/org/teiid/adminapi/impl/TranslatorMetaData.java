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
package org.teiid.adminapi.impl;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

import org.jboss.managed.api.annotation.ManagementComponent;
import org.jboss.managed.api.annotation.ManagementObject;
import org.jboss.managed.api.annotation.ManagementObjectID;
import org.jboss.managed.api.annotation.ManagementProperties;
import org.jboss.managed.api.annotation.ManagementProperty;
import org.jboss.managed.api.annotation.ManagementPropertyFactory;
import org.teiid.adminapi.Translator;

@ManagementObject(componentType=@ManagementComponent(type="teiid",subtype="translator"), properties=ManagementProperties.EXPLICIT)
@XmlAccessorType(XmlAccessType.NONE)
public class TranslatorMetaData extends AdminObjectImpl implements Translator {

	private static final long serialVersionUID = 1680003620274793056L;
	public static final String EXECUTION_FACTORY_CLASS = "execution-factory-class"; //$NON-NLS-1$
	public static final String IMMUTABLE = "immutable"; //$NON-NLS-1$
	public static final String EXCEPTION_ON_MAX_ROWS = "exception-on-max-rows"; //$NON-NLS-1$
	public static final String MAX_RESULT_ROWS = "max-result-rows"; //$NON-NLS-1$
	public static final String XA_CAPABLE = "xa-capable"; //$NON-NLS-1$
	public static final String NAME = "name"; //$NON-NLS-1$
	public static final String TEMPLATE_NAME = "template-name"; //$NON-NLS-1$
	
	// objects are used to keep the jaxb putting verbose xml elements when they are not defined.
	private String executionFactoryClass;
	private boolean immutable = false;
	private boolean exceptionOnMaxRows = false;
	private int maxResultRows = -1;
	private boolean xaCapable = false;
	private String templateName;
	
	@Override
	@ManagementProperty(name="name", description="Name of the Translator", mandatory = true)
	@ManagementObjectID(type="translator")
	public String getName() {
		return super.getName();
	}	
	
	@XmlElement(name = NAME)
	public void setName(String name) {
		super.setName(name);
	}
	
	@Override
	@ManagementProperty(name=EXECUTION_FACTORY_CLASS, description="Execution Factory Class", mandatory = true)
	public String getExecutionFactoryClass() {
		return executionFactoryClass;
	}	
	
	@XmlElement(name = EXECUTION_FACTORY_CLASS)
	public void setExecutionFactoryClass(String arg0) {
		this.executionFactoryClass = arg0;
	}
	
	@Override
	@ManagementProperty(name=IMMUTABLE, description="Is Immutable, True if the source never changes.",mandatory=true, defaultValue="false")
	public boolean isImmutable() {
		return immutable;
	}

	@XmlElement(name = IMMUTABLE)
	public void setImmutable(boolean arg0) {
		this.immutable = arg0;
	}	
	
	@Override
	@ManagementProperty(name=EXCEPTION_ON_MAX_ROWS, description="Indicates if an Exception should be thrown if the specified value for Maximum Result Rows is exceeded; else no exception and no more than the maximum will be returned",mandatory=true)
	public boolean isExceptionOnMaxRows() {
		return exceptionOnMaxRows;
	}

	@XmlElement(name = EXCEPTION_ON_MAX_ROWS)
	public void setExceptionOnMaxRows(boolean arg0) {
		this.exceptionOnMaxRows = arg0;
	}
	
	@Override
	@ManagementProperty(name=MAX_RESULT_ROWS, description="Maximum Result Rows allowed", mandatory=true)
	public int getMaxResultRows() {
		return maxResultRows;
	}
	
	@XmlElement(name = MAX_RESULT_ROWS)
	public void setMaxResultRows(int arg0) {
		this.maxResultRows = arg0;
	}	
	
	@Override
	@ManagementProperty(name=XA_CAPABLE, description="True, if this connector supports XA Transactions")
	public boolean isXaCapable() {
		return xaCapable;
	}

	@XmlElement(name=XA_CAPABLE)
	public void setXaCapable(boolean arg0) {
		this.xaCapable = arg0;
	}	
	
	@Override
	@XmlElement(name = "translator-property", type = PropertyMetadata.class)
	@ManagementProperty(name="translator-property", description = "Translator Properties", managed=true)
	@ManagementPropertyFactory(TranslatorPropertyFactory.class)
	public List<PropertyMetadata> getJAXBProperties(){
		return super.getJAXBProperties();
	}	
	
	@Override
	@ManagementProperty(name=TEMPLATE_NAME, description = "Template with which this Translator is created")	
	public String getTemplateName() {
		return templateName;
	}

	@XmlElement(name = TEMPLATE_NAME)
	public void setTemplateName(String templateName) {
		this.templateName = templateName;
	}	
	
	public String toString() {
		return getName();
	}
}
