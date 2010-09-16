/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
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
package org.teiid.adminapi.impl;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

import org.jboss.managed.api.annotation.ManagementObject;
import org.jboss.managed.api.annotation.ManagementObjectID;
import org.jboss.managed.api.annotation.ManagementProperties;
import org.jboss.managed.api.annotation.ManagementProperty;

@XmlAccessorType(XmlAccessType.NONE)
@XmlType(name = "")
@ManagementObject(properties=ManagementProperties.EXPLICIT)
public class SourceMappingMetadata implements Serializable {
	private static final long serialVersionUID = -4417878417697685794L;

	@XmlAttribute(name = "name", required = true)
    private String name;
    
    @XmlAttribute(name = "connection-jndi-name")
    private String jndiName;
    
    @XmlAttribute(name = "translator-name",required = true)
    private String translatorName;
    
	public SourceMappingMetadata() {}
    
    public SourceMappingMetadata(String name, String translatorName, String connJndiName) {
    	this.name = name;
    	this.translatorName = translatorName;
    	this.jndiName = connJndiName;
    }

    @ManagementProperty (description="Source Name")
    @ManagementObjectID(type="source")
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@ManagementProperty (description="JNDI Name of the resource to assosiate with Source name")
	public String getConnectionJndiName() {
		// this default could be controlled if needed.
		if (this.jndiName == null) {
			return "java:"+name; //$NON-NLS-1$
		}
		return jndiName;
	}

	public void setConnectionJndiName(String jndiName) {
		this.jndiName = jndiName;
	}
	
	@ManagementProperty (description="Translator Name")
    public String getTranslatorName() {
		return translatorName;
	}

	public void setTranslatorName(String translatorName) {
		this.translatorName = translatorName;
	}	
	
	public String toString() {
		return getName()+"/"+getTranslatorName()+"/"+getConnectionJndiName(); //$NON-NLS-1$ //$NON-NLS-2$
	}
}