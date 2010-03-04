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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.jboss.managed.api.annotation.ManagementObject;
import org.jboss.managed.api.annotation.ManagementProperty;


/**
 * <pre>
 * &lt;complexType name="reference-mapping">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="ref-name" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="resource-name" type="{http://www.w3.org/2001/XMLSchema}string" maxOccurs="unbounded"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.NONE)
@XmlType(name = "reference-mapping", propOrder = {
    "refName",
    "resourceNames"
})
@ManagementObject
public class ReferenceMappingMetadata implements Serializable {

    @XmlElement(name = "ref-name", required = true)
    protected String refName;
 
    @XmlElement(name = "resource-name", required = true)
    protected List<String> resourceNames;

    public ReferenceMappingMetadata() {
    }
    
    public ReferenceMappingMetadata(String refName, String resourceName) {
    	setRefName(refName);
    	addResourceName(resourceName);
    }
    
    public ReferenceMappingMetadata(String refName, List<String> resourceNames) {
    	setRefName(refName);
    	this.resourceNames = new ArrayList<String>(resourceNames);
    }    
    
    @ManagementProperty(description="Reference Name", readOnly=true)
    public String getRefName() {
        return refName;
    }

    public void setRefName(String value) {
        this.refName = value;
    }

    @ManagementProperty(description="Resource Names")
    public List<String> getResourceNames() {
        if (this.resourceNames == null) {
            this.resourceNames = new ArrayList<String>();
        }
        return this.resourceNames;
    }
    
    public void setResourceNames(List<String> names) {
    	this.resourceNames = new ArrayList<String>(names);
    }
    
    public void addResourceName(String name) {
    	getResourceNames().add(name);
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((refName == null) ? 0 : refName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ReferenceMappingMetadata other = (ReferenceMappingMetadata) obj;
		if (refName == null) {
			if (other.refName != null)
				return false;
		} else if (!refName.equals(other.refName))
			return false;
		return true;
	}
    
  
}
