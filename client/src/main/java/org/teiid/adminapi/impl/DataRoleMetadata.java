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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.jboss.managed.api.annotation.ManagementObject;


/**
 * <pre>
 * &lt;complexType>
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="description" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="resource-name" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="allow-create" type="{http://www.w3.org/2001/XMLSchema}boolean" minOccurs="0"/>
 *         &lt;element name="allow-read" type="{http://www.w3.org/2001/XMLSchema}boolean" minOccurs="0"/>
 *         &lt;element name="allow-update" type="{http://www.w3.org/2001/XMLSchema}boolean" minOccurs="0"/>
 *         &lt;element name="allow-delete" type="{http://www.w3.org/2001/XMLSchema}boolean" minOccurs="0"/>
 *       &lt;/sequence>
 *       &lt;attribute name="name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "description",
    "resourceName",
    "allowCreate",
    "allowRead",
    "allowUpdate",
    "allowDelete"
})
@ManagementObject
public class DataRoleMetadata implements Serializable {

    @XmlAttribute(name = "name", required = true)
    protected String name;
	@XmlElement(name = "description")
    protected String description;
    @XmlElement(name = "resource-name", required = true)
    protected String resourceName;
    @XmlElement(name = "allow-create")
    protected Boolean allowCreate;
    @XmlElement(name = "allow-read")
    protected Boolean allowRead;
    @XmlElement(name = "allow-update")
    protected Boolean allowUpdate;
    @XmlElement(name = "allow-delete")
    protected Boolean allowDelete;

    public String getName() {
        return name;
    }

    public void setName(String value) {
        this.name = value;
    }
    
    public String getDescription() {
        return description;
    }

    public void setDescription(String value) {
        this.description = value;
    }

    public String getResourceName() {
        return resourceName;
    }

    public void setResourceName(String value) {
        this.resourceName = value;
    }

    public Boolean isAllowCreate() {
        return allowCreate;
    }

    public void setAllowCreate(Boolean value) {
        this.allowCreate = value;
    }

    public Boolean isAllowRead() {
        return allowRead;
    }

    public void setAllowRead(Boolean value) {
        this.allowRead = value;
    }

    public Boolean isAllowUpdate() {
        return allowUpdate;
    }

    public void setAllowUpdate(Boolean value) {
        this.allowUpdate = value;
    }

    public Boolean isAllowDelete() {
        return allowDelete;
    }

    public void setAllowDelete(Boolean value) {
        this.allowDelete = value;
    }
}
