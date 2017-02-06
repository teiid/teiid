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
package org.teiid.metadata;

/**
 * Represents a Server and its properties. Distinction is this is NOT connection,
 * you can create connections to Server.
 */
public class Server extends AbstractMetadataRecord {
    private static final long serialVersionUID = -3969389574210542638L;
    private String type;
    private String version;
    private String dataWrapperName;
    
    public Server(String name) {
        super.setName(name);
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getDataWrapper() {
        return dataWrapperName;
    }

    public void setDataWrapper(String wrapperName) {     
        this.dataWrapperName = wrapperName;
    }
    
    public String getJndiName() {
        return getProperty("jndi-name", false);//$NON-NLS-1$
    }
    
    public void setJndiName(String value) {
        setProperty("jndi-name", value); //$NON-NLS-1$
    }

	public boolean isVirtual() {
		return type == null;
	}
}
