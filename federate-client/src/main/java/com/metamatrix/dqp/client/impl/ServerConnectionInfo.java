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

package com.metamatrix.dqp.client.impl;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.dqp.client.ConnectionInfo;
import com.metamatrix.jdbc.api.ConnectionProperties;


/** 
 * @since 4.2
 */
public class ServerConnectionInfo implements ConnectionInfo {
    
    private static final String HEADER = "ServerConnectionInfo:"; //$NON-NLS-1$
    
    private String url;
    private String user;
    private String password;
    private Serializable trustedPayload;
    private String vdbName;
    private String vdbVersion;
    private Map optionalProperties;
    
    public ServerConnectionInfo() {
        
    }
    
    ServerConnectionInfo(String portableString) throws MetaMatrixProcessingException {
        String[] parts = PortableStringUtil.getParts(portableString, PortableStringUtil.PROP_SEPARATOR);
        if (parts == null || parts.length < 4 || parts.length > 7 || !parts[0].startsWith(HEADER)) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ServerConnectionInfo.invalid_context", portableString)); //$NON-NLS-1$
        }
        parts[0] = parts[0].substring(HEADER.length());
        this.url = PortableStringUtil.unescapeString(PortableStringUtil.getParts(parts[0], PortableStringUtil.EQUALS)[1]);
        this.user = PortableStringUtil.unescapeString(PortableStringUtil.getParts(parts[1], PortableStringUtil.EQUALS)[1]);
        this.password = PortableStringUtil.unescapeString(PortableStringUtil.getParts(parts[2], PortableStringUtil.EQUALS)[1]);
        this.vdbName = PortableStringUtil.unescapeString(PortableStringUtil.getParts(parts[3], PortableStringUtil.EQUALS)[1]);
        for (int i = 4; i < parts.length; i++) {
            String[] propValPair = PortableStringUtil.getParts(parts[i], PortableStringUtil.EQUALS);
            try {
                if (propValPair[0].equals(ConnectionProperties.VDB_VERSION)) {
                    this.vdbVersion = PortableStringUtil.unescapeString(propValPair[1]);
                } else if (propValPair[0].equals(ConnectionProperties.TRUSTED_PAYLOAD_PROP)) {
                    this.trustedPayload = (Serializable)PortableStringUtil.decode(propValPair[1]);
                } else if (propValPair[0].equals("optionalProperties")) { //$NON-NLS-1$
                    this.optionalProperties = (Map)PortableStringUtil.decode(propValPair[1]);
                }
            } catch (Exception e) {
                throw new MetaMatrixProcessingException(e, AdminPlugin.Util.getString("ServerConnectionInfo.invalid_encoding", propValPair[1])); //$NON-NLS-1$
            }
        }
    }

    String getServerUrl() {
        return url;
    }

    /** 
     * @see com.metamatrix.dqp.client.ConnectionInfo#setServerUrl(java.lang.String)
     * @since 4.2
     */
    public void setServerUrl(String url) {
        this.url = url;
    }
    
    String getUser() {
        return user;
    }

    /** 
     * @see com.metamatrix.dqp.client.ConnectionInfo#setUser(java.lang.String)
     * @since 4.2
     */
    public void setUser(String user) {
        this.user = user;
    }

    String getPassword() {
        return password;
    }
    /** 
     * @see com.metamatrix.dqp.client.ConnectionInfo#setPassword(java.lang.String)
     * @since 4.2
     */
    public void setPassword(String password) {
        this.password = password;
    }
    
    Serializable getTrustedPayload() {
        return trustedPayload;
    }

    /** 
     * @see com.metamatrix.dqp.client.ConnectionInfo#setTrustedPayload(java.io.Serializable)
     * @since 4.2
     */
    public void setTrustedPayload(Serializable trustedPayload) {
        this.trustedPayload = trustedPayload;
    }

    String getVDBName() {
        return vdbName;
    }
    /** 
     * @see com.metamatrix.dqp.client.ConnectionInfo#setVDBName(java.lang.String)
     * @since 4.2
     */
    public void setVDBName(String vdbName) {
        this.vdbName = vdbName;
    }
    
    String getVDBVersion() {
        return vdbVersion;
    }

    /** 
     * @see com.metamatrix.dqp.client.ConnectionInfo#setVDBVersion(java.lang.String)
     * @since 4.2
     */
    public void setVDBVersion(String vdbVersion) {
        if (vdbVersion != null && vdbVersion.length() != 0) {
            this.vdbVersion = vdbVersion;
        } else {
            this.vdbVersion = null;
        }
    }

    Map getOptionalProperties() {
        return optionalProperties;
    }
    /** 
     * @see com.metamatrix.dqp.client.ConnectionInfo#setOptionalProperty(java.lang.String, java.lang.Object)
     * @since 4.2
     */
    public void setOptionalProperty(String propName, Object propValue) {
        if (optionalProperties == null) {
            optionalProperties = new HashMap();
        }
        optionalProperties.put(propName, propValue);
    }
    
    Properties getConnectionProperties() {
        Properties props = new Properties();
        props.setProperty(ConnectionProperties.SERVER_URL, url);
        props.setProperty(ConnectionProperties.USER_PROP, user);
        props.setProperty(ConnectionProperties.PWD_PROP, password);
        props.setProperty(ConnectionProperties.VDB_NAME, vdbName);
        props.setProperty(ConnectionProperties.VDB_NAME_DQP, vdbName);
        if (vdbVersion != null && vdbVersion.length() != 0) {
            props.setProperty(ConnectionProperties.VDB_VERSION, vdbVersion);
            props.setProperty(ConnectionProperties.VDB_VERSION_DQP, vdbVersion);
        }

        setHostAndPort(props, url);
        
        if (trustedPayload != null) {
            props.put(ConnectionProperties.TRUSTED_PAYLOAD_PROP, trustedPayload);
        }
        if (optionalProperties != null && !optionalProperties.isEmpty()) {
            props.putAll(optionalProperties);
        }
        return props;
    }

    public int hashCode() {
        int hash = HashCodeUtil.hashCode(0, url);
        hash = HashCodeUtil.hashCode(hash, user);
        hash = HashCodeUtil.hashCode(hash, password);
        hash = HashCodeUtil.hashCode(hash, vdbName);
        if (vdbVersion != null && vdbVersion.length() != 0) {
            return HashCodeUtil.hashCode(hash, vdbVersion);
        }
        return hash;
    }
    
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof ServerConnectionInfo)) {
            return false;
        }
        ServerConnectionInfo info = (ServerConnectionInfo)obj;
        return EquivalenceUtil.areEqual(this.url, info.url) &&
                EquivalenceUtil.areEqual(this.user, info.user) &&
                EquivalenceUtil.areEqual(this.password, info.password) &&
                EquivalenceUtil.areEqual(this.vdbName, info.vdbName) &&
                EquivalenceUtil.areEqual(this.vdbVersion, info.vdbVersion) &&
                EquivalenceUtil.areEqual(this.trustedPayload, info.trustedPayload) &&
                EquivalenceUtil.areEqual(this.optionalProperties, info.optionalProperties);
    }
    
    String getPortableString() {
        StringBuffer buf = new StringBuffer(HEADER)
        .append(ConnectionProperties.SERVER_URL).append(PortableStringUtil.EQUALS).append(PortableStringUtil.escapeString(url)).append(PortableStringUtil.PROP_SEPARATOR)
        .append(ConnectionProperties.USER_PROP).append(PortableStringUtil.EQUALS).append(PortableStringUtil.escapeString(user)).append(PortableStringUtil.PROP_SEPARATOR)
        .append(ConnectionProperties.PWD_PROP).append(PortableStringUtil.EQUALS).append(PortableStringUtil.escapeString(password)).append(PortableStringUtil.PROP_SEPARATOR)
        .append(ConnectionProperties.VDB_NAME).append(PortableStringUtil.EQUALS).append(PortableStringUtil.escapeString(vdbName));
        if (vdbVersion != null && vdbVersion.length() != 0) {
            buf.append(PortableStringUtil.PROP_SEPARATOR).append(ConnectionProperties.VDB_VERSION).append(PortableStringUtil.EQUALS).append(PortableStringUtil.escapeString(vdbVersion));
        }
        if (trustedPayload != null) {
            try {
                String encodedString = PortableStringUtil.encode(trustedPayload);
                buf.append(PortableStringUtil.PROP_SEPARATOR).append(ConnectionProperties.TRUSTED_PAYLOAD_PROP).append(PortableStringUtil.EQUALS).append(encodedString);
            } catch (IOException e) {
                // TODO warn
            }
        }
        if (optionalProperties != null && !optionalProperties.isEmpty()) {
            try {
                String encodedString = PortableStringUtil.encode(optionalProperties);
                buf.append(PortableStringUtil.PROP_SEPARATOR).append("optionalProperties").append(PortableStringUtil.EQUALS).append(encodedString); //$NON-NLS-1$
            } catch (IOException e) {
                // TODO warn
            }
        }
        return buf.toString();
    }
    
    private void setHostAndPort(Properties props, String url) {
        String[] parts = PortableStringUtil.getParts(url, ':');
        if (parts == null || parts.length != 3 || !parts[1].startsWith("//")) { //$NON-NLS-1$
            throw new IllegalArgumentException(AdminPlugin.Util.getString("ServerConnectionInfo.invalid_url")); //$NON-NLS-1$
        }
        parts[1] = parts[1].substring(2);
        props.setProperty(ConnectionProperties.HOST, parts[1]);
        props.setProperty(ConnectionProperties.PORT, parts[2]);
    }
}
