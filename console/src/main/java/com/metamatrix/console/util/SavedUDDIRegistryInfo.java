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

package com.metamatrix.console.util;


/** 
 * Data class to represent the info on UDDI registy that is saved to the properties file.
 * 
 * @since 4.2
 */
public class SavedUDDIRegistryInfo {
    private String name;
    private String userName;
    private String host;
    private String port;
    
    public SavedUDDIRegistryInfo(String name, String userName, String host, String port) {
        super();
        this.name = name;
        this.userName = userName;
        this.host = host;
        this.port = port;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public String getUserName() {
        return userName;
    }
    
    public void setUserName(String userName) {
        this.userName = userName;
    }
    
    public String getHost() {
        return host;
    }
    
    public void setHost(String host) {
        this.host = host;
    }
    
    public String getPort() {
        return port;
    }
    
    public void setPort(String port) {
        this.port = port;
    }
    
    public boolean equals(Object obj) {
        boolean same;
        if (obj == this) {
            same = true;
        } else if (!(obj instanceof SavedUDDIRegistryInfo)) {
            same = false;
        } else {
            SavedUDDIRegistryInfo that = (SavedUDDIRegistryInfo)obj;
            boolean nameMatches;
            boolean userNameMatches = false;
            boolean hostMatches = false;
            boolean portMatches = false;
            String thatName = that.getName();
            if (name == null) {
                nameMatches = (thatName == null);
            } else {
                nameMatches = name.equals(thatName);
            }
            if (nameMatches) {
                String thatUserName = that.getUserName();
                if (userName == null) {
                    userNameMatches = (thatUserName == null);
                } else {
                    userNameMatches = userName.equals(thatUserName);
                }
                if (userNameMatches) {
                    String thatHost = that.getHost();
                    if (host == null) {
                        hostMatches = (thatHost == null);
                    } else {
                        hostMatches = host.equals(thatHost);
                    }
                    if (hostMatches) {
                        String thatPort = that.getPort();
                        if (port == null) {
                            portMatches = (thatPort == null);
                        } else {
                            portMatches = port.equals(thatPort);
                        }
                    }
                }
            }
            same = (nameMatches && userNameMatches && hostMatches && portMatches);
        }
        return same;
    }
}
