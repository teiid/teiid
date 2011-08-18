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

package org.teiid.adminapi;


/**
 * A Session represents a single connection between a client and the server.
 * 
 * A user is allowed to have multiple sessions active simultaneously.
 */
public interface Session extends AdminObject {
    
    /**
     * Get the Last time Client has check to see if the server is still available
     * 
     * @return Date of the last ping to the server.
     */
    public long getLastPingTime();
    

    /**
     * Get the Application Name
     * 
     * @return String of the Application Name
     */
    public String getApplicationName();

    /**
     * Get the unique Teiid session
     * within a given Teiid System
     * 
     * @return String of the Session ID
     */
    public String getSessionId();

    /**
     * Get User Name for this Session
     * 
     * @return String of UserName
     */
    public String getUserName();

    /**
     * Get the VDB Name for this Session
     * 
     * @return String name of the VDB
     */
    public String getVDBName();

    /**
     * Get the VDB Version for this Session
     * 
     * @return String name/number of the VDB Version
     */
    public int getVDBVersion();
    
    /**
     * Get the IPAddress for this Session.  Note this value is reported from the client.
     * @return
     */
    public String getIPAddress();
      
 
    /**
     * Get the host name of the machine the client is 
     * accessing from.  Note this value is reported from the client.
     * @return 
     */
    public String getClientHostName();
    
    /**
     * Get the client hardware (typically MAC) address. Note this value is reported from the client.
     * @return the hardware address as a hex string or null if not available.
     */
    public String getClientHardwareAddress();
    
    /**
     * Get the time the {@link Session} was created.
     * @return
     */
    public long getCreatedTime();

    
    /**
     * Security Domain user logged into currently
     * @return
     */
    public String getSecurityDomain();

}