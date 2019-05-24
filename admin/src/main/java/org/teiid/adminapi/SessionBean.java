/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.adminapi;

public interface SessionBean {

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
     * <br>It will not include the Security Domain, see {@link #getSecurityDomain()}
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
    public String getVDBVersion();

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
