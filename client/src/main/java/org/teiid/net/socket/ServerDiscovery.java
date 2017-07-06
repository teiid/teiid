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

package org.teiid.net.socket;

import java.util.List;
import java.util.Properties;

import org.teiid.client.security.LogonResult;
import org.teiid.net.HostInfo;
import org.teiid.net.TeiidURL;



/**
 * Customizable ServerDiscovery interface
 */
public interface ServerDiscovery {
	
	/**
	 * Initialize the {@link ServerDiscovery}
	 * @param url
	 * @param p
	 */
	void init(TeiidURL url, Properties p);
	
	/**
	 * Get the currently known hosts.  Will be called prior to connecting and after
	 * authentication for each connection. 
	 * @param result, the current {@link LogonResult} - may be null if unauthenticated 
	 * @param instance, the currently connected instance - may be null if not connected
	 * @return
	 */
	List<HostInfo> getKnownHosts(LogonResult result, SocketServerInstance instance);
	
	/**
	 * Indicates that a connection was made successfully to the given host.
	 * @param info
	 */
	void connectionSuccessful(HostInfo info);
	
	/**
	 * Indicates that a connection could not be made to the given host.
	 * @param info
	 */
	void markInstanceAsBad(HostInfo info);
	
	/**
	 * Shutdown this {@link ServerDiscovery}
	 */
	void shutdown();
	
	/**
	 * Select the next instance to try.  The entry should be removed from the list
	 * when no more attempts are desired.  
	 * and not return null.
	 * @param hosts
	 * @return a non-null HostInfo to try
	 */
	HostInfo selectNextInstance(List<HostInfo> hosts);
		
}
