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
 * Simple URL discovery strategy with a random load balancing policy
 * TOOD: add black listing support
 */
public class UrlServerDiscovery implements ServerDiscovery {

	private TeiidURL url;
	
	public UrlServerDiscovery() {
	}
	
	public UrlServerDiscovery(TeiidURL url) {
		this.url = url;
	}
	
	@Override
	public List<HostInfo> getKnownHosts(LogonResult result,
			SocketServerInstance instance) {
		return url.getHostInfo();
	}

	@Override
	public void init(TeiidURL url, Properties p) {
		this.url = url;
	}
	
	@Override
	public void connectionSuccessful(HostInfo info) {
		
	}

	@Override
	public void markInstanceAsBad(HostInfo info) {
		
	}
		
	@Override
	public void shutdown() {
		
	}
	
	@Override
	public HostInfo selectNextInstance(List<HostInfo> hosts) {
		return hosts.remove((int) (Math.random() * hosts.size()));
	}
	
}
