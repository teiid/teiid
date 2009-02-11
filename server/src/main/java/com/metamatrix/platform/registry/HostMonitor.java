package com.metamatrix.platform.registry;

import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class HostMonitor {
	
	ClusteredRegistryState registry;
	
	@Inject
	public HostMonitor(ClusteredRegistryState registry) {
		this.registry = registry;
	}
	
	public void hostAdded(HostControllerRegistryBinding binding) {
		this.registry.addHost(binding);
	}
	
	public void hostRemoved(String hostName) {
		this.registry.removeHost(hostName);
	}
}
