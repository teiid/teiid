package com.metamatrix.platform.registry;

import java.util.Properties;

import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.server.HostManagement;

public class HostControllerRegistryBinding extends RegistryBinding<HostManagement> {

    private Properties hostProperties;
    
    public HostControllerRegistryBinding(String hostName, Properties properties, HostManagement controller, MessageBus bus) {
    	super(controller, hostName, bus);
    	this.hostProperties = properties;
    }
    
    @Override
    public synchronized HostManagement getBindObject() {
    	HostManagement bindObject = super.getBindObject();
    	if (bindObject == null) {
    		throw new IllegalStateException("Cannot locate host controller.  It may need to be started or restarted if jgroups properties have changed"); //$NON-NLS-1$
    	}
    	return bindObject;
    }
        
    public Properties getProperties() {
    	Properties p =  new Properties();
    	p.putAll(this.hostProperties);
    	return p;
    }
    
    public HostManagement getHostController() {
    	return getBindObject();
    }
}
