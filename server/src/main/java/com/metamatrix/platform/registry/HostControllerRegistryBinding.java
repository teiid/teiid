package com.metamatrix.platform.registry;

import java.io.Serializable;
import java.util.Properties;

import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.server.HostManagement;
import com.metamatrix.server.ResourceFinder;

public class HostControllerRegistryBinding implements Serializable {

	private transient HostManagement hostController;
    private transient MessageBus messageBus;
    
    /** remote reference */
    private Object hostControllerStub;    
    
    private String hostName;
    
    private Properties hostProperties;
    
    
    public HostControllerRegistryBinding(String hostName, Properties properties, HostManagement controller, MessageBus bus) {
    	this.messageBus = bus;	
    	this.hostName = hostName;
    	this.hostProperties = properties;
    	setHostController(controller);
    }
    
    public String getHostName() {
    	return this.hostName;
    }
    
	private synchronized void setHostController(HostManagement controller) {
		this.hostController = controller;
    	if (this.hostControllerStub != null) {
    		this.messageBus.unExport(hostControllerStub);
    		hostControllerStub = null;
    	}
        if (controller != null) {
            this.hostControllerStub = this.messageBus.export(controller, controller.getClass().getInterfaces());
		}
	}    
	
    public synchronized HostManagement getHostController() {
    	if (this.hostController != null) {
    		return hostController;
    	}
    	if (this.hostControllerStub == null) {
    		return null;
    	}
    	// when exported to the remote, use remote's message bus instance.
    	MessageBus bus = this.messageBus;
    	if(bus == null) {
    		bus = ResourceFinder.getMessageBus();
    	}
    	this.hostController = (HostManagement)bus.getRPCProxy(this.hostControllerStub);
    	return this.hostController;
    }	
    
    public Properties getProperties() {
    	return new Properties(this.hostProperties);
    }
}
