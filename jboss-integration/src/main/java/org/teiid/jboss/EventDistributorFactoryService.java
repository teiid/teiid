/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.teiid.jboss;

import java.util.concurrent.Executor;

import org.jboss.as.controller.ModelController;
import org.jboss.as.server.moduleservice.ServiceModuleLoader;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.jboss.AdminFactory;
import org.teiid.adminapi.jboss.AdminFactory.AdminImpl;
import org.teiid.deployers.CombinedClassLoader;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.metadata.MetadataException;
import org.teiid.query.ObjectReplicator;
import org.teiid.services.AbstractEventDistributorFactoryService;
import org.teiid.services.InternalEventDistributorFactory;

public class EventDistributorFactoryService extends AbstractEventDistributorFactoryService implements Service<InternalEventDistributorFactory> {
	
	InjectedValue<ObjectReplicator> objectReplicatorInjector = new InjectedValue<ObjectReplicator>();
	InjectedValue<VDBRepository> vdbRepositoryInjector = new InjectedValue<VDBRepository>();
    protected final InjectedValue<Executor> executorInjector = new InjectedValue<Executor>();
    protected final InjectedValue<ModelController> controllerValueInjector = new InjectedValue<ModelController>();
	protected final InjectedValue<ServiceModuleLoader> serviceModuleLoader = new InjectedValue<ServiceModuleLoader>();
	
	private ConnectorManagerRepository cmr;
	
	public EventDistributorFactoryService(ConnectorManagerRepository cmr) {
	    this.cmr = cmr;
	}
	
	@Override
	public void start(StartContext context) throws StartException {
		start();
	}

	@Override
	public void stop(StopContext context) {
		stop();
	}
	
	@Override
	protected ObjectReplicator getObjectReplicator() {
		return objectReplicatorInjector.getValue();
	}
	
	@Override
	protected VDBRepository getVdbRepository() {
		return vdbRepositoryInjector.getValue();
	}

    @Override
    protected ConnectorManagerRepository getConnectorManagerRepository() {
        return cmr;
    }

    @Override
    protected Admin getAdmin() {
        Admin admin = (AdminImpl) AdminFactory.getInstance()
                .createAdmin(controllerValueInjector.getValue().createClient(executorInjector.getValue()));
        return admin;
    }

    @Override
    protected ClassLoader getClassLoader(String[] paths) throws MetadataException {
        if (paths.length == 1) {
            return getClassLoader(paths[0]);
        }
        ClassLoader cl[] = new ClassLoader[paths.length];
        for (int i = 0; i< paths.length; i++) {
            cl[i] = getClassLoader(paths[i]);
        }
        return new CombinedClassLoader(cl[0], cl);
    }
    
    private ClassLoader getClassLoader(String path) {
        int index = path.indexOf(':');
        String moduleName = path;
        String slot = null;
        if (index != -1) {
            moduleName = path.substring(0,index);
            slot = path.substring(index);
        }
        
        ModuleIdentifier lib = ModuleIdentifier.create(moduleName);
        if (slot != null) {
            lib = ModuleIdentifier.create(moduleName, slot);
        }
        
        try {
            ModuleLoader moduleLoader = Module.getCallerModuleLoader();
            final Module module = moduleLoader.loadModule(lib);
            return module.getClassLoader();
        } catch (ModuleLoadException e) {
            try {                
                Module module = serviceModuleLoader.getValue().loadModule(lib);
                return module.getClassLoader();
            } catch (ModuleLoadException e1) {
                throw new MetadataException(e);                  
            }
        }        
    }
}
