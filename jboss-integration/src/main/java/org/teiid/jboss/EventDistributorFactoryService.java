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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.teiid.deployers.EventDistributorImpl;
import org.teiid.deployers.VDBRepository;
import org.teiid.events.EventDistributor;
import org.teiid.events.EventDistributorFactory;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.ObjectReplicator;
import org.teiid.transport.LocalServerConnection;

public class EventDistributorFactoryService implements Service<EventDistributorFactory>, EventDistributorFactory {
	
	InjectedValue<ObjectReplicator> objectReplicatorInjector = new InjectedValue<ObjectReplicator>();
	InjectedValue<VDBRepository> vdbRepositoryInjector = new InjectedValue<VDBRepository>();
	private EventDistributor replicatableEventDistributor;
	private EventDistributor eventDistributorProxy;
	
	@Override
	public EventDistributorFactory getValue() throws IllegalStateException, IllegalArgumentException {
		return new EventDistributorFactory() {
			@Override
			public EventDistributor getEventDistributor() {
				return replicatableEventDistributor;
			}
		};
	}

	@Override
	public void start(StartContext context) throws StartException {
		final EventDistributor ed = new EventDistributorImpl() {
			@Override
			public VDBRepository getVdbRepository() {
				return vdbRepositoryInjector.getValue();
			}
		};
		
		// this instance is by use of teiid internally; only invokes the remote instances
		if (objectReplicatorInjector.getValue() != null) {
			try {
				this.replicatableEventDistributor = objectReplicatorInjector.getValue().replicate(LocalServerConnection.TEIID_RUNTIME_CONTEXT, EventDistributor.class, ed, 0);
			} catch (Exception e) {
				LogManager.logError(LogConstants.CTX_RUNTIME, e, IntegrationPlugin.Util.getString("replication_failed", this)); //$NON-NLS-1$
			}
			LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("distributed_cache_enabled")); //$NON-NLS-1$
		}
		else {
			LogManager.logDetail(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("distributed_cache_not_enabled")); //$NON-NLS-1$
		}
		
		// for external client to call. invokes local instance and remote ones too.
		this.eventDistributorProxy = (EventDistributor)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {EventDistributor.class}, new InvocationHandler() {
			
			@Override
			public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
				method.invoke(ed, args);
				if (replicatableEventDistributor != null) {
					method.invoke(replicatableEventDistributor, args);
				}
				return null;
			}
		});		
	}

	@Override
	public void stop(StopContext context) {
    	if (objectReplicatorInjector.getValue() != null && this.replicatableEventDistributor != null) {
    		objectReplicatorInjector.getValue().stop(this.replicatableEventDistributor);
    		this.replicatableEventDistributor = null;
    	}
	}

	@Override
	public org.teiid.events.EventDistributor getEventDistributor() {
		return eventDistributorProxy;
	}
}
