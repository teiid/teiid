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
package org.teiid.jboss;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;
import static org.teiid.jboss.TeiidConstants.*;

import java.util.List;

import javax.naming.InitialContext;

import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.as.naming.ManagedReferenceFactory;
import org.jboss.as.naming.ServiceBasedNamingStore;
import org.jboss.as.naming.deployment.ContextNames;
import org.jboss.as.naming.service.BinderService;
import org.jboss.as.network.SocketBinding;
import org.jboss.as.security.plugins.SecurityDomainContext;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.inject.ConcurrentMapInjector;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceTarget;
import org.teiid.common.buffer.BufferManager;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.jboss.TeiidConstants.TeiidAttribute;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.transport.ClientServiceRegistry;
import org.teiid.transport.LocalServerConnection;
import org.teiid.transport.SSLConfiguration;
import org.teiid.transport.SocketConfiguration;

class TransportAdd extends AbstractAddStepHandler {
	public static TransportAdd INSTANCE = new TransportAdd();
	
	public static TeiidAttribute[] ATTRIBUTES = {
		TeiidConstants.TRANSPORT_PROTOCOL_ATTRIBUTE,
		TeiidConstants.TRANSPORT_SOCKET_BINDING_ATTRIBUTE,
		TeiidConstants.TRANSPORT_MAX_SOCKET_THREADS_ATTRIBUTE,
		TeiidConstants.TRANSPORT_IN_BUFFER_SIZE_ATTRIBUTE,
		TeiidConstants.TRANSPORT_OUT_BUFFER_SIZE_ATTRIBUTE,
		
		TeiidConstants.AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE,
		TeiidConstants.AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE,
		TeiidConstants.AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE,
		TeiidConstants.AUTHENTICATION_KRB5_DOMAIN_ATTRIBUTE,
		TeiidConstants.AUTHENTICATION_TYPE_ATTRIBUTE,
		
		TeiidConstants.PG_MAX_LOB_SIZE_ALLOWED_ELEMENT,
		
		TeiidConstants.SSL_MODE_ATTRIBUTE,
		TeiidConstants.SSL_AUTH_MODE_ATTRIBUTE,
		TeiidConstants.SSL_SSL_PROTOCOL_ATTRIBUTE,
		TeiidConstants.SSL_KEY_MANAGEMENT_ALG_ATTRIBUTE,
		TeiidConstants.SSL_ENABLED_CIPHER_SUITES_ATTRIBUTE,
		TeiidConstants.SSL_KETSTORE_NAME_ATTRIBUTE,
		TeiidConstants.SSL_KETSTORE_PASSWORD_ATTRIBUTE,
		TeiidConstants.SSL_KETSTORE_TYPE_ATTRIBUTE,
		TeiidConstants.SSL_TRUSTSTORE_NAME_ATTRIBUTE,
		TeiidConstants.SSL_TRUSTSTORE_PASSWORD_ATTRIBUTE,
		TeiidConstants.SSL_KETSTORE_ALIAS_ATTRIBUTE,
		TeiidConstants.SSL_KETSTORE_KEY_PASSWORD_ATTRIBUTE
	};
	
	@Override
	protected void populateModel(ModelNode operation, ModelNode model) throws OperationFailedException {
		for (int i = 0; i < ATTRIBUTES.length; i++) {
			ATTRIBUTES[i].validateAndSet(operation, model);
		}
	}

	@Override
	protected void performRuntime(final OperationContext context, final ModelNode operation, final ModelNode model,
            final ServiceVerificationHandler verificationHandler, final List<ServiceController<?>> newControllers) throws OperationFailedException {

    	ServiceTarget target = context.getServiceTarget();
    	
        final ModelNode address = operation.require(OP_ADDR);
        final PathAddress pathAddress = PathAddress.pathAddress(address);
    	final String transportName = pathAddress.getLastElement().getValue();
    	
    	TransportService transport = new TransportService(transportName);
    	    	
    	String socketBinding = null;
		if (TRANSPORT_SOCKET_BINDING_ATTRIBUTE.isDefined(operation, context)) {
			socketBinding = TRANSPORT_SOCKET_BINDING_ATTRIBUTE.asString(operation, context);
    		transport.setSocketConfig(buildSocketConfiguration(context, operation));
		}
		else {
			transport.setEmbedded(true);
			LogManager.logDetail(LogConstants.CTX_SECURITY, IntegrationPlugin.Util.getString("socket_binding_not_defined",  transportName)); //$NON-NLS-1$
		}
    	
		String securityDomain = null;
   		if (AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE.isDefined(operation, context)) {
    		securityDomain = AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE.asString(operation, context);
    		transport.setAuthenticationDomain(securityDomain);
    	}  
   		
   		if (AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE.isDefined(operation, context)) {
   			transport.setSessionMaxLimit(AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE.asLong(operation, context));
   		}
    	
   		if (AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE.isDefined(operation, context)) {
   			transport.setSessionExpirationTimeLimit(AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE.asLong(operation, context));
   		}
   		
   		if (AUTHENTICATION_KRB5_DOMAIN_ATTRIBUTE.isDefined(operation, context)) {
   				LogManager.logWarning(LogConstants.CTX_SECURITY, IntegrationPlugin.Util.getString("security_not_correct",  transportName)); //$NON-NLS-1$
    			transport.setAuthenticationType(AuthenticationType.GSS);
    			transport.setAuthenticationDomain(AUTHENTICATION_KRB5_DOMAIN_ATTRIBUTE.asString(operation, context));
   		}   		
   		
   		if (AUTHENTICATION_TYPE_ATTRIBUTE.isDefined(operation, context)) {
   			transport.setAuthenticationType(AuthenticationType.valueOf(AUTHENTICATION_TYPE_ATTRIBUTE.asString(operation, context)));
   		}
   		else {
   			transport.setAuthenticationType(AuthenticationType.ANY);
   		}
   		
   		if (PG_MAX_LOB_SIZE_ALLOWED_ELEMENT.isDefined(operation, context)) {
   			transport.setMaxODBCLobSizeAllowed(PG_MAX_LOB_SIZE_ALLOWED_ELEMENT.asInt(operation, context));
   		}
   		
    	ServiceBuilder<ClientServiceRegistry> transportBuilder = target.addService(TeiidServiceNames.transportServiceName(transportName), transport);
    	if (socketBinding != null) {
    		transportBuilder.addDependency(ServiceName.JBOSS.append("binding", socketBinding), SocketBinding.class, transport.getSocketBindingInjector()); //$NON-NLS-1$
    	}
    	transportBuilder.addDependency(TeiidServiceNames.BUFFER_MGR, BufferManager.class, transport.getBufferManagerInjector());
    	transportBuilder.addDependency(TeiidServiceNames.VDB_REPO, VDBRepository.class, transport.getVdbRepositoryInjector());
    	transportBuilder.addDependency(TeiidServiceNames.ENGINE, DQPCore.class, transport.getDqpInjector());

    	
        // add security domains
        if (securityDomain != null) {
        	LogManager.logInfo(LogConstants.CTX_SECURITY, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50011, securityDomain, transportName));
        	transportBuilder.addDependency(ServiceName.JBOSS.append("security", "security-domain", securityDomain), SecurityDomainContext.class, new ConcurrentMapInjector<String,SecurityDomainContext>(transport.securityDomains, securityDomain)); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        transportBuilder.setInitialMode(ServiceController.Mode.ACTIVE);
        newControllers.add(transportBuilder.install());
        
        // register a JNDI name, this looks hard.
        if (transport.isEmbedded() && !isEmbeddedRegistered(transportName)) {
			final ReferenceFactoryService<ClientServiceRegistry> referenceFactoryService = new ReferenceFactoryService<ClientServiceRegistry>();
			final ServiceName referenceFactoryServiceName = TeiidServiceNames.embeddedTransportServiceName(transportName).append("reference-factory"); //$NON-NLS-1$
			final ServiceBuilder<?> referenceBuilder = target.addService(referenceFactoryServiceName,referenceFactoryService);
			referenceBuilder.addDependency(TeiidServiceNames.transportServiceName(transportName), ClientServiceRegistry.class, referenceFactoryService.getInjector());
			referenceBuilder.setInitialMode(ServiceController.Mode.ACTIVE);
			  
			final ContextNames.BindInfo bindInfo = ContextNames.bindInfoFor(LocalServerConnection.jndiNameForRuntime(transportName));
			final BinderService binderService = new BinderService(bindInfo.getBindName());
			final ServiceBuilder<?> binderBuilder = target.addService(bindInfo.getBinderServiceName(), binderService);
			binderBuilder.addDependency(referenceFactoryServiceName, ManagedReferenceFactory.class, binderService.getManagedObjectInjector());
			binderBuilder.addDependency(bindInfo.getParentContextServiceName(), ServiceBasedNamingStore.class, binderService.getNamingStoreInjector());        
			binderBuilder.setInitialMode(ServiceController.Mode.ACTIVE);
				
			newControllers.add(referenceBuilder.install());
			newControllers.add(binderBuilder.install());         	
        }
	}
	
	protected boolean isEmbeddedRegistered(String transportName) {
		try {
			InitialContext ic = new InitialContext();
			ic.lookup(LocalServerConnection.jndiNameForRuntime(transportName));
			return true;
		} catch (Throwable e) {
			return false;
		}
	}	
	
	private SocketConfiguration buildSocketConfiguration(final OperationContext context, ModelNode node) throws OperationFailedException {
		
		SocketConfiguration socket = new SocketConfiguration();

		if (TRANSPORT_PROTOCOL_ATTRIBUTE.isDefined(node, context)) {
			socket.setProtocol(TRANSPORT_PROTOCOL_ATTRIBUTE.asString(node, context));
		}
				
   		if (TRANSPORT_MAX_SOCKET_THREADS_ATTRIBUTE.isDefined(node, context)) {
    		socket.setMaxSocketThreads(TRANSPORT_MAX_SOCKET_THREADS_ATTRIBUTE.asInt(node, context));
    	}
   		
    	if (TRANSPORT_IN_BUFFER_SIZE_ATTRIBUTE.isDefined(node, context)) {
    		socket.setInputBufferSize(TRANSPORT_IN_BUFFER_SIZE_ATTRIBUTE.asInt(node, context));
    	}	
    	
    	if (TRANSPORT_OUT_BUFFER_SIZE_ATTRIBUTE.isDefined(node, context)) {
    		socket.setOutputBufferSize(TRANSPORT_OUT_BUFFER_SIZE_ATTRIBUTE.asInt(node, context));
    	}		   
    	
    	SSLConfiguration ssl = new SSLConfiguration();

    	if (SSL_MODE_ATTRIBUTE.isDefined(node, context)) {
    		ssl.setMode(SSL_MODE_ATTRIBUTE.asString(node, context));
    	}
    	
    	if (SSL_SSL_PROTOCOL_ATTRIBUTE.isDefined(node, context)) {
    		ssl.setSslProtocol(SSL_SSL_PROTOCOL_ATTRIBUTE.asString(node, context));
    	}	    	
    	
    	if (SSL_KEY_MANAGEMENT_ALG_ATTRIBUTE.isDefined(node, context)) {
    		ssl.setKeymanagementAlgorithm(SSL_KEY_MANAGEMENT_ALG_ATTRIBUTE.asString(node, context));
    	}    	
    	
    	if (SSL_AUTH_MODE_ATTRIBUTE.isDefined(node, context)) {
    		ssl.setAuthenticationMode(SSL_AUTH_MODE_ATTRIBUTE.asString(node, context));
    	}
    	
    	if (SSL_KETSTORE_NAME_ATTRIBUTE.isDefined(node, context)) {
    		ssl.setKeystoreFilename(SSL_KETSTORE_NAME_ATTRIBUTE.asString(node, context));
    	}	
    	
    	if (SSL_KETSTORE_ALIAS_ATTRIBUTE.isDefined(node, context)) {
    		ssl.setKeystoreKeyAlias(SSL_KETSTORE_ALIAS_ATTRIBUTE.asString(node, context));
    	}    	
    	
    	if (SSL_KETSTORE_KEY_PASSWORD_ATTRIBUTE.isDefined(node, context)) {
    		ssl.setKeystoreKeyPassword(SSL_KETSTORE_KEY_PASSWORD_ATTRIBUTE.asString(node, context));
    	}
    	
    	if (SSL_ENABLED_CIPHER_SUITES_ATTRIBUTE.isDefined(node, context)) {
    		ssl.setEnabledCipherSuites(SSL_ENABLED_CIPHER_SUITES_ATTRIBUTE.asString(node, context));
    	}
    	
    	if (SSL_KETSTORE_PASSWORD_ATTRIBUTE.isDefined(node, context)) {
    		ssl.setKeystorePassword(SSL_KETSTORE_PASSWORD_ATTRIBUTE.asString(node, context));
    	}	
    	
    	if (SSL_KETSTORE_TYPE_ATTRIBUTE.isDefined(node, context)) {
    		ssl.setKeystoreType(SSL_KETSTORE_TYPE_ATTRIBUTE.asString(node, context));
    	}		

    	if (SSL_TRUSTSTORE_NAME_ATTRIBUTE.isDefined(node, context)) {
    		ssl.setTruststoreFilename(SSL_TRUSTSTORE_NAME_ATTRIBUTE.asString(node, context));
    	}
    	if (SSL_TRUSTSTORE_PASSWORD_ATTRIBUTE.isDefined(node, context)) {
    		ssl.setTruststorePassword(SSL_TRUSTSTORE_PASSWORD_ATTRIBUTE.asString(node, context));
    	}
		socket.setSSLConfiguration(ssl);
		return socket;
	}
}
