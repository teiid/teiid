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
import static org.teiid.jboss.TeiidConstants.AUTHENTICATION_KRB5_DOMAIN_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.AUTHENTICATION_TYPE_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.PG_MAX_LOB_SIZE_ALLOWED_ELEMENT;
import static org.teiid.jboss.TeiidConstants.SSL_AUTH_MODE_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.SSL_ENABLED_CIPHER_SUITES_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.SSL_KETSTORE_ALIAS_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.SSL_KETSTORE_KEY_PASSWORD_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.SSL_KETSTORE_NAME_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.SSL_KETSTORE_PASSWORD_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.SSL_KETSTORE_TYPE_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.SSL_KEY_MANAGEMENT_ALG_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.SSL_MODE_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.SSL_SSL_PROTOCOL_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.SSL_TRUSTSTORE_NAME_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.SSL_TRUSTSTORE_PASSWORD_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.TRANSPORT_IN_BUFFER_SIZE_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.TRANSPORT_MAX_SOCKET_THREADS_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.TRANSPORT_OUT_BUFFER_SIZE_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.TRANSPORT_PROTOCOL_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.TRANSPORT_SOCKET_BINDING_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.asInt;
import static org.teiid.jboss.TeiidConstants.asLong;
import static org.teiid.jboss.TeiidConstants.asString;
import static org.teiid.jboss.TeiidConstants.isDefined;

import java.util.List;

import javax.naming.InitialContext;

import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.naming.ManagedReferenceFactory;
import org.jboss.as.naming.ServiceBasedNamingStore;
import org.jboss.as.naming.deployment.ContextNames;
import org.jboss.as.naming.service.BinderService;
import org.jboss.as.network.SocketBinding;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceTarget;
import org.teiid.common.buffer.BufferManager;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.transport.ClientServiceRegistry;
import org.teiid.transport.LocalServerConnection;
import org.teiid.transport.SSLConfiguration;
import org.teiid.transport.SocketConfiguration;

class TransportAdd extends AbstractAddStepHandler {
	public static TransportAdd INSTANCE = new TransportAdd();
	
	public static SimpleAttributeDefinition[] ATTRIBUTES = {
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
		if (isDefined(TRANSPORT_SOCKET_BINDING_ATTRIBUTE, operation, context)) {
			socketBinding = asString(TRANSPORT_SOCKET_BINDING_ATTRIBUTE, operation, context);
    		transport.setSocketConfig(buildSocketConfiguration(context, operation));
		}
		else {
			transport.setEmbedded(true);
			LogManager.logDetail(LogConstants.CTX_SECURITY, IntegrationPlugin.Util.getString("socket_binding_not_defined",  transportName)); //$NON-NLS-1$
		}
    	
		String securityDomain = null;
   		if (isDefined(AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE, operation, context)) {
    		securityDomain = asString(AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE, operation, context);
    		transport.setAuthenticationDomain(securityDomain);
    	}  
   		
   		if (isDefined(AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE, operation, context)) {
   			transport.setSessionMaxLimit(asLong(AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE, operation, context));
   		}
    	
   		if (isDefined(AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE, operation, context)) {
   			transport.setSessionExpirationTimeLimit(asLong(AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE, operation, context));
   		}
   		
   		if (isDefined(AUTHENTICATION_KRB5_DOMAIN_ATTRIBUTE, operation, context)) {
   				LogManager.logWarning(LogConstants.CTX_SECURITY, IntegrationPlugin.Util.getString("security_not_correct",  transportName)); //$NON-NLS-1$
    			transport.setAuthenticationType(AuthenticationType.GSS);
    			transport.setAuthenticationDomain(asString(AUTHENTICATION_KRB5_DOMAIN_ATTRIBUTE, operation, context));
   		}   		
   		
   		if (isDefined(AUTHENTICATION_TYPE_ATTRIBUTE, operation, context)) {
   			transport.setAuthenticationType(AuthenticationType.valueOf(asString(AUTHENTICATION_TYPE_ATTRIBUTE, operation, context)));
   		}
   		else {
   			transport.setAuthenticationType(AuthenticationType.USERPASSWORD);
   		}
   		
   		if (isDefined(PG_MAX_LOB_SIZE_ALLOWED_ELEMENT, operation, context)) {
   			transport.setMaxODBCLobSizeAllowed(asInt(PG_MAX_LOB_SIZE_ALLOWED_ELEMENT, operation, context));
   		}
   		
    	ServiceBuilder<ClientServiceRegistry> transportBuilder = target.addService(TeiidServiceNames.transportServiceName(transportName), transport);
    	if (socketBinding != null) {
    		transportBuilder.addDependency(ServiceName.JBOSS.append("binding", socketBinding), SocketBinding.class, transport.getSocketBindingInjector()); //$NON-NLS-1$
    	}
    	transportBuilder.addDependency(TeiidServiceNames.BUFFER_MGR, BufferManager.class, transport.getBufferManagerInjector());
    	transportBuilder.addDependency(TeiidServiceNames.VDB_REPO, VDBRepository.class, transport.getVdbRepositoryInjector());
    	transportBuilder.addDependency(TeiidServiceNames.ENGINE, DQPCore.class, transport.getDqpInjector());
        
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

		if (isDefined(TRANSPORT_PROTOCOL_ATTRIBUTE, node, context)) {
			socket.setProtocol(asString(TRANSPORT_PROTOCOL_ATTRIBUTE, node, context));
		}
				
   		if (isDefined(TRANSPORT_MAX_SOCKET_THREADS_ATTRIBUTE, node, context)) {
    		socket.setMaxSocketThreads(asInt(TRANSPORT_MAX_SOCKET_THREADS_ATTRIBUTE, node, context));
    	}
   		
    	if (isDefined(TRANSPORT_IN_BUFFER_SIZE_ATTRIBUTE, node, context)) {
    		socket.setInputBufferSize(asInt(TRANSPORT_IN_BUFFER_SIZE_ATTRIBUTE, node, context));
    	}	
    	
    	if (isDefined(TRANSPORT_OUT_BUFFER_SIZE_ATTRIBUTE, node, context)) {
    		socket.setOutputBufferSize(asInt(TRANSPORT_OUT_BUFFER_SIZE_ATTRIBUTE, node, context));
    	}		   
    	
    	SSLConfiguration ssl = new SSLConfiguration();

    	if (isDefined(SSL_MODE_ATTRIBUTE, node, context)) {
    		ssl.setMode(asString(SSL_MODE_ATTRIBUTE, node, context));
    	}
    	
    	if (isDefined(SSL_SSL_PROTOCOL_ATTRIBUTE, node, context)) {
    		ssl.setSslProtocol(asString(SSL_SSL_PROTOCOL_ATTRIBUTE, node, context));
    	}	    	
    	
    	if (isDefined(SSL_KEY_MANAGEMENT_ALG_ATTRIBUTE, node, context)) {
    		ssl.setKeymanagementAlgorithm(asString(SSL_KEY_MANAGEMENT_ALG_ATTRIBUTE, node, context));
    	}    	
    	
    	if (isDefined(SSL_AUTH_MODE_ATTRIBUTE, node, context)) {
    		ssl.setAuthenticationMode(asString(SSL_AUTH_MODE_ATTRIBUTE, node, context));
    	}
    	
    	if (isDefined(SSL_KETSTORE_NAME_ATTRIBUTE, node, context)) {
    		ssl.setKeystoreFilename(asString(SSL_KETSTORE_NAME_ATTRIBUTE, node, context));
    	}	
    	
    	if (isDefined(SSL_KETSTORE_ALIAS_ATTRIBUTE, node, context)) {
    		ssl.setKeystoreKeyAlias(asString(SSL_KETSTORE_ALIAS_ATTRIBUTE, node, context));
    	}    	
    	
    	if (isDefined(SSL_KETSTORE_KEY_PASSWORD_ATTRIBUTE, node, context)) {
    		ssl.setKeystoreKeyPassword(asString(SSL_KETSTORE_KEY_PASSWORD_ATTRIBUTE, node, context));
    	}
    	
    	if (isDefined(SSL_ENABLED_CIPHER_SUITES_ATTRIBUTE, node, context)) {
    		ssl.setEnabledCipherSuites(asString(SSL_ENABLED_CIPHER_SUITES_ATTRIBUTE, node, context));
    	}
    	
    	if (isDefined(SSL_KETSTORE_PASSWORD_ATTRIBUTE, node, context)) {
    		ssl.setKeystorePassword(asString(SSL_KETSTORE_PASSWORD_ATTRIBUTE, node, context));
    	}	
    	
    	if (isDefined(SSL_KETSTORE_TYPE_ATTRIBUTE, node, context)) {
    		ssl.setKeystoreType(asString(SSL_KETSTORE_TYPE_ATTRIBUTE, node, context));
    	}		

    	if (isDefined(SSL_TRUSTSTORE_NAME_ATTRIBUTE, node, context)) {
    		ssl.setTruststoreFilename(asString(SSL_TRUSTSTORE_NAME_ATTRIBUTE, node, context));
    	}
    	if (isDefined(SSL_TRUSTSTORE_PASSWORD_ATTRIBUTE, node, context)) {
    		ssl.setTruststorePassword(asString(SSL_TRUSTSTORE_PASSWORD_ATTRIBUTE, node, context));
    	}
		socket.setSSLConfiguration(ssl);
		return socket;
	}
}
