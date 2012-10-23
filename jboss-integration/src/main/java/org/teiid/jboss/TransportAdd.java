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

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

import javax.naming.InitialContext;

import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.as.controller.descriptions.DescriptionProvider;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.registry.AttributeAccess.Storage;
import org.jboss.as.naming.ManagedReferenceFactory;
import org.jboss.as.naming.ServiceBasedNamingStore;
import org.jboss.as.naming.deployment.ContextNames;
import org.jboss.as.naming.service.BinderService;
import org.jboss.as.network.SocketBinding;
import org.jboss.as.security.plugins.SecurityDomainContext;
import org.jboss.as.security.vault.RuntimeVaultReader;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.inject.ConcurrentMapInjector;
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

class TransportAdd extends AbstractAddStepHandler implements DescriptionProvider {

	private static Element[] attributes = {
		Element.TRANSPORT_PROTOCOL_ATTRIBUTE,
		Element.TRANSPORT_SOCKET_BINDING_ATTRIBUTE,
		Element.TRANSPORT_MAX_SOCKET_THREADS_ATTRIBUTE,
		Element.TRANSPORT_IN_BUFFER_SIZE_ATTRIBUTE,
		Element.TRANSPORT_OUT_BUFFER_SIZE_ATTRIBUTE,
		
		Element.AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE,
		Element.AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE,
		Element.AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE,
		Element.AUTHENTICATION_KRB5_DOMAIN_ATTRIBUTE,
		
		Element.PG_MAX_LOB_SIZE_ALLOWED_ELEMENT,
		
		Element.SSL_MODE_ATTRIBUTE,
		Element.SSL_AUTH_MODE_ATTRIBUTE,
		Element.SSL_SSL_PROTOCOL_ATTRIBUTE,
		Element.SSL_KEY_MANAGEMENT_ALG_ATTRIBUTE,
		Element.SSL_ENABLED_CIPHER_SUITES_ATTRIBUTE,
		Element.SSL_KETSTORE_NAME_ATTRIBUTE,
		Element.SSL_KETSTORE_PASSWORD_ATTRIBUTE,
		Element.SSL_KETSTORE_TYPE_ATTRIBUTE,
		Element.SSL_TRUSTSTORE_NAME_ATTRIBUTE,
		Element.SSL_TRUSTSTORE_PASSWORD_ATTRIBUTE
		
	};
	
	@Override
	public ModelNode getModelDescription(Locale locale) {
		final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);
		
        final ModelNode node = new ModelNode();
        node.get(OPERATION_NAME).set(ADD);
        node.get(DESCRIPTION).set("transport.add");  //$NON-NLS-1$
        
        describeTransport(node, REQUEST_PROPERTIES, bundle);
        return node;
	}
	
	static void describeTransport(ModelNode node, String type, ResourceBundle bundle) {
		transportDescribe(node, type, bundle);
	}

	static void transportDescribe(ModelNode node, String type, ResourceBundle bundle) {
		for (int i = 0; i < attributes.length; i++) {
			attributes[i].describe(node, type, bundle);
		}
	}
	
	@Override
	protected void populateModel(ModelNode operation, ModelNode model) {
		populate(operation, model);
	}

	public static void populate(ModelNode operation, ModelNode model) {
		for (int i = 0; i < attributes.length; i++) {
			attributes[i].populate(operation, model);
		}
	}
	
	@Override
	protected void performRuntime(final OperationContext context, final ModelNode operation, final ModelNode model,
            final ServiceVerificationHandler verificationHandler, final List<ServiceController<?>> newControllers) throws OperationFailedException {

    	ServiceTarget target = context.getServiceTarget();
    	
        final ModelNode address = operation.require(OP_ADDR);
        final PathAddress pathAddress = PathAddress.pathAddress(address);
    	final String transportName = pathAddress.getLastElement().getValue();
    	
    	TransportService transport = new TransportService();
    	    	
    	String socketBinding = null;
		if (Element.TRANSPORT_SOCKET_BINDING_ATTRIBUTE.isDefined(operation)) {
			socketBinding = Element.TRANSPORT_SOCKET_BINDING_ATTRIBUTE.asString(operation);
    		transport.setSocketConfig(buildSocketConfiguration(operation));
		}
		else {
			transport.setEmbedded(true);
			LogManager.logDetail(LogConstants.CTX_SECURITY, IntegrationPlugin.Util.getString("socket_binding_not_defined",  transportName)); //$NON-NLS-1$
		}
    	
    	List<String> domainList = Collections.emptyList();
   		if (Element.AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE.isDefined(operation)) {
    		String domains = Element.AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE.asString(operation);
    		domainList = Arrays.asList(domains.split(","));//$NON-NLS-1$
    	}  
   		transport.setAuthenticationDomains(domainList);
   		
   		if (Element.AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE.isDefined(operation)) {
   			transport.setSessionMaxLimit(Element.AUTHENTICATION_MAX_SESSIONS_ALLOWED_ATTRIBUTE.asLong(operation));
   		}
    	
   		if (Element.AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE.isDefined(operation)) {
   			transport.setSessionExpirationTimeLimit(Element.AUTHENTICATION_SESSION_EXPIRATION_TIME_LIMIT_ATTRIBUTE.asLong(operation));
   		}   		
   		if (Element.AUTHENTICATION_KRB5_DOMAIN_ATTRIBUTE.isDefined(operation)) {
   			transport.setAuthenticationType(AuthenticationType.GSS);
   			transport.setKrb5Domain(Element.AUTHENTICATION_KRB5_DOMAIN_ATTRIBUTE.asString(operation));
   		}
   		else {
   			transport.setAuthenticationType(AuthenticationType.CLEARTEXT);
   		}
   		
   		if (Element.PG_MAX_LOB_SIZE_ALLOWED_ELEMENT.isDefined(operation)) {
   			transport.setMaxODBCLobSizeAllowed(Element.PG_MAX_LOB_SIZE_ALLOWED_ELEMENT.asInt(operation));
   		}
   		
    	ServiceBuilder<ClientServiceRegistry> transportBuilder = target.addService(TeiidServiceNames.transportServiceName(transportName), transport);
    	if (socketBinding != null) {
    		transportBuilder.addDependency(ServiceName.JBOSS.append("binding", socketBinding), SocketBinding.class, transport.getSocketBindingInjector()); //$NON-NLS-1$
    	}
    	transportBuilder.addDependency(TeiidServiceNames.BUFFER_MGR, BufferManager.class, transport.getBufferManagerInjector());
    	transportBuilder.addDependency(TeiidServiceNames.VDB_REPO, VDBRepository.class, transport.getVdbRepositoryInjector());
    	transportBuilder.addDependency(TeiidServiceNames.ENGINE, DQPCore.class, transport.getDqpInjector());

    	
        // add security domains
        for (String domain:domainList) {
        	LogManager.logInfo(LogConstants.CTX_SECURITY, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50011, domain, transportName));
        	transportBuilder.addDependency(ServiceName.JBOSS.append("security", "security-domain", domain), SecurityDomainContext.class, new ConcurrentMapInjector<String,SecurityDomainContext>(transport.securityDomains, domain)); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        transportBuilder.setInitialMode(ServiceController.Mode.ACTIVE);
        newControllers.add(transportBuilder.install());
        
        // register a JNDI name, this looks hard.
        if (transport.isEmbedded() && !isEmbeddedRegistered()) {
			final ReferenceFactoryService<ClientServiceRegistry> referenceFactoryService = new ReferenceFactoryService<ClientServiceRegistry>();
			final ServiceName referenceFactoryServiceName = TeiidServiceNames.embeddedTransportServiceName(transportName).append("reference-factory"); //$NON-NLS-1$
			final ServiceBuilder<?> referenceBuilder = target.addService(referenceFactoryServiceName,referenceFactoryService);
			referenceBuilder.addDependency(TeiidServiceNames.transportServiceName(transportName), ClientServiceRegistry.class, referenceFactoryService.getInjector());
			referenceBuilder.setInitialMode(ServiceController.Mode.ACTIVE);
			  
			final ContextNames.BindInfo bindInfo = ContextNames.bindInfoFor(LocalServerConnection.TEIID_RUNTIME_CONTEXT);
			final BinderService binderService = new BinderService(bindInfo.getBindName());
			final ServiceBuilder<?> binderBuilder = target.addService(bindInfo.getBinderServiceName(), binderService);
			binderBuilder.addDependency(referenceFactoryServiceName, ManagedReferenceFactory.class, binderService.getManagedObjectInjector());
			binderBuilder.addDependency(bindInfo.getParentContextServiceName(), ServiceBasedNamingStore.class, binderService.getNamingStoreInjector());        
			binderBuilder.setInitialMode(ServiceController.Mode.ACTIVE);
				
			newControllers.add(referenceBuilder.install());
			newControllers.add(binderBuilder.install());         	
        }
	}
	
	protected boolean isEmbeddedRegistered() {
		try {
			InitialContext ic = new InitialContext();
			ic.lookup(LocalServerConnection.TEIID_RUNTIME_CONTEXT);
			return true;
		} catch (Throwable e) {
			return false;
		}
	}	
	
	private SocketConfiguration buildSocketConfiguration(ModelNode node) {
		
		SocketConfiguration socket = new SocketConfiguration();

		if (Element.TRANSPORT_PROTOCOL_ATTRIBUTE.isDefined(node)) {
			socket.setProtocol(Element.TRANSPORT_PROTOCOL_ATTRIBUTE.asString(node));
		}
				
   		if (Element.TRANSPORT_MAX_SOCKET_THREADS_ATTRIBUTE.isDefined(node)) {
    		socket.setMaxSocketThreads(Element.TRANSPORT_MAX_SOCKET_THREADS_ATTRIBUTE.asInt(node));
    	}
   		
    	if (Element.TRANSPORT_IN_BUFFER_SIZE_ATTRIBUTE.isDefined(node)) {
    		socket.setInputBufferSize(Element.TRANSPORT_IN_BUFFER_SIZE_ATTRIBUTE.asInt(node));
    	}	
    	
    	if (Element.TRANSPORT_OUT_BUFFER_SIZE_ATTRIBUTE.isDefined(node)) {
    		socket.setOutputBufferSize(Element.TRANSPORT_OUT_BUFFER_SIZE_ATTRIBUTE.asInt(node));
    	}		   
    	
    	SSLConfiguration ssl = new SSLConfiguration();

    	if (Element.SSL_MODE_ATTRIBUTE.isDefined(node)) {
    		ssl.setMode(Element.SSL_MODE_ATTRIBUTE.asString(node));
    	}
    	
    	if (Element.SSL_SSL_PROTOCOL_ATTRIBUTE.isDefined(node)) {
    		ssl.setSslProtocol(Element.SSL_SSL_PROTOCOL_ATTRIBUTE.asString(node));
    	}	    	
    	
    	if (Element.SSL_KEY_MANAGEMENT_ALG_ATTRIBUTE.isDefined(node)) {
    		ssl.setKeymanagementAlgorithm(Element.SSL_KEY_MANAGEMENT_ALG_ATTRIBUTE.asString(node));
    	}    	
    	
    	if (Element.SSL_AUTH_MODE_ATTRIBUTE.isDefined(node)) {
    		ssl.setAuthenticationMode(Element.SSL_AUTH_MODE_ATTRIBUTE.asString(node));
    	}
    	
    	if (Element.SSL_KETSTORE_NAME_ATTRIBUTE.isDefined(node)) {
    		ssl.setKeystoreFilename(Element.SSL_KETSTORE_NAME_ATTRIBUTE.asString(node));
    	}	
    	
    	if (Element.SSL_ENABLED_CIPHER_SUITES_ATTRIBUTE.isDefined(node)) {
    		ssl.setEnabledCipherSuites(Element.SSL_ENABLED_CIPHER_SUITES_ATTRIBUTE.asString(node));
    	}
    	
    	if (Element.SSL_KETSTORE_PASSWORD_ATTRIBUTE.isDefined(node)) {
    		String passcode = Element.SSL_KETSTORE_PASSWORD_ATTRIBUTE.asString(node);
    		RuntimeVaultReader vaultReader = new RuntimeVaultReader();
    		if (vaultReader.isVaultFormat(passcode)) {
    			passcode = vaultReader.retrieveFromVault(passcode);
    		}
    		ssl.setKeystorePassword(passcode);
    	}	
    	
    	if (Element.SSL_KETSTORE_TYPE_ATTRIBUTE.isDefined(node)) {
    		ssl.setKeystoreType(Element.SSL_KETSTORE_TYPE_ATTRIBUTE.asString(node));
    	}		

    	if (Element.SSL_TRUSTSTORE_NAME_ATTRIBUTE.isDefined(node)) {
    		ssl.setTruststoreFilename(Element.SSL_TRUSTSTORE_NAME_ATTRIBUTE.asString(node));
    	}
    	if (Element.SSL_TRUSTSTORE_PASSWORD_ATTRIBUTE.isDefined(node)) {
    		String passcode = Element.SSL_TRUSTSTORE_PASSWORD_ATTRIBUTE.asString(node);
    		RuntimeVaultReader vaultReader = new RuntimeVaultReader();
    		if (vaultReader.isVaultFormat(passcode)) {
    			passcode = vaultReader.retrieveFromVault(passcode);
    		}
    		ssl.setTruststorePassword(passcode);
    	}
		socket.setSSLConfiguration(ssl);
		return socket;
	}

	
	public static void registerReadWriteAttributes(ManagementResourceRegistration subsystem) {
		for (int i = 0; i < attributes.length; i++) {
			subsystem.registerReadWriteAttribute(attributes[i].getModelName(), null, AttributeWrite.INSTANCE, Storage.CONFIGURATION);
		}		
	}
}
