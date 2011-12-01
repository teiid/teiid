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

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.ADD;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.DESCRIPTION;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OPERATION_NAME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.REQUEST_PROPERTIES;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.StringTokenizer;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.as.controller.descriptions.DescriptionProvider;
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
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.services.BufferServiceImpl;
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
    	
    	Transport transport = new Transport();
    	    	
    	String socketBinding = null;
		if (Element.TRANSPORT_SOCKET_BINDING_ATTRIBUTE.isDefined(operation)) {
			socketBinding = Element.TRANSPORT_SOCKET_BINDING_ATTRIBUTE.asString(operation);
    		transport.setSocketConfig(buildSocketConfiguration(operation));
		}
		else {
			transport.setEmbedded(true);
			LogManager.logDetail(LogConstants.CTX_SECURITY, IntegrationPlugin.Util.getString("socket_binding_not_defined",  transportName)); //$NON-NLS-1$
		}
    	
    	ArrayList<String> domainList = new ArrayList<String>();
   		if (Element.AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE.isDefined(operation)) {
    		String domains = Element.AUTHENTICATION_SECURITY_DOMAIN_ATTRIBUTE.asString(operation);
    		StringTokenizer st = new StringTokenizer(domains, ","); //$NON-NLS-1$
    		while(st.hasMoreTokens()) {
    			domainList.add(st.nextToken());
    		}
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
    	transportBuilder.addDependency(TeiidServiceNames.BUFFER_MGR, BufferServiceImpl.class, transport.getBufferServiceInjector());
    	transportBuilder.addDependency(TeiidServiceNames.VDB_REPO, VDBRepository.class, transport.getVdbRepositoryInjector());
    	transportBuilder.addDependency(TeiidServiceNames.ENGINE, DQPCore.class, transport.getDqpInjector());

    	
        // add security domains
        for (String domain:domainList) {
        	LogManager.logInfo(LogConstants.CTX_SECURITY, IntegrationPlugin.Util.getString("security_enabled", domain, transportName)); //$NON-NLS-1$
        	transportBuilder.addDependency(ServiceName.JBOSS.append("security", "security-domain", domain), SecurityDomainContext.class, new ConcurrentMapInjector<String,SecurityDomainContext>(transport.securityDomains, domain)); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        transportBuilder.setInitialMode(ServiceController.Mode.ACTIVE);
        newControllers.add(transportBuilder.install());
        
        // register a JNDI name, this looks hard.
        if (transport.isEmbedded() && !isEmbeddedRegistered()) {
			final CSRReferenceFactoryService referenceFactoryService = new CSRReferenceFactoryService();
			final ServiceName referenceFactoryServiceName =TeiidServiceNames.transportServiceName(transportName).append("reference-factory"); //$NON-NLS-1$
			final ServiceBuilder<?> referenceBuilder = target.addService(referenceFactoryServiceName,referenceFactoryService);
			referenceBuilder.addDependency(TeiidServiceNames.transportServiceName(transportName), ClientServiceRegistry.class, referenceFactoryService.getCSRInjector());
			referenceBuilder.setInitialMode(ServiceController.Mode.ACTIVE);
			  
			final ContextNames.BindInfo bindInfo = ContextNames.bindInfoFor(LocalServerConnection.TEIID_RUNTIME_CONTEXT);
			final BinderService embedded = new BinderService(bindInfo.getBindName());
			final ServiceBuilder<?> embeddedBinderBuilder = target.addService(bindInfo.getBinderServiceName(), embedded);
			embeddedBinderBuilder.addDependency(referenceFactoryServiceName, ManagedReferenceFactory.class, embedded.getManagedObjectInjector());
			embeddedBinderBuilder.addDependency(bindInfo.getParentContextServiceName(), ServiceBasedNamingStore.class, embedded.getNamingStoreInjector());        
			embeddedBinderBuilder.setInitialMode(ServiceController.Mode.ACTIVE);
				
			newControllers.add(referenceBuilder.install());
			newControllers.add(embeddedBinderBuilder.install());         	
        }
	}
	
	protected boolean isEmbeddedRegistered() {
		try {
			InitialContext ic = new InitialContext();
			ic.lookup(LocalServerConnection.TEIID_RUNTIME_CONTEXT);
			return true;
		} catch (NamingException e) {
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
    	
    	boolean sslEnabled = false;
    	SSLConfiguration ssl = new SSLConfiguration();
    	ssl.setAuthenticationMode(SSLConfiguration.ANONYMOUS);
    	
    	if (Element.SSL_MODE_ATTRIBUTE.isDefined(node)) {
    		ssl.setMode(Element.SSL_MODE_ATTRIBUTE.asString(node));
    		sslEnabled = true;
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
    		ssl.setKeystorePassword(Element.SSL_KETSTORE_PASSWORD_ATTRIBUTE.asString(node));
    	}	
    	
    	if (Element.SSL_KETSTORE_TYPE_ATTRIBUTE.isDefined(node)) {
    		ssl.setKeystoreType(Element.SSL_KETSTORE_TYPE_ATTRIBUTE.asString(node));
    	}		

    	if (Element.SSL_TRUSTSTORE_NAME_ATTRIBUTE.isDefined(node)) {
    		ssl.setTruststoreFilename(Element.SSL_TRUSTSTORE_NAME_ATTRIBUTE.asString(node));
    	}
    	if (Element.SSL_TRUSTSTORE_PASSWORD_ATTRIBUTE.isDefined(node)) {
    		ssl.setTruststorePassword(Element.SSL_TRUSTSTORE_PASSWORD_ATTRIBUTE.asString(node));
    	}
    	if (sslEnabled) {
    		socket.setSSLConfiguration(ssl);
    	}
		return socket;
	}

}
