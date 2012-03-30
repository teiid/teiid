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
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.ATTRIBUTES;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.CHILDREN;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.DESCRIBE;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.DESCRIPTION;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.REMOVE;

import java.util.Locale;
import java.util.ResourceBundle;

import org.jboss.as.controller.Extension;
import org.jboss.as.controller.ExtensionContext;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SubsystemRegistration;
import org.jboss.as.controller.descriptions.DescriptionProvider;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.controller.parsing.ExtensionParsingContext;
import org.jboss.as.controller.registry.AttributeAccess.Storage;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.teiid.logging.LogManager;

public class TeiidExtension implements Extension {
	
	private static final String ACTIVE_SESSION_COUNT = "active-session-count"; //$NON-NLS-1$
	private static final String RUNTIME_VERSION = "runtime-version"; //$NON-NLS-1$
	
	public static final String TEIID_SUBSYSTEM = "teiid"; //$NON-NLS-1$
	public static final int MAJOR_VERSION = 1;
	public static final int MINOR_VERSION = 0;
	
	private static TeiidSubsystemParser parser = new TeiidSubsystemParser();
	private static TransportAdd TRANSPORT_ADD = new TransportAdd();
	private static TransportRemove TRANSPORT_REMOVE = new TransportRemove();
	private static TranslatorAdd TRANSLATOR_ADD = new TranslatorAdd();
	private static TranslatorRemove TRANSLATOR_REMOVE = new TranslatorRemove();
	private static TeiidAdd TEIID_BOOT_ADD = new TeiidAdd();
	private static TeiidSubsystemDescribe TEIID_DESCRIBE = new TeiidSubsystemDescribe();
	
	@Override
	public void initialize(ExtensionContext context) {
		final SubsystemRegistration registration = context.registerSubsystem(TEIID_SUBSYSTEM, MAJOR_VERSION, MINOR_VERSION);
		
		LogManager.setLogListener(new JBossLogger());
		
		registration.registerXMLElementWriter(parser);

		// Main Teiid system, with children query engine and translators.
		
		final ManagementResourceRegistration teiidSubsystem = registration.registerSubsystemModel(new DescriptionProvider() {
			@Override
			public ModelNode getModelDescription(Locale locale) {
				final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);
		        ModelNode node = new ModelNode();
		        node.get(ModelDescriptionConstants.DESCRIPTION).set("teiid subsystem"); //$NON-NLS-1$
		        
		        TeiidAdd.describeTeiid(node, ATTRIBUTES, bundle);
		        node.get(CHILDREN, Element.TRANSPORT_ELEMENT.getLocalName(), DESCRIPTION).set(Element.TRANSPORT_ELEMENT.getDescription(bundle)); 
		        node.get(CHILDREN, Element.TRANSLATOR_ELEMENT.getLocalName(), DESCRIPTION).set(Element.TRANSLATOR_ELEMENT.getDescription(bundle));
		        return node;
			}
		});
		teiidSubsystem.registerOperationHandler(ADD, TEIID_BOOT_ADD, TEIID_BOOT_ADD, false);
		teiidSubsystem.registerOperationHandler(DESCRIBE, TEIID_DESCRIBE, TEIID_DESCRIBE, false);     
				
		// Translator Subsystem
        final ManagementResourceRegistration translatorSubsystem = teiidSubsystem.registerSubModel(PathElement.pathElement(Element.TRANSLATOR_ELEMENT.getLocalName()), new DescriptionProvider() {
			@Override
			public ModelNode getModelDescription(Locale locale) {
				final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);

				final ModelNode node = new ModelNode();
	            node.get(DESCRIPTION).set(Element.TRANSLATOR_ELEMENT.getDescription(bundle));
	            Element.TRANSLATOR_MODULE_ATTRIBUTE.describe(node, ATTRIBUTES, bundle);
	            return node;
			}
		});
        translatorSubsystem.registerOperationHandler(ADD, TRANSLATOR_ADD, TRANSLATOR_ADD, false);
        translatorSubsystem.registerOperationHandler(REMOVE, TRANSLATOR_REMOVE, TRANSLATOR_REMOVE, false);

        
        // Query engine subsystem
        final ManagementResourceRegistration transportModel = teiidSubsystem.registerSubModel(PathElement.pathElement(Element.TRANSPORT_ELEMENT.getLocalName()), new DescriptionProvider() {
			@Override
			public ModelNode getModelDescription(Locale locale) {
				final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);
				
				final ModelNode node = new ModelNode();
	            node.get(DESCRIPTION).set(Element.TRANSPORT_ELEMENT.getDescription(bundle));
	            TransportAdd.transportDescribe(node, ATTRIBUTES, bundle);
	            return node;
			}
		});
        transportModel.registerOperationHandler(ADD, TRANSPORT_ADD, TRANSPORT_ADD, false);
        transportModel.registerOperationHandler(REMOVE, TRANSPORT_REMOVE, TRANSPORT_REMOVE, false);     
        
		
        teiidSubsystem.registerReadOnlyAttribute(RUNTIME_VERSION, new GetRuntimeVersion(RUNTIME_VERSION), Storage.RUNTIME); 
        teiidSubsystem.registerReadOnlyAttribute(ACTIVE_SESSION_COUNT, new GetActiveSessionsCount(ACTIVE_SESSION_COUNT), Storage.RUNTIME); 
		
		// teiid level admin api operation handlers
		new GetTranslator().register(teiidSubsystem);
		new ListTranslators().register(teiidSubsystem);
		new MergeVDBs().register(teiidSubsystem);
		new ListVDBs().register(teiidSubsystem);
		new GetVDB().register(teiidSubsystem);
		new CacheTypes().register(teiidSubsystem);
		new ClearCache().register(teiidSubsystem);
		new CacheStatistics().register(teiidSubsystem);
		new AddDataRole().register(teiidSubsystem);
		new RemoveDataRole().register(teiidSubsystem);
		new AddAnyAuthenticatedDataRole().register(teiidSubsystem);
		new AssignDataSource().register(teiidSubsystem);
		new ChangeVDBConnectionType().register(teiidSubsystem);
		new RemoveAnyAuthenticatedDataRole().register(teiidSubsystem);
		new ListRequests().register(teiidSubsystem);
		new ListSessions().register(teiidSubsystem);
		new ListRequestsPerSession().register(teiidSubsystem);
		new ListRequestsPerVDB().register(teiidSubsystem);
		new ListLongRunningRequests().register(teiidSubsystem);
		new TerminateSession().register(teiidSubsystem);
		new CancelRequest().register(teiidSubsystem);
		new WorkerPoolStatistics().register(teiidSubsystem);
		new ListTransactions().register(teiidSubsystem);
		new TerminateTransaction().register(teiidSubsystem);
		new ExecuteQuery().register(teiidSubsystem);
		new MarkDataSourceAvailable().register(teiidSubsystem);
		new ReadRARDescription().register(teiidSubsystem);
	}

	@Override
	public void initializeParsers(ExtensionParsingContext context) {
		context.setSubsystemXmlMapping(TEIID_SUBSYSTEM, Namespace.CURRENT.getUri(), parser);
	}
}
