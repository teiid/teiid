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
import static org.teiid.jboss.Configuration.addAttribute;

import java.util.Locale;
import java.util.ResourceBundle;

import org.jboss.as.controller.Extension;
import org.jboss.as.controller.ExtensionContext;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SubsystemRegistration;
import org.jboss.as.controller.descriptions.DescriptionProvider;
import org.jboss.as.controller.parsing.ExtensionParsingContext;
import org.jboss.as.controller.registry.AttributeAccess.Storage;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.teiid.logging.Log4jListener;
import org.teiid.logging.LogManager;

public class TeiidExtension implements Extension {
	
	private static final String ACTIVE_SESSION_COUNT = "active-session-count"; //$NON-NLS-1$
	private static final String RUNTIME_VERSION = "runtime-version"; //$NON-NLS-1$
	
	public static final String TEIID_SUBSYSTEM = "teiid"; //$NON-NLS-1$
	private static TeiidSubsystemParser parser = new TeiidSubsystemParser();
	private static QueryEngineAdd ENGINE_ADD = new QueryEngineAdd();
	private static QueryEngineRemove ENGINE_REMOVE = new QueryEngineRemove();
	private static TranslatorAdd TRANSLATOR_ADD = new TranslatorAdd();
	private static TranslatorRemove TRANSLATOR_REMOVE = new TranslatorRemove();
	private static TeiidBootServicesAdd TEIID_BOOT_ADD = new TeiidBootServicesAdd();
	private static TeiidSubsystemDescribe TEIID_DESCRIBE = new TeiidSubsystemDescribe();
	
	@Override
	public void initialize(ExtensionContext context) {
		final SubsystemRegistration registration = context.registerSubsystem(TEIID_SUBSYSTEM);
		
		LogManager.setLogListener(new Log4jListener());
		
		registration.registerXMLElementWriter(parser);

		// Main Teiid system, with children query engine and translators.
		
		final ManagementResourceRegistration teiidSubsystem = registration.registerSubsystemModel(TEIID_DESCRIBE);
		teiidSubsystem.registerOperationHandler(ADD, TEIID_BOOT_ADD, TEIID_BOOT_ADD, false);
		teiidSubsystem.registerOperationHandler(DESCRIBE, TEIID_DESCRIBE, TEIID_DESCRIBE, false);     
				
		// Translator Subsystem
        final ManagementResourceRegistration translatorSubsystem = teiidSubsystem.registerSubModel(PathElement.pathElement(Configuration.TRANSLATOR), new DescriptionProvider() {
			@Override
			public ModelNode getModelDescription(Locale locale) {
				final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);

				final ModelNode node = new ModelNode();
	            node.get(DESCRIPTION).set(bundle.getString(Configuration.TRANSLATOR+Configuration.DESC));
	            node.get(HEAD_COMMENT_ALLOWED).set(true);
	            node.get(TAIL_COMMENT_ALLOWED).set(true);
	            addAttribute(node, Configuration.TRANSLATOR_MODULE, ATTRIBUTES, bundle.getString(Configuration.TRANSLATOR_MODULE+Configuration.DESC), ModelType.STRING, true, null);
	            return node;
			}
		});
        translatorSubsystem.registerOperationHandler(ADD, TRANSLATOR_ADD, TRANSLATOR_ADD, false);
        translatorSubsystem.registerOperationHandler(REMOVE, TRANSLATOR_REMOVE, TRANSLATOR_REMOVE, false);

        
        // Query engine subsystem
        final ManagementResourceRegistration engineSubsystem = teiidSubsystem.registerSubModel(PathElement.pathElement(Configuration.QUERY_ENGINE), new DescriptionProvider() {
			@Override
			public ModelNode getModelDescription(Locale locale) {
				final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);
				
				final ModelNode node = new ModelNode();
	            node.get(DESCRIPTION).set(bundle.getString(Configuration.QUERY_ENGINE+Configuration.DESC));
	            node.get(HEAD_COMMENT_ALLOWED).set(true);
	            node.get(TAIL_COMMENT_ALLOWED).set(true);
	            QueryEngineAdd.describeQueryEngine(node, ATTRIBUTES, bundle);
	            return node;
			}
		});
        engineSubsystem.registerOperationHandler(ADD, ENGINE_ADD, ENGINE_ADD, false);
        engineSubsystem.registerOperationHandler(REMOVE, ENGINE_REMOVE, ENGINE_REMOVE, false);     
        
		
		engineSubsystem.registerReadOnlyAttribute(RUNTIME_VERSION, new GetRuntimeVersion(RUNTIME_VERSION), Storage.RUNTIME); 
		engineSubsystem.registerReadOnlyAttribute(ACTIVE_SESSION_COUNT, new GetActiveSessionsCount(ACTIVE_SESSION_COUNT), Storage.RUNTIME); 
		
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
		
		// engine level admin api handlers
		new ListSessions().register(engineSubsystem);
		new RequestsPerSession().register(engineSubsystem);
		new RequestsPerVDB().register(engineSubsystem);
		new GetLongRunningQueries().register(engineSubsystem);
		new TerminateSession().register(engineSubsystem);
		new CancelRequest().register(engineSubsystem);
		new WorkerPoolStatistics().register(engineSubsystem);
		new ListTransactions().register(engineSubsystem);
		new TerminateTransaction().register(engineSubsystem);
		new ExecuteQuery().register(engineSubsystem);
	}

	@Override
	public void initializeParsers(ExtensionParsingContext context) {
		context.setSubsystemXmlMapping(Namespace.CURRENT.getUri(), parser);
	}
}
