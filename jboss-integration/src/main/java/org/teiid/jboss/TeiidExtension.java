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
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.controller.parsing.ExtensionParsingContext;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.registry.AttributeAccess.Storage;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.teiid.logging.Log4jListener;
import org.teiid.logging.LogManager;

public class TeiidExtension implements Extension {
	
	private static final String ACTIVE_SESSION_COUNT = "active-session-count"; //$NON-NLS-1$
	private static final String RUNTIME_VERSION = "runtime-version"; //$NON-NLS-1$
	private static final String REQUESTS_PER_SESSION = "requests-per-session"; //$NON-NLS-1$
	private static final String ACTIVE_SESSIONS = "active-sessions"; //$NON-NLS-1$
	private static final String REQUESTS_PER_VDB = "requests-per-vdb"; //$NON-NLS-1$
	private static final String LONG_RUNNING_QUERIES = "long-running-queries"; //$NON-NLS-1$
	private static final String TERMINATE_SESSION = "terminate-session";//$NON-NLS-1$
	private static final String CANCEL_QUERY = "cancel-query";//$NON-NLS-1$
	private static final String CACHE_TYPES = "cache-types";//$NON-NLS-1$
	private static final String CLEAR_CACHE = "clear-cache";//$NON-NLS-1$
	private static final String CACHE_STATISTICS = "cache-statistics";//$NON-NLS-1$
	private static final String WORKERPOOL_STATISTICS = "workerpool-statistics";//$NON-NLS-1$
	private static final String ACTIVE_TRANSACTIONS = "active-transactions";//$NON-NLS-1$
	private static final String TERMINATE_TRANSACTION = "terminate-transaction";//$NON-NLS-1$
	private static final String MERGE_VDBS = "merge-vdbs";//$NON-NLS-1$
	private static final String EXECUTE_QUERY = "execute-query";//$NON-NLS-1$
	private static final String GETVDBS = "getVDBs";//$NON-NLS-1$
	private static final String GETVDB = "getVDB";//$NON-NLS-1$
	
	public static final String TEIID_SUBSYSTEM = "teiid"; //$NON-NLS-1$
	private static TeiidSubsystemParser parser = new TeiidSubsystemParser();
	private static QueryEngineAdd ENGINE_ADD = new QueryEngineAdd();
	private static QueryEngineRemove ENGINE_REMOVE = new QueryEngineRemove();
	private static TranslatorAdd TRANSLATOR_ADD = new TranslatorAdd();
	private static TranslatorRemove TRANSLATOR_REMOVE = new TranslatorRemove();
	private static TeiidBootServicesAdd TEIID_BOOT_ADD = new TeiidBootServicesAdd();
	
	@Override
	public void initialize(ExtensionContext context) {
		final SubsystemRegistration registration = context.registerSubsystem(TEIID_SUBSYSTEM);
		
		LogManager.setLogListener(new Log4jListener());
		
		registration.registerXMLElementWriter(parser);

		// Main Teiid system, with children query engine and translators.
		final ManagementResourceRegistration teiidSubsystem = registration.registerSubsystemModel(new DescriptionProvider() {
			@Override
			public ModelNode getModelDescription(Locale locale) {
				final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);
				
		        ModelNode node = new ModelNode();
		        node.get(ModelDescriptionConstants.DESCRIPTION).set("teiid subsystem"); //$NON-NLS-1$
		        node.get(ModelDescriptionConstants.HEAD_COMMENT_ALLOWED).set(true);
		        node.get(ModelDescriptionConstants.TAIL_COMMENT_ALLOWED).set(true);
		        node.get(ModelDescriptionConstants.NAMESPACE).set(Namespace.CURRENT.getUri());
		        
		        TeiidBootServicesAdd.describeTeiidRoot(bundle, ATTRIBUTES, node);
		        node.get(CHILDREN, Configuration.QUERY_ENGINE, DESCRIPTION).set(bundle.getString(Configuration.QUERY_ENGINE+Configuration.DESC)); 
		        node.get(CHILDREN, Configuration.QUERY_ENGINE, REQUIRED).set(false);
		        
		        node.get(CHILDREN, Configuration.TRANSLATOR, DESCRIPTION).set(bundle.getString(Configuration.TRANSLATOR+Configuration.DESC));
		        node.get(CHILDREN, Configuration.TRANSLATOR, REQUIRED).set(false);

		        return node;
		    }
		});
		teiidSubsystem.registerOperationHandler(ADD, TEIID_BOOT_ADD, TEIID_BOOT_ADD, false);
		//teiidSubsystem.registerOperationHandler(REMOVE, ENGINE_REMOVE, ENGINE_REMOVE, false);     
				
		// Translator Subsystem
        final ManagementResourceRegistration translatorSubsystem = teiidSubsystem.registerSubModel(PathElement.pathElement(Configuration.TRANSLATOR), new DescriptionProvider() {
			@Override
			public ModelNode getModelDescription(Locale locale) {
				final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);

				final ModelNode node = new ModelNode();
	            node.get(DESCRIPTION).set(bundle.getString(Configuration.TRANSLATOR+Configuration.DESC));
	            node.get(HEAD_COMMENT_ALLOWED).set(true);
	            node.get(TAIL_COMMENT_ALLOWED).set(true);

	            addAttribute(node, Configuration.TRANSLATOR_NAME, ATTRIBUTES, bundle.getString(Configuration.TRANSLATOR_NAME+Configuration.DESC), ModelType.STRING, true, null);
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
        
		
		QueryEngineOperationHandler op;
		engineSubsystem.registerReadOnlyAttribute(RUNTIME_VERSION, new GetRuntimeVersion(RUNTIME_VERSION), Storage.RUNTIME); 
		engineSubsystem.registerReadOnlyAttribute(ACTIVE_SESSION_COUNT, new GetActiveSessionsCount(ACTIVE_SESSION_COUNT), Storage.RUNTIME); 
		
		op = new GetActiveSessions(ACTIVE_SESSIONS);
		engineSubsystem.registerOperationHandler(ACTIVE_SESSIONS, op, op); 
		
		op = new GetRequestsPerSession(REQUESTS_PER_SESSION);
		engineSubsystem.registerOperationHandler(REQUESTS_PER_SESSION, op, op);

		op = new GetRequestsPerVDB(REQUESTS_PER_VDB);
		engineSubsystem.registerOperationHandler(REQUESTS_PER_VDB, op, op);
		
		op = new GetLongRunningQueries(LONG_RUNNING_QUERIES);
		engineSubsystem.registerOperationHandler(LONG_RUNNING_QUERIES, op, op);		
		
		op = new TerminateSession(TERMINATE_SESSION);
		engineSubsystem.registerOperationHandler(TERMINATE_SESSION, op, op);	
		
		op = new CancelQuery(CANCEL_QUERY);
		engineSubsystem.registerOperationHandler(CANCEL_QUERY, op, op);		
		
		op = new CacheTypes(CACHE_TYPES);
		engineSubsystem.registerOperationHandler(CACHE_TYPES, op, op);	
		
		op = new ClearCache(CLEAR_CACHE);
		engineSubsystem.registerOperationHandler(CLEAR_CACHE, op, op);	
		
		op = new CacheStatistics(CACHE_STATISTICS);
		engineSubsystem.registerOperationHandler(CACHE_STATISTICS, op, op);		
		
		op = new WorkerPoolStatistics(WORKERPOOL_STATISTICS);
		engineSubsystem.registerOperationHandler(WORKERPOOL_STATISTICS, op, op);		
		
		op = new ActiveTransactions(ACTIVE_TRANSACTIONS);
		engineSubsystem.registerOperationHandler(ACTIVE_TRANSACTIONS, op, op);	
		
		op = new TerminateTransaction(TERMINATE_TRANSACTION);
		engineSubsystem.registerOperationHandler(TERMINATE_TRANSACTION, op, op);		
		
		op = new MergeVDBs(MERGE_VDBS);
		engineSubsystem.registerOperationHandler(MERGE_VDBS, op, op);	
		
		op = new ExecuteQuery(EXECUTE_QUERY);
		engineSubsystem.registerOperationHandler(EXECUTE_QUERY, op, op);	
		
		op = new GetVDBs(GETVDBS);
		engineSubsystem.registerOperationHandler(GETVDBS, op, op);
		
		op = new GetVDB(GETVDB);
		engineSubsystem.registerOperationHandler(GETVDB, op, op);		
	}

	@Override
	public void initializeParsers(ExtensionParsingContext context) {
		context.setSubsystemXmlMapping(Namespace.CURRENT.getUri(), parser);
	}
}
