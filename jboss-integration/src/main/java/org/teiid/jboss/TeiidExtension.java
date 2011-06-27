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
import org.jboss.as.controller.registry.ModelNodeRegistration;
import org.jboss.as.controller.registry.AttributeAccess.Storage;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

public class TeiidExtension implements Extension {
	
	private static final String ACTIVE_SESSION_COUNT = "active-session-count";
	private static final String RUNTIME_VERSION = "runtime-version";
	private static final String REQUESTS_PER_SESSION = "requests-per-session";
	private static final String ACTIVE_SESSIONS = "active-sessions";
	private static final String REQUESTS_PER_VDB = "requests-per-vdb";
	private static final String LONG_RUNNING_QUERIES = "long-running-queries";
	private static final String TERMINATE_SESSION = "terminate-session";
	private static final String CANCEL_QUERY = "cancel-query";
	private static final String CACHE_TYPES = "cache-types";
	private static final String CLEAR_CACHE = "clear-cache";
	private static final String CACHE_STATISTICS = "cache-statistics";
	private static final String WORKERPOOL_STATISTICS = "workerpool-statistics";
	private static final String ACTIVE_TRANSACTIONS = "active-transactions";
	private static final String TERMINATE_TRANSACTION = "terminate-transaction";
	private static final String MERGE_VDBS = "merge-vdbs";
	private static final String EXECUTE_QUERY = "execute-query";
	
	public static final String SUBSYSTEM_NAME = "teiid"; //$NON-NLS-1$
	private static TeiidSubsystemParser parser = new TeiidSubsystemParser();
	private static QueryEngineDescription ENGINE_DESC = new QueryEngineDescription();
	private static QueryEngineAdd ENGINE_ADD = new QueryEngineAdd();
	private static TranslatorAdd TRANSLATOR_ADD = new TranslatorAdd();
	private static TranslatorRemove TRANSLATOR_REMOVE = new TranslatorRemove();
	
	@Override
	public void initialize(ExtensionContext context) {
		final SubsystemRegistration registration = context.registerSubsystem(SUBSYSTEM_NAME);
		
		registration.registerXMLElementWriter(parser);
		
		final ModelNodeRegistration subsystem = registration.registerSubsystemModel(new DescriptionProvider() {
			
			@Override
			public ModelNode getModelDescription(Locale locale) {
				final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);
				
		        ModelNode node = new ModelNode();
		        node.get(ModelDescriptionConstants.DESCRIPTION).set("teiid subsystem"); //$NON-NLS-1$
		        node.get(ModelDescriptionConstants.HEAD_COMMENT_ALLOWED).set(true);
		        node.get(ModelDescriptionConstants.TAIL_COMMENT_ALLOWED).set(true);
		        node.get(ModelDescriptionConstants.NAMESPACE).set(Namespace.CURRENT.getUri());
		        
		        //getQueryEngineDescription(node.get(CHILDREN, Configuration.QUERY_ENGINE), ATTRIBUTES, bundle);

		        node.get(CHILDREN, Configuration.QUERY_ENGINE, DESCRIPTION).set(bundle.getString(Configuration.QUERY_ENGINE)); 
		        node.get(CHILDREN, Configuration.QUERY_ENGINE, REQUIRED).set(false);
		        
		        node.get(CHILDREN, Configuration.TRANSLATOR, DESCRIPTION).set(bundle.getString(Configuration.TRANSLATOR));
		        node.get(CHILDREN, Configuration.TRANSLATOR, REQUIRED).set(false);

		        return node;
		    }
		});
		subsystem.registerOperationHandler(ModelDescriptionConstants.ADD, ENGINE_ADD, ENGINE_DESC);
		//subsystem.registerOperationHandler(ModelDescriptionConstants.DESCRIBE, describe, describe, false);
		
        final ModelNodeRegistration translators = subsystem.registerSubModel(PathElement.pathElement(Configuration.TRANSLATOR), new DescriptionProvider() {
			@Override
			public ModelNode getModelDescription(Locale locale) {
				final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);

				final ModelNode node = new ModelNode();
	            node.get(DESCRIPTION).set(bundle.getString(Configuration.TRANSLATOR));
	            node.get(HEAD_COMMENT_ALLOWED).set(true);
	            node.get(TAIL_COMMENT_ALLOWED).set(true);

	            addAttribute(node, Configuration.TRANSLATOR_NAME, ATTRIBUTES, bundle.getString(Configuration.TRANSLATOR_NAME+Configuration.DESC), ModelType.STRING, true, null);
	            addAttribute(node, Configuration.TRANSLATOR_MODULE, ATTRIBUTES, bundle.getString(Configuration.TRANSLATOR_MODULE+Configuration.DESC), ModelType.STRING, true, null);
	            return node;
			}
		});
        translators.registerOperationHandler(ADD, TRANSLATOR_ADD, TRANSLATOR_ADD, false);
        translators.registerOperationHandler(REMOVE, TRANSLATOR_REMOVE, TRANSLATOR_REMOVE, false);		
		
		QueryEngineOperationHandler op;
		subsystem.registerReadOnlyAttribute(RUNTIME_VERSION, new GetRuntimeVersion(RUNTIME_VERSION), Storage.RUNTIME); 
		subsystem.registerReadOnlyAttribute(ACTIVE_SESSION_COUNT, new GetActiveSessionsCount(ACTIVE_SESSION_COUNT), Storage.RUNTIME); 
		
		op = new GetActiveSessions(ACTIVE_SESSIONS);
		subsystem.registerOperationHandler(ACTIVE_SESSIONS, op, op); 
		
		op = new GetRequestsPerSession(REQUESTS_PER_SESSION);
		subsystem.registerOperationHandler(REQUESTS_PER_SESSION, op, op);

		op = new GetRequestsPerVDB(REQUESTS_PER_VDB);
		subsystem.registerOperationHandler(REQUESTS_PER_VDB, op, op);
		
		op = new GetLongRunningQueries(LONG_RUNNING_QUERIES);
		subsystem.registerOperationHandler(LONG_RUNNING_QUERIES, op, op);		
		
		op = new TerminateSession(TERMINATE_SESSION);
		subsystem.registerOperationHandler(TERMINATE_SESSION, op, op);	
		
		op = new CancelQuery(CANCEL_QUERY);
		subsystem.registerOperationHandler(CANCEL_QUERY, op, op);		
		
		op = new CacheTypes(CACHE_TYPES);
		subsystem.registerOperationHandler(CACHE_TYPES, op, op);	
		
		op = new ClearCache(CLEAR_CACHE);
		subsystem.registerOperationHandler(CLEAR_CACHE, op, op);	
		
		op = new CacheStatistics(CACHE_STATISTICS);
		subsystem.registerOperationHandler(CACHE_STATISTICS, op, op);		
		
		op = new WorkerPoolStatistics(WORKERPOOL_STATISTICS);
		subsystem.registerOperationHandler(WORKERPOOL_STATISTICS, op, op);		
		
		op = new ActiveTransactions(ACTIVE_TRANSACTIONS);
		subsystem.registerOperationHandler(ACTIVE_TRANSACTIONS, op, op);	
		
		op = new TerminateTransaction(TERMINATE_TRANSACTION);
		subsystem.registerOperationHandler(TERMINATE_TRANSACTION, op, op);		
		
		op = new MergeVDBs(MERGE_VDBS);
		subsystem.registerOperationHandler(MERGE_VDBS, op, op);	
		
		op = new ExecuteQuery(EXECUTE_QUERY);
		subsystem.registerOperationHandler(EXECUTE_QUERY, op, op);			
	}

	@Override
	public void initializeParsers(ExtensionParsingContext context) {
		context.setSubsystemXmlMapping(Namespace.CURRENT.getUri(), parser);
	}
	
	
	

}
