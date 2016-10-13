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

package org.teiid.query.processor;

import static org.teiid.query.analysis.AnalysisRecord.*;

import java.util.Arrays;
import java.util.List;

import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.language.SQLConstants;
import org.teiid.metadata.Database;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.DatabaseStorage;
import org.teiid.query.metadata.DatabaseStore;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.AlterProcedure;
import org.teiid.query.sql.lang.AlterTrigger;
import org.teiid.query.sql.lang.AlterView;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.util.CommandContext;

public class DdlPlan extends ProcessorPlan {
	
    class AlterProcessor extends LanguageVisitor {
    	DQPWorkContext workContext = DQPWorkContext.getWorkContext();
    	
    	private MetadataRepository getMetadataRepository(VDBMetaData vdb, String schemaName) {
    		ModelMetaData model = vdb.getModel(schemaName);
    		return model.getAttachment(MetadataRepository.class);
    	}
    	
    	@Override
    	public void visit(AlterView obj) {
    		VDBMetaData vdb = workContext.getVDB();
    		Table t = (Table)obj.getTarget().getMetadataID();
    		String sql = obj.getDefinition().toString();
			if (getMetadataRepository(vdb, t.getParent().getName()) != null) {
				getMetadataRepository(vdb, t.getParent().getName()).setViewDefinition(workContext.getVdbName(), workContext.getVdbVersion(), t, sql);
			}
    		alterView(vdb, t, sql);
    		if (pdm.getEventDistributor() != null) {
    			pdm.getEventDistributor().setViewDefinition(workContext.getVdbName(), workContext.getVdbVersion(), t.getParent().getName(), t.getName(), sql);
			}
    	}

    	@Override
    	public void visit(AlterProcedure obj) {
    		VDBMetaData vdb = workContext.getVDB();
    		Procedure p = (Procedure)obj.getTarget().getMetadataID();
    		String sql = obj.getDefinition().toString();
			if (getMetadataRepository(vdb, p.getParent().getName()) != null) {
				getMetadataRepository(vdb, p.getParent().getName()).setProcedureDefinition(workContext.getVdbName(), workContext.getVdbVersion(), p, sql);
			}
    		alterProcedureDefinition(vdb, p, sql);
    		if (pdm.getEventDistributor() != null) {
    			pdm.getEventDistributor().setProcedureDefinition(workContext.getVdbName(), workContext.getVdbVersion(), p.getParent().getName(), p.getName(), sql);
			}
    	}

    	@Override
    	public void visit(AlterTrigger obj) {
    		VDBMetaData vdb = workContext.getVDB();
    		Table t = (Table)obj.getTarget().getMetadataID();
    		String sql = null;
    		TriggerEvent event = obj.getEvent();
    		if (obj.getEnabled() == null) {
    			if (obj.isCreate()) {
        			if (getPlanForEvent(t, event) != null) {
        				 throw new TeiidRuntimeException(new TeiidProcessingException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30156, t.getName(), obj.getEvent())));
        			}
    			} else if (getPlanForEvent(t, event) == null) {
    				 throw new TeiidRuntimeException(new TeiidProcessingException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30158, t.getName(), obj.getEvent())));
    			}
    			sql = obj.getDefinition().toString();
    		} else if (getPlanForEvent(t, event) == null) {
				 throw new TeiidRuntimeException(new TeiidProcessingException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30158, t.getName(), obj.getEvent())));
    		}
			if (getMetadataRepository(vdb, t.getParent().getName()) != null) {
				if (sql != null) {
					getMetadataRepository(vdb, t.getParent().getName()).setInsteadOfTriggerDefinition(workContext.getVdbName(), workContext.getVdbVersion(), t, obj.getEvent(), sql);
				} else {
					getMetadataRepository(vdb, t.getParent().getName()).setInsteadOfTriggerEnabled(workContext.getVdbName(), workContext.getVdbVersion(), t, obj.getEvent(), obj.getEnabled());
				}
			}
    		alterInsteadOfTrigger(vdb, t, sql, obj.getEnabled(), event);
    		if (pdm.getEventDistributor() != null) {
    			pdm.getEventDistributor().setInsteadOfTriggerDefinition(workContext.getVdbName(), workContext.getVdbVersion(), t.getParent().getName(), t.getName(), obj.getEvent(), sql, obj.getEnabled());
			}
    	}
    }

	public static void alterView(final VDBMetaData vdb, final Table t, final String sql) {
		TransformationMetadata metadata = vdb.getAttachment(TransformationMetadata.class);
		DatabaseStorage storage = vdb.getAttachment(DatabaseStorage.class);
		
		try {
			Command command = QueryParser.getQueryParser().parseCommand(t.getSelectTransformation());
			QueryResolver.resolveCommand(command, metadata);
			MetadataValidator.determineDependencies(t, command);
		} catch (TeiidException e) {
			//should have been caught in validation, but this logic
			//is also not mature so since there is no lock on the vdb
			//it is possible that the plan is no longer valid at this point due
			//to a concurrent execution
		}
		t.setSelectTransformation(sql);
		t.setLastModified(System.currentTimeMillis());
		metadata.addToMetadataCache(t, "transformation/"+SQLConstants.Reserved.SELECT, null); //$NON-NLS-1$
		
		if (storage != null) {
			DatabaseStore store = storage.getStore();
			alterDatabaseStore(store, vdb.getName(), vdb.getVersion(), new DDLChange() {
				@Override
				public void process(DatabaseStore store) {
					store.databaseSwitched(vdb.getName(), vdb.getVersion());
					store.schemaSwitched(t.getParent().getName());
					store.tableModified(t);
				}
			});
		}
	}
	
	public interface DDLChange {
		void process(DatabaseStore store);
	}
	public static void alterDatabaseStore(DatabaseStore store, String vdbName, String version, DDLChange change) {
		Database db = store.getDatabase(vdbName, version);
		if (db != null) {
			store.startEditing();
			try {
				change.process(store);
			} finally {
				store.stopEditing();
			}
		}		
	}

	public static void alterProcedureDefinition(final VDBMetaData vdb, final Procedure p, String sql) {
		TransformationMetadata metadata = vdb.getAttachment(TransformationMetadata.class);
		DatabaseStorage storage = vdb.getAttachment(DatabaseStorage.class);
		
		try {
			Command command = QueryParser.getQueryParser().parseProcedure(p.getQueryPlan(), false);
			QueryResolver.resolveCommand(command, new GroupSymbol(p.getFullName()), Command.TYPE_STORED_PROCEDURE, metadata, false);
			MetadataValidator.determineDependencies(p, command);
		} catch (TeiidException e) {
			//should have been caught in validation, but this logic
			//is also not mature so since there is no lock on the vdb
			//it is possible that the plan is no longer valid at this point due
			//to a concurrent execution
		}
		p.setQueryPlan(sql);
		p.setLastModified(System.currentTimeMillis());
		metadata.addToMetadataCache(p, "transformation/"+StoredProcedure.class.getSimpleName().toUpperCase(), null); //$NON-NLS-1$
		
		if (storage != null) {
			DatabaseStore store = storage.getStore();
			alterDatabaseStore(store, vdb.getName(), vdb.getVersion(), new DDLChange() {
				@Override
				public void process(DatabaseStore store) {
					store.databaseSwitched(vdb.getName(), vdb.getVersion());
					store.schemaSwitched(p.getParent().getName());
					store.procedureModified(p);
				}
			});
		}		
	}

	public static void alterInsteadOfTrigger(final VDBMetaData vdb, final Table t,
			final String sql, final Boolean enabled, final TriggerEvent event) {
		switch (event) {
		case DELETE:
			if (sql != null) {
				t.setDeletePlan(sql);
			} else {
				t.setDeletePlanEnabled(enabled);
			}
			break;
		case INSERT:
			if (sql != null) {
				t.setInsertPlan(sql);
			} else {
				t.setInsertPlanEnabled(enabled);
			}
			break;
		case UPDATE:
			if (sql != null) {
				t.setUpdatePlan(sql);
			} else {
				t.setUpdatePlanEnabled(enabled);
			}
			break;
		}
		TransformationMetadata indexMetadata = vdb.getAttachment(TransformationMetadata.class);
		indexMetadata.addToMetadataCache(t, "transformation/"+event, null); //$NON-NLS-1$
		t.setLastModified(System.currentTimeMillis());
		
		DatabaseStorage storage = vdb.getAttachment(DatabaseStorage.class);
		if (storage != null) {
			DatabaseStore store = storage.getStore();
			alterDatabaseStore(store, vdb.getName(), vdb.getVersion(), new DDLChange() {
				@Override
				public void process(DatabaseStore store) {
					store.databaseSwitched(vdb.getName(), vdb.getVersion());
					store.schemaSwitched(t.getParent().getName());
					store.setTableTriggerPlan(t.getName(), event, sql);
					store.enableTableTriggerPlan(t.getName(), event, enabled);
				}
			});
		}		
	}
	
	private static String getPlanForEvent(Table t, TriggerEvent event) {
		switch (event) {
		case DELETE:
			return t.getDeletePlan();
		case INSERT:
			return t.getInsertPlan();
		case UPDATE:
			return t.getUpdatePlan();
		}
		throw new AssertionError();
	}

	private Command command;
	private ProcessorDataManager pdm;
	
	public DdlPlan(Command command) {
		this.command = command;
	}

	@Override
	public ProcessorPlan clone() {
		return new DdlPlan(command);
	}

	@Override
	public void close() throws TeiidComponentException {
	}

	@Override
	public List getOutputElements() {
		return command.getProjectedSymbols();
	}

	@Override
	public void initialize(CommandContext context,
			ProcessorDataManager dataMgr, BufferManager bufferMgr) {
		this.setContext(context);
		this.pdm = dataMgr;
	}
	
	@Override
	public TupleBatch nextBatch() throws BlockedException,
			TeiidComponentException, TeiidProcessingException {
		TupleBatch tupleBatch = new TupleBatch(1, new List[] {Arrays.asList(0)});
		tupleBatch.setTerminationFlag(true);
		return tupleBatch;
	}

	@Override
	public void open() throws TeiidComponentException, TeiidProcessingException {
		AlterProcessor ap = new AlterProcessor();
		ap.workContext = DQPWorkContext.getWorkContext();
		try {
			command.acceptVisitor(ap);
		} catch (TeiidRuntimeException e) {
			throw (TeiidProcessingException)e.getCause();
		}
	}
	
	@Override
	public PlanNode getDescriptionProperties() {
		PlanNode props = super.getDescriptionProperties();
        props.addProperty(PROP_SQL, this.command.toString());
        return props;
	}
	
	@Override
	public String toString() {
        return command.toString();
	}
	
}
