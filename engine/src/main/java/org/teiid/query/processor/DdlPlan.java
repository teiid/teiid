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

import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.language.SQLConstants;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.AlterProcedure;
import org.teiid.query.sql.lang.AlterTrigger;
import org.teiid.query.sql.lang.AlterView;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.util.CommandContext;

public class DdlPlan extends ProcessorPlan {
	
    class AlterProcessor extends LanguageVisitor {
    	DQPWorkContext workContext = DQPWorkContext.getWorkContext();
    	
    	@Override
    	public void visit(AlterView obj) {
    		VDBMetaData vdb = workContext.getVDB();
    		Table t = (Table)obj.getTarget().getMetadataID();
    		String sql = obj.getDefinition().toString();
			if (pdm.getMetadataRepository() != null) {
				pdm.getMetadataRepository().setViewDefinition(workContext.getVdbName(), workContext.getVdbVersion(), t, sql);
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
			if (pdm.getMetadataRepository() != null) {
				pdm.getMetadataRepository().setProcedureDefinition(workContext.getVdbName(), workContext.getVdbVersion(), p, sql);
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
        				throw new TeiidRuntimeException(new TeiidProcessingException(QueryPlugin.Util.getString("DdlPlan.event_already_exists", t.getName(), obj.getEvent()))); //$NON-NLS-1$
        			}
    			} else if (getPlanForEvent(t, event) == null) {
    				throw new TeiidRuntimeException(new TeiidProcessingException(QueryPlugin.Util.getString("DdlPlan.event_not_exists", t.getName(), obj.getEvent()))); //$NON-NLS-1$
    			}
    			sql = obj.getDefinition().toString();
    		} else if (getPlanForEvent(t, event) == null) {
				throw new TeiidRuntimeException(new TeiidProcessingException(QueryPlugin.Util.getString("DdlPlan.event_not_exists", t.getName(), obj.getEvent()))); //$NON-NLS-1$
    		}
			if (pdm.getMetadataRepository() != null) {
				if (sql != null) {
					pdm.getMetadataRepository().setInsteadOfTriggerDefinition(workContext.getVdbName(), workContext.getVdbVersion(), t, obj.getEvent(), sql);
				} else {
					pdm.getMetadataRepository().setInsteadOfTriggerEnabled(workContext.getVdbName(), workContext.getVdbVersion(), t, obj.getEvent(), obj.getEnabled());
				}
			}
    		alterInsteadOfTrigger(vdb, t, sql, obj.getEnabled(), event);
    		if (pdm.getEventDistributor() != null) {
    			pdm.getEventDistributor().setInsteadOfTriggerDefinition(workContext.getVdbName(), workContext.getVdbVersion(), t.getParent().getName(), t.getName(), obj.getEvent(), sql, obj.getEnabled());
			}
    	}
    }

	public static void alterView(VDBMetaData vdb, Table t, String sql) {
		t.setSelectTransformation(sql);
		t.setLastModified(System.currentTimeMillis());
		TransformationMetadata indexMetadata = vdb.getAttachment(TransformationMetadata.class);
		indexMetadata.addToMetadataCache(t, "transformation/"+SQLConstants.Reserved.SELECT, null); //$NON-NLS-1$
	}

	public static void alterProcedureDefinition(VDBMetaData vdb, Procedure p, String sql) {
		p.setQueryPlan(sql);
		p.setLastModified(System.currentTimeMillis());
		TransformationMetadata indexMetadata = vdb.getAttachment(TransformationMetadata.class);
		indexMetadata.addToMetadataCache(p, "transformation/"+StoredProcedure.class.getSimpleName().toUpperCase(), null); //$NON-NLS-1$
	}

	public static void alterInsteadOfTrigger(VDBMetaData vdb, Table t,
			String sql, Boolean enabled, TriggerEvent event) {
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
	
}
