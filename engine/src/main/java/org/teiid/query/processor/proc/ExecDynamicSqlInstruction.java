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

package org.teiid.query.processor.proc;

import static org.teiid.query.analysis.AnalysisRecord.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.QueryProcessingException;
import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.id.IDGenerator;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.internal.process.Request;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.QueryOptimizer;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolveVirtualGroupCriteriaVisitor;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.ProcedureReservedWords;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Create;
import org.teiid.query.sql.lang.DynamicCommand;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.SetClause;
import org.teiid.query.sql.proc.CreateUpdateProcedureCommand;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.util.VariableContext;
import org.teiid.query.util.CommandContext;
import org.teiid.query.validator.ValidationVisitor;


/**
 * <p>
 * Executes a SQL statement, and remove its results from the buffer manager.
 * Executing this instruction does not modify the values of any of the
 * variables, hence it's results are not important so they are removed
 * immediately.
 * </p>
 */
public class ExecDynamicSqlInstruction extends ProgramInstruction {
    
	// the DynamicCommand
	private DynamicCommand dynamicCommand;

	// the idGenerator
	IDGenerator idGenerator;

	// the CapabilitiesFinder
	CapabilitiesFinder capFinder;

	// the metadata for this plan
	private QueryMetadataInterface metadata;

	// The parent command
	CreateUpdateProcedureCommand parentProcCommand;
    
    private Program dynamicProgram;

	public ExecDynamicSqlInstruction(
			CreateUpdateProcedureCommand parentProcCommand,
			DynamicCommand command, QueryMetadataInterface metadata,
			IDGenerator idGenerator, CapabilitiesFinder capFinder) {
		this.parentProcCommand = parentProcCommand;
		this.dynamicCommand = command;
		this.metadata = metadata;
		this.capFinder = capFinder;
		this.idGenerator = idGenerator;
	}

	/**
	 * <p>
	 * Processing this instruction executes the ProcessorPlan for the command on
	 * the CommandStatement of the update procedure language. Executing this
	 * plan does not effect the values of any of the variables defined as part
	 * of the update procedure and hence the results of the ProcessPlan
	 * execution need not be stored for further processing. The results are
	 * removed from the buffer manager immediately after execution. The program
	 * counter is incremented after execution of the plan.
	 * </p>
	 * 
	 * @throws BlockedException
	 *             if this processing the plan throws a currentVarContext
	 */
	public void process(ProcedurePlan procEnv) throws BlockedException,
			TeiidComponentException, TeiidProcessingException {

		VariableContext localContext = procEnv.getCurrentVariableContext();

		try {
			Object value = procEnv.evaluateExpression(dynamicCommand.getSql());

			if (value == null) {
				throw new QueryProcessingException(QueryPlugin.Util
						.getString("ExecDynamicSqlInstruction.0")); //$NON-NLS-1$
			}

			LogManager.logTrace(org.teiid.logging.LogConstants.CTX_DQP,
					new Object[] { "Executing dynamic sql ", value }); //$NON-NLS-1$

			Command command = QueryParser.getQueryParser().parseCommand(value.toString());
			command.setExternalGroupContexts(dynamicCommand.getExternalGroupContexts());
			command.setTemporaryMetadata(dynamicCommand.getTemporaryMetadata());
			updateContextWithUsingValues(procEnv, localContext);
			
			Map tempMetadata = command.getTemporaryMetadata();
			final TempMetadataStore metadataStore = new TempMetadataStore(tempMetadata);
            
            if (dynamicCommand.getUsing() != null
                            && !dynamicCommand.getUsing().isEmpty()) {
                metadataStore.addTempGroup(Reserved.USING, new LinkedList(dynamicCommand.getUsing().getClauseMap().keySet()));
                GroupSymbol using = new GroupSymbol(Reserved.USING);
                using.setMetadataID(metadataStore.getTempGroupID(Reserved.USING));
                command.addExternalGroupToContext(using);
                metadataStore.addTempGroup(ProcedureReservedWords.DVARS, new LinkedList(dynamicCommand.getUsing().getClauseMap().keySet()));
                using = new GroupSymbol(ProcedureReservedWords.DVARS);
                using.setMetadataID(metadataStore.getTempGroupID(ProcedureReservedWords.DVARS));
                command.addExternalGroupToContext(using);
            }

			// Resolve any groups
			if (parentProcCommand.isUpdateProcedure()) {
				ResolveVirtualGroupCriteriaVisitor.resolveCriteria(command,
						parentProcCommand.getVirtualGroup(), metadata);
			}

			QueryResolver.resolveCommand(command, metadata.getDesignTimeMetadata());

			validateDynamicCommand(procEnv, command);

			// create a new set of variables including vars
			Map<ElementSymbol, Expression> nameValueMap = createVariableValuesMap(localContext);
            nameValueMap.putAll(QueryResolver.getVariableValues(parentProcCommand.getUserCommand(), false, metadata));
            ValidationVisitor visitor = new ValidationVisitor();
            visitor.setUpdateProc(parentProcCommand);
            Request.validateWithVisitor(visitor, metadata, command);

            if (dynamicCommand.getAsColumns() != null
					&& !dynamicCommand.getAsColumns().isEmpty()) {
        		command = QueryRewriter.createInlineViewQuery(new GroupSymbol("X"), command, metadata, dynamicCommand.getAsColumns()); //$NON-NLS-1$
				if (dynamicCommand.getIntoGroup() != null) {
					Insert insert = new Insert(dynamicCommand.getIntoGroup(), dynamicCommand.getAsColumns(), Collections.emptyList());
					insert.setQueryExpression((Query)command);
					command = insert;
				}
			}
            
			command = QueryRewriter.rewrite(command, parentProcCommand, metadata,
					procEnv.getContext(), nameValueMap, parentProcCommand.getUserCommand().getType());

            ProcessorPlan commandPlan = QueryOptimizer.optimizePlan(command, metadata,
					idGenerator, capFinder, AnalysisRecord
							.createNonRecordingRecord(), procEnv
							.getContext());
            
			CreateCursorResultSetInstruction inst = new CreateCursorResultSetInstruction(CreateCursorResultSetInstruction.RS_NAME, commandPlan, dynamicCommand.getIntoGroup() != null) {
				@Override
				public void process(ProcedurePlan procEnv)
						throws BlockedException, TeiidComponentException,
						TeiidProcessingException {
					super.process(procEnv);
					procEnv.getContext().popCall();
				}
			};

            dynamicProgram = new Program(false);
            dynamicProgram.addInstruction(inst);

            if (dynamicCommand.getIntoGroup() != null) {
                String groupName = dynamicCommand.getIntoGroup().getCanonicalName();
                if (!procEnv.getTempTableStore().getAllTempTables().contains(groupName)) {
                	//create the temp table in the parent scope
                	Create create = new Create();
                	create.setTable(new GroupSymbol(groupName));
                	for (ElementSymbol es : ((Insert)command).getVariables()) {
                		Column c = new Column();
                		c.setName(es.getShortName());
                		c.setRuntimeType(DataTypeManager.getDataTypeName(es.getType()));
                		create.getColumns().add(c);
                	}
                    procEnv.getDataManager().registerRequest(procEnv.getContext(), create, TempMetadataAdapter.TEMP_MODEL.getName(), null, 0, -1);
                }
            }

            procEnv.push(dynamicProgram);
		} catch (TeiidProcessingException e) {
			Object[] params = {dynamicCommand, dynamicCommand.getSql(), e.getMessage()};
			throw new QueryProcessingException(e, QueryPlugin.Util.getString("ExecDynamicSqlInstruction.couldnt_execute", params)); //$NON-NLS-1$
		} 
	}

	/**
	 * @param procEnv
	 * @param localContext
	 * @throws TeiidComponentException
	 * @throws TeiidComponentException
	 * @throws TeiidProcessingException 
	 */
	private void updateContextWithUsingValues(ProcedurePlan procEnv,
			VariableContext localContext) throws TeiidComponentException, TeiidProcessingException {
		if (dynamicCommand.getUsing() != null
				&& !dynamicCommand.getUsing().isEmpty()) {
			for (SetClause setClause : dynamicCommand.getUsing().getClauses()) {
				Object assignment = procEnv.evaluateExpression(setClause.getValue());

				LogManager.logTrace(org.teiid.logging.LogConstants.CTX_DQP,
						new Object[] { this, " The using variable ", //$NON-NLS-1$
						setClause.getSymbol(), " has value :", assignment }); //$NON-NLS-1$
				localContext.setValue(setClause.getSymbol(), assignment);
				ElementSymbol es = setClause.getSymbol().clone();
				es.getGroupSymbol().setShortName(Reserved.USING);
				localContext.setValue(es, assignment);
			}
		}
	}
    
	/**
	 * @param localContext
	 * @return
	 */
	private Map<ElementSymbol, Expression> createVariableValuesMap(VariableContext localContext) {
		Map<ElementSymbol, Object> variableMap = new HashMap<ElementSymbol, Object>();
		localContext.getFlattenedContextMap(variableMap);
		Map<ElementSymbol, Expression> nameValueMap = new HashMap<ElementSymbol, Expression>(variableMap.size());
		for (Map.Entry<ElementSymbol, Object> entry : variableMap.entrySet()) {
			nameValueMap.put(entry.getKey(), new Constant(entry.getValue(), entry.getKey().getType()));
		}
		return nameValueMap;
	}

	/**
	 * @param procEnv
	 * @param command
	 * @throws TeiidComponentException
	 * @throws QueryProcessingException
	 */
	private void validateDynamicCommand(ProcedurePlan procEnv,
			Command command) throws TeiidComponentException,
			QueryProcessingException {
		// validate project symbols
		List dynamicExpectedColumns = dynamicCommand.getAsColumns();
		List<SingleElementSymbol> sourceProjectedSymbolList = command.getProjectedSymbols();

		if (dynamicExpectedColumns != null && !dynamicExpectedColumns.isEmpty()) {
			if (dynamicExpectedColumns.size() != sourceProjectedSymbolList.size()) {
				throw new QueryProcessingException(QueryPlugin.Util
						.getString("ExecDynamicSqlInstruction.4")); //$NON-NLS-1$
			}
			// If there is only one project symbol, we won't validate the name.

			Iterator dynamicIter = dynamicExpectedColumns.iterator();
			Iterator<SingleElementSymbol> sourceIter = sourceProjectedSymbolList.iterator();
			// Check for proper element name and datatype definition in the
			// dynamic SQL
			// If the projected symbol list equal to 1, we won't bother checking
			// the name.
			while (dynamicIter.hasNext()) {
				SingleElementSymbol dynamicSymbol = (SingleElementSymbol) dynamicIter.next();
				Class<?> sourceSymbolDatatype = sourceIter.next().getType();

				// Check if the the dynamic sql element types are equal or
				// implicitly convertible to the source types
				Class<?> dynamicType = dynamicSymbol.getType();
				String dynamicTypeName = DataTypeManager
						.getDataTypeName(dynamicType);
				String sourceTypeName = DataTypeManager
						.getDataTypeName(sourceSymbolDatatype);
				if (!dynamicTypeName.equals(sourceTypeName)
						&& // If the types aren't the same, and...
						!DataTypeManager.isImplicitConversion(sourceTypeName,
								dynamicTypeName)) { // if there's no implicit
					// conversion between the
					// two
					Object[] params = new Object[] { sourceTypeName,
							dynamicSymbol.getShortName(),
							dynamicTypeName };
					throw new QueryProcessingException(QueryPlugin.Util
							.getString("ExecDynamicSqlInstruction.6", params)); //$NON-NLS-1$
				}
			}
		}

		// do a recursion check
		// Add group to recursion stack
		CommandContext context = procEnv.getContext();
		context.pushCall(parentProcCommand.getVirtualGroup().getCanonicalName());
	}

	/**
	 * Returns a deep clone
	 */
	public ExecDynamicSqlInstruction clone() {
		ExecDynamicSqlInstruction clone = new ExecDynamicSqlInstruction(
				parentProcCommand, dynamicCommand, metadata, idGenerator, capFinder);
		return clone;
	}

	public String toString() {
		return "ExecDynamicSqlInstruction"; //$NON-NLS-1$
	}

	public PlanNode getDescriptionProperties() {
		PlanNode props = new PlanNode("ExecDynamicSqlInstruction"); //$NON-NLS-1$
		props.addProperty(PROP_SQL, dynamicCommand.toString()); 
		return props;
	}

}
