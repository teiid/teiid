/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.query.processor.proc;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.api.exception.query.QueryProcessingException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.id.IDGenerator;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.eval.ExpressionEvaluator;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.execution.multisource.PlanModifier;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.TempMetadataStore;
import com.metamatrix.query.optimizer.QueryOptimizer;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.program.Program;
import com.metamatrix.query.processor.program.ProgramEnvironment;
import com.metamatrix.query.processor.program.ProgramInstruction;
import com.metamatrix.query.processor.xml.ProcessorInstruction;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.resolver.util.ResolveVirtualGroupCriteriaVisitor;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.rewriter.VariableSubstitutionVisitor;
import com.metamatrix.query.sql.ProcedureReservedWords;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.DynamicCommand;
import com.metamatrix.query.sql.lang.From;
import com.metamatrix.query.sql.lang.Into;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.Select;
import com.metamatrix.query.sql.lang.SetClause;
import com.metamatrix.query.sql.lang.SubqueryFromClause;
import com.metamatrix.query.sql.proc.CreateUpdateProcedureCommand;
import com.metamatrix.query.sql.symbol.AliasSymbol;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.ExpressionSymbol;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.util.VariableContext;
import com.metamatrix.query.sql.visitor.GroupCollectorVisitor;
import com.metamatrix.query.sql.visitor.ReferenceCollectorVisitor;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.LogConstants;

/**
 * <p>
 * Executes a SQL statement, and remove its results from the buffer manager.
 * Executing this instruction does not modify the values of any of the
 * variables, hence it's results are not important so they are removed
 * immediately.
 * </p>
 */
public class ExecDynamicSqlInstruction extends CommandInstruction {
    
    private static class PopCallInstruction extends ProgramInstruction {

        /** 
         * @see com.metamatrix.query.processor.program.ProgramInstruction#process(com.metamatrix.query.processor.program.ProgramEnvironment)
         */
        public void process(ProgramEnvironment env) throws MetaMatrixComponentException,
                                                   MetaMatrixProcessingException {
            ProcedureEnvironment procEnv = (ProcedureEnvironment) env;
            
            procEnv.getContext().popCall();
        }

    }
    
    private static PopCallInstruction POP_CALL_INSTRUCTION = new PopCallInstruction();

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
			DynamicCommand command, List references,
			QueryMetadataInterface metadata, IDGenerator idGenerator,
			CapabilitiesFinder capFinder) {
		this.parentProcCommand = parentProcCommand;
		this.dynamicCommand = command;
		this.metadata = metadata;
		this.capFinder = capFinder;
		this.idGenerator = idGenerator;
		setReferences(references);
	}

	/**
	 * <p>
	 * Processing this instruction executes the ProcessorPlan for the command on
	 * the CommandStatement of the update procedure language. Executing this
	 * plan does not effect the values of any of the variables defined as part
	 * of the update procedure and hence the results of the ProcessPlan
	 * execution need not be stored for furthur processing. The results are
	 * removed from the buffer manager immediately after execution. The program
	 * counter is incremented after execution of the plan.
	 * </p>
	 * 
	 * @throws BlockedException
	 *             if this processing the plan thows a currentVarContext
	 * @see ProcessorInstruction#process(ProcessorEnvironment)
	 */
	public void process(ProgramEnvironment env) throws BlockedException,
			MetaMatrixComponentException, MetaMatrixProcessingException {

		LogManager
				.logTrace(
						LogConstants.CTX_QUERY_PLANNER,
						"Processing ExecDynamicSqlInstruction as part of processing the update procedure"); //$NON-NLS-1$

		ProcedureEnvironment procEnv = (ProcedureEnvironment) env;
        
		VariableContext localContext = procEnv.getCurrentVariableContext();

		// get the current set of references and set their values
		setReferenceValues(localContext);
        
		try {
			Object value = ExpressionEvaluator.evaluate(dynamicCommand
					.getSql(), Collections.EMPTY_MAP,
					Collections.EMPTY_LIST, procEnv.getDataManager(),
					procEnv.getContext());

			if (value == null) {
				throw new QueryProcessingException(QueryExecPlugin.Util
						.getString("ExecDynamicSqlInstruction.0")); //$NON-NLS-1$
			}

			LogManager.logTrace(LogConstants.CTX_QUERY_PLANNER,
					new Object[] { "Executing dynamic sql ", value }); //$NON-NLS-1$

			Command command = QueryParser.getQueryParser().parseCommand(value.toString());
			command.setExternalGroupContexts(dynamicCommand.getExternalGroupContexts());
			command.setTemporaryMetadata(dynamicCommand.getTemporaryMetadata());
            command.setVariableValues(new HashMap(dynamicCommand.getVariableValues()));
			updateContextWithUsingValues(procEnv, localContext);
			
			Map tempMetadata = command.getTemporaryMetadata();
			final TempMetadataStore metadataStore = new TempMetadataStore(tempMetadata);
            
            if (dynamicCommand.getUsing() != null
                            && !dynamicCommand.getUsing().isEmpty()) {
                metadataStore.addTempGroup(ProcedureReservedWords.USING, new LinkedList(dynamicCommand.getUsing().getClauseMap().keySet()));
                GroupSymbol using = new GroupSymbol(ProcedureReservedWords.USING);
                using.setMetadataID(metadataStore.getTempGroupID(ProcedureReservedWords.USING));
                command.addExternalGroupToContext(using);
            }

			// Resolve any groups
			if (parentProcCommand.isUpdateProcedure()) {
				ResolveVirtualGroupCriteriaVisitor.resolveCriteria(command,
						parentProcCommand.getVirtualGroup(), metadata);
			}

			QueryResolver.resolveCommand(command, metadata);

			validateDynamicCommand(procEnv, command);

			// create a new set of variables including vars
			Map nameValueMap = createVariableValuesMap(localContext);
			command.getVariableValues().putAll(nameValueMap);
            
            // validation visitor?

            VariableSubstitutionVisitor.substituteVariables(command, command.getVariableValues(), command.getType(), false);
            
			QueryRewriter.rewrite(command, parentProcCommand, metadata,
					procEnv.getContext());

			if (dynamicCommand.getAsColumns() != null
					&& !dynamicCommand.getAsColumns().isEmpty()) {
				command = wrapCommand(command);
			}

            ProcessorPlan commandPlan = QueryOptimizer.optimizePlan(command, metadata,
					idGenerator, capFinder, AnalysisRecord
							.createNonRecordingRecord(), procEnv
							.getContext());
            
            PlanModifier multiSourcePlanModifier = (PlanModifier) procEnv.getContext().getMultiSourcePlanModifier();
            
            if (multiSourcePlanModifier != null) {
                multiSourcePlanModifier.modifyPlan(commandPlan, metadata);
            }

			List references = ReferenceCollectorVisitor.getReferences(command);

			ExecSqlInstruction inst = new ExecSqlInstruction(commandPlan,
					references, dynamicCommand.getIntoGroup());

            dynamicProgram = new Program();
            dynamicProgram.addInstruction(inst);
            dynamicProgram.addInstruction(POP_CALL_INSTRUCTION);

            if (dynamicCommand.getIntoGroup() != null) {
                String groupName = dynamicCommand.getIntoGroup().getCanonicalName();
                if (!procEnv.getTempTableStore().getAllTempTables().contains(groupName)) {
                    procEnv.getTempContext().add(groupName);
                }
            }

            procEnv.push(dynamicProgram);
		} catch (MetaMatrixProcessingException e) {
			Object[] params = {dynamicCommand, dynamicCommand.getSql(), e.getMessage()};
			throw new QueryProcessingException(e, QueryExecPlugin.Util.getString("ExecDynamicSqlInstruction.couldnt_execute", params)); //$NON-NLS-1$
		} 
	}

	/**
	 * @param procEnv
	 * @param localContext
	 * @throws MetaMatrixComponentException
	 * @throws ExpressionEvaluationException
	 * @throws MetaMatrixComponentException
	 */
	private void updateContextWithUsingValues(ProcedureEnvironment procEnv,
			VariableContext localContext) throws ExpressionEvaluationException,
			MetaMatrixComponentException {
		if (dynamicCommand.getUsing() != null
				&& !dynamicCommand.getUsing().isEmpty()) {
			for (SetClause setClause : dynamicCommand.getUsing().getClauses()) {
				Object assignment = ExpressionEvaluator.evaluate(
						setClause.getValue(), Collections.EMPTY_MAP,
						Collections.EMPTY_LIST, procEnv.getDataManager(),
						procEnv.getContext());

				LogManager.logTrace(LogConstants.CTX_QUERY_PLANNER,
						new Object[] { this, " The using variable ", //$NON-NLS-1$
						setClause.getSymbol(), " has value :", assignment }); //$NON-NLS-1$
				localContext.setValue(setClause.getSymbol(),
						assignment);
			}
		}
	}
    
	/**
	 * @param command
	 * @return
	 * @throws QueryResolverException
	 * @throws MetaMatrixComponentException
	 */
	private Query wrapCommand(Command command) throws QueryResolverException,
			MetaMatrixComponentException {
		Collection groups = GroupCollectorVisitor.getGroups(command, true);
		int i = 0;
		String subquery_group_name = "X"; //$NON-NLS-1$
		GroupSymbol groupSymbol = new GroupSymbol(subquery_group_name);
		while (groups.contains(groupSymbol)) {
			subquery_group_name = "X" + i; //$NON-NLS-1$
			i++;
			groupSymbol = new GroupSymbol(subquery_group_name);
		}

		Select select = new Select();
		
		Map shortNameMap = createShortNameMap(command.getProjectedSymbols());

		Iterator columns = dynamicCommand.getAsColumns().iterator();
		while (columns.hasNext()) {
			Class projectedSymbolType = null;
			ElementSymbol expectedSymbol = (ElementSymbol) columns.next();
			String expectedName = expectedSymbol.getShortCanonicalName();
			String actualName = expectedName;
			boolean shouldAlias = false;

			if (dynamicCommand.getAsColumns().size() == 1) {
				SingleElementSymbol projectedSymbol = (SingleElementSymbol) command
						.getProjectedSymbols().get(0);
				actualName = projectedSymbol.getShortCanonicalName();
				projectedSymbolType = projectedSymbol.getType();
				shouldAlias = !actualName.equals(expectedName);
			} else {
				projectedSymbolType = (Class)shortNameMap.get(expectedName); 
			}
			SingleElementSymbol wrappedSymbol = new ElementSymbol(groupSymbol
					.getCanonicalName()
					+ SingleElementSymbol.SEPARATOR
					+ actualName);

            Expression result = ResolverUtil.convertExpression(wrappedSymbol, DataTypeManager.getDataTypeName(projectedSymbolType), DataTypeManager.getDataTypeName(expectedSymbol.getType()));
            
			if (!(result instanceof SingleElementSymbol)) {
				wrappedSymbol = new ExpressionSymbol(expectedName, result);
				shouldAlias = true;
			}
			if (shouldAlias) {
				wrappedSymbol = new AliasSymbol(expectedName, wrappedSymbol);
			}
			
			select.addSymbol(wrappedSymbol);
		}
		Query query = new Query();
		query.setSelect(select);
		From from = new From();
		SubqueryFromClause sqfc = new SubqueryFromClause(subquery_group_name,
				command);
		from.addClause(sqfc);
		query.setFrom(from);
		if (dynamicCommand.getIntoGroup() != null) {
			query.setInto(new Into(dynamicCommand.getIntoGroup()));
		}
		QueryResolver.resolveCommand(query, metadata);
		return query;
	}

	/**
	 * @param localContext
	 * @return
	 */
	private Map createVariableValuesMap(VariableContext localContext) {
		Map variableMap = new HashMap();
		localContext.getFlattenedContextMap(variableMap);
		Map nameValueMap = new HashMap(variableMap.size());
		Iterator entries = variableMap.entrySet().iterator();
		while (entries.hasNext()) {
			Map.Entry entry = (Map.Entry) entries.next();
			nameValueMap.put(((ElementSymbol) entry.getKey())
					.getCanonicalName(), new Constant(entry.getValue()));
		}

		nameValueMap.putAll(dynamicCommand.getVariableValues());
		return nameValueMap;
	}

	/**
	 * @param procEnv
	 * @param command
	 * @throws MetaMatrixComponentException
	 * @throws QueryProcessingException
	 */
	private void validateDynamicCommand(ProcedureEnvironment procEnv,
			Command command) throws MetaMatrixComponentException,
			QueryProcessingException {
		// validate project symbols
		List dynamicExpectedColumns = dynamicCommand.getAsColumns();
		List sourceProjectedSymbolList = command.getProjectedSymbols();

		if (dynamicExpectedColumns != null && !dynamicExpectedColumns.isEmpty()) {
			if (dynamicExpectedColumns.size() != sourceProjectedSymbolList
					.size()) {
				throw new QueryProcessingException(QueryExecPlugin.Util
						.getString("ExecDynamicSqlInstruction.4")); //$NON-NLS-1$
			}
			// If there is only one project symbol, we won't validate the name.

			Iterator dynamicIter = dynamicExpectedColumns.iterator();
			Map sourceMap = createShortNameMap(sourceProjectedSymbolList);

			// Check for proper element name and datatype definition in the
			// dynamic SQL
			// If the projected symbol list equal to 1, we won't bother checking
			// the name.
			while (dynamicIter.hasNext()) {
				SingleElementSymbol dynamicSymbol = (SingleElementSymbol) dynamicIter
						.next();
				Object sourceSymbolDatatype = null;
				// If the project list is greater than one, use the dynamic
				// symbol
				// to get the source project. Otherwise just get the one and
				// only.
				if (dynamicExpectedColumns.size() > 1) {
					sourceSymbolDatatype = sourceMap.get(dynamicSymbol
							.getShortCanonicalName());

					if (sourceSymbolDatatype == null) {
						Object[] params = new Object[] { dynamicSymbol
								.getShortCanonicalName() };
						throw new QueryProcessingException(QueryExecPlugin.Util
								.getString(
										"ExecDynamicSqlInstruction.5", params)); //$NON-NLS-1$
					}
				} else {
					sourceSymbolDatatype = sourceMap.values().iterator().next();
				}

				// Check if the the dynamic sql element types are equal or
				// implicitly convertible to the source types
				Class dynamicType = dynamicSymbol.getType();
				String dynamicTypeName = DataTypeManager
						.getDataTypeName(dynamicType);
				String sourceTypeName = DataTypeManager
						.getDataTypeName((Class) sourceSymbolDatatype);
				if (!dynamicTypeName.equals(sourceTypeName)
						&& // If the types aren't the same, and...
						!DataTypeManager.isImplicitConversion(sourceTypeName,
								dynamicTypeName)) { // if there's no implicit
					// conversion between the
					// two
					Object[] params = new Object[] { sourceTypeName,
							dynamicSymbol.getShortCanonicalName(),
							dynamicTypeName };
					throw new QueryProcessingException(QueryExecPlugin.Util
							.getString("ExecDynamicSqlInstruction.6", params)); //$NON-NLS-1$
				}
			}
		}

		// validate the updating model count if transaction is optimistic
		// Error if orig = 0 and command > 0 OR if orig = 1 and command <> 1
		if (procEnv.getContext().isOptimisticTransaction()) {
			final int origModelCount = dynamicCommand.getUpdatingModelCount();
			final int newModelCount = command.updatingModelCount(metadata);
			if ((origModelCount == 0 || origModelCount == 1)
					&& newModelCount > origModelCount) {
				Object[] params = new Object[] { new Integer(newModelCount),
						new Integer(origModelCount) };
				throw new QueryProcessingException(QueryExecPlugin.Util
						.getString("ExecDynamicSqlInstruction.1", params)); //$NON-NLS-1$
			}
		}

		// do a recursion check
		// Add group to recursion stack
		CommandContext context = procEnv.getContext();
		if (context.pushCall(parentProcCommand.getVirtualGroup()
				.getCanonicalName())) {
			Object[] params = new Object[] { parentProcCommand
					.getVirtualGroup().getCanonicalName() };
			throw new QueryProcessingException(QueryExecPlugin.Util.getString(
					"ExecDynamicSqlInstruction.3", params)); //$NON-NLS-1$
		}
	}

	/**
	 * @param sourceProjectedSymbolList
	 * @return
	 */
	private Map createShortNameMap(List sourceProjectedSymbolList) {
		Iterator sourceIter = sourceProjectedSymbolList.iterator();
		Map sourceMap = new HashMap();

		// Load source project symbol map
		while (sourceIter.hasNext()) {
			SingleElementSymbol symbol = (SingleElementSymbol) sourceIter
					.next();
			sourceMap.put(symbol.getShortCanonicalName(), symbol.getType());
		}
		return sourceMap;
	}

	/**
	 * Returns a deep clone
	 */
	public Object clone() {
		List copyReferences = cloneReferences();

		ExecDynamicSqlInstruction clone = new ExecDynamicSqlInstruction(
				parentProcCommand, dynamicCommand, copyReferences, metadata, idGenerator, capFinder);
		return clone;
	}

	public String toString() {
		return "ExecDynamicSqlInstruction"; //$NON-NLS-1$
	}

	public Map getDescriptionProperties() {
		Map props = new HashMap();
		props.put(PROP_TYPE, "SQL"); //$NON-NLS-1$
		props.put(PROP_SQL, ExecSqlInstruction.RS_NAME); 
		if (dynamicCommand.getIntoGroup() != null) {
			props.put(PROP_GROUP, dynamicCommand.getIntoGroup().toString());
		}
		return props;
	}

	/**
	 * @see com.metamatrix.query.processor.program.ProgramInstruction#getChildPlans()
	 * @since 4.2
	 */
	public Collection getChildPlans() {
		if (this.dynamicProgram != null) {
		    return dynamicProgram.getChildPlans();
        }
        return Collections.EMPTY_LIST;
	}

}
