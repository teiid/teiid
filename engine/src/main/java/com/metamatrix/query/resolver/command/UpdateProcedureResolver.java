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

package com.metamatrix.query.resolver.command;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.api.exception.query.UnresolvedSymbolDescription;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.mapping.relational.QueryNode;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.metadata.TempMetadataStore;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.resolver.CommandResolver;
import com.metamatrix.query.resolver.ProcedureContainerResolver;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.resolver.util.ResolveVirtualGroupCriteriaVisitor;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.resolver.util.ResolverVisitor;
import com.metamatrix.query.sql.ProcedureReservedWords;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.DynamicCommand;
import com.metamatrix.query.sql.lang.GroupContext;
import com.metamatrix.query.sql.lang.SubqueryContainer;
import com.metamatrix.query.sql.proc.AssignmentStatement;
import com.metamatrix.query.sql.proc.Block;
import com.metamatrix.query.sql.proc.CommandStatement;
import com.metamatrix.query.sql.proc.CreateUpdateProcedureCommand;
import com.metamatrix.query.sql.proc.DeclareStatement;
import com.metamatrix.query.sql.proc.IfStatement;
import com.metamatrix.query.sql.proc.LoopStatement;
import com.metamatrix.query.sql.proc.Statement;
import com.metamatrix.query.sql.proc.WhileStatement;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.util.SymbolMap;
import com.metamatrix.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import com.metamatrix.query.util.ErrorMessageKeys;
import com.metamatrix.query.util.LogConstants;

/**
 */
public class UpdateProcedureResolver implements CommandResolver {

    public void resolveVirtualGroupElements(CreateUpdateProcedureCommand procCommand, boolean useMetadataCommands, QueryMetadataInterface metadata)
        throws QueryMetadataException, QueryResolverException, MetaMatrixComponentException {

		// virtual group on procedure
		GroupSymbol virtualGroup = procCommand.getVirtualGroup();

		// not set by user command resolver in case of modeler
		if(virtualGroup == null) {
	        Iterator groupIter = procCommand.getAllExternalGroups().iterator();
	        while(groupIter.hasNext()) {
	        	GroupSymbol groupSymbol = (GroupSymbol) groupIter.next();
	        	String groupName = groupSymbol.getName();
	        	if(!groupName.equalsIgnoreCase(ProcedureReservedWords.INPUT) &&
		        	 !groupName.equalsIgnoreCase(ProcedureReservedWords.CHANGING) ) {
		        	 // set the groupSymbol on the procedure
		        	 ResolverUtil.resolveGroup(groupSymbol, metadata);
	        		 procCommand.setVirtualGroup(groupSymbol);
		        	 virtualGroup = groupSymbol;
                     break;
	        	 }
	        }
		}

        // If still haven't found virtual group, the external metadata is bad
        if(virtualGroup == null) {
            throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0012, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0012));
        }

		ResolveVirtualGroupCriteriaVisitor.resolveCriteria(procCommand, virtualGroup, metadata);

		// symbol map need not be checked as we are not validating
		// in the modeler
		if(useMetadataCommands) {
	    	// get a symbol map between virtual elements and the elements that define
	    	// then in the query transformation, this info is used in evaluating/validating
	    	// has criteria/trnaslate criteria clauses
			Command transformCmd = getQueryTransformCmd(virtualGroup, metadata);
			Map symbolMap = SymbolMap.createSymbolMap(virtualGroup, (List<SingleElementSymbol>)transformCmd.getProjectedSymbols()).asMap();
	        // set the symbolMap on the procedure
			procCommand.setSymbolMap(symbolMap);
		}
    }

	/**
	 * Get the command for the transformation query that defines this virtual group.
	 */
    private Command getQueryTransformCmd(GroupSymbol virtualGroup, QueryMetadataInterface metadata)
    throws QueryMetadataException, QueryResolverException, MetaMatrixComponentException {

        Command transformCmd = null;
    	QueryNode queryNode = metadata.getVirtualPlan(virtualGroup.getMetadataID());
    	String transformQuery = queryNode.getQuery();
        try {
            transformCmd = QueryParser.getQueryParser().parseCommand(transformQuery);
        } catch(QueryParserException e) {
            throw new QueryResolverException(e, ErrorMessageKeys.RESOLVER_0013, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0013, virtualGroup));
        }

        QueryResolver.resolveCommand(transformCmd, metadata);

        return transformCmd;
    }

    /**
     * @see com.metamatrix.query.resolver.CommandResolver#resolveCommand(com.metamatrix.query.sql.lang.Command, java.util.Collection, TempMetadataAdapter, AnalysisRecord, boolean)
     */
    public void resolveCommand(Command command, boolean useMetadataCommands, TempMetadataAdapter metadata, AnalysisRecord analysis, boolean resolveNullLiterals)
        throws QueryMetadataException, QueryResolverException, MetaMatrixComponentException {

        CreateUpdateProcedureCommand procCommand = (CreateUpdateProcedureCommand) command;

        //by creating a new group context here it means that variables will resolve with a higher precedence than input/changing
        GroupContext externalGroups = command.getExternalGroupContexts();
        
        List symbols = new LinkedList();
        
        // virtual group elements in HAS and TRANSLATE criteria have to be resolved
        if(procCommand.isUpdateProcedure()){
            resolveVirtualGroupElements(procCommand, useMetadataCommands, metadata);

            //add the default variables
            String countVar = ProcedureReservedWords.VARIABLES + ElementSymbol.SEPARATOR + ProcedureReservedWords.ROWS_UPDATED;
            ElementSymbol updateCount = new ElementSymbol(countVar);
            updateCount.setType(DataTypeManager.DefaultDataClasses.INTEGER);
            symbols.add(updateCount);
            ProcedureContainerResolver.addScalarGroup(ProcedureReservedWords.VARIABLES, metadata.getMetadataStore(), externalGroups, symbols);         
        }
        
        resolveBlock(procCommand, procCommand.getBlock(), externalGroups, metadata, useMetadataCommands, procCommand.isUpdateProcedure(), analysis);
    }

	private void resolveBlock(CreateUpdateProcedureCommand command, Block block, GroupContext externalGroups, 
                              TempMetadataAdapter metadata, boolean useMetadataCommands, boolean isUpdateProcedure, AnalysisRecord analysis)
        throws QueryResolverException, QueryMetadataException, MetaMatrixComponentException {
        LogManager.logTrace(LogConstants.CTX_QUERY_RESOLVER, new Object[]{"Resolving block", block}); //$NON-NLS-1$
        
        //create a new variable and metadata context for this block so that discovered metadata is not visible else where
        TempMetadataStore store = new TempMetadataStore(new HashMap(metadata.getMetadataStore().getData()));
        metadata = new TempMetadataAdapter(metadata.getMetadata(), store);
        externalGroups = new GroupContext(externalGroups, null);
        
        //create a new variables group for this block
        GroupSymbol variables = ProcedureContainerResolver.addScalarGroup(ProcedureReservedWords.VARIABLES, store, externalGroups, new LinkedList());
        
        Iterator stmtIter = block.getStatements().iterator();
        while(stmtIter.hasNext()) {
            resolveStatement(command, (Statement)stmtIter.next(), externalGroups, variables, metadata, useMetadataCommands, isUpdateProcedure, analysis);
        }
    }

	private void resolveStatement(CreateUpdateProcedureCommand command, Statement statement, GroupContext externalGroups, GroupSymbol variables, TempMetadataAdapter metadata, boolean expandCommand, boolean isUpdateProcedure, AnalysisRecord analysis)
        throws QueryResolverException, QueryMetadataException, MetaMatrixComponentException {
        LogManager.logTrace(LogConstants.CTX_QUERY_RESOLVER, new Object[]{"Resolving statement", statement}); //$NON-NLS-1$

        switch(statement.getType()) {
            case Statement.TYPE_IF:
                IfStatement ifStmt = (IfStatement) statement;
                Criteria ifCrit = ifStmt.getCondition();
                for (SubqueryContainer container : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(ifCrit)) {
                	resolveEmbeddedCommand(metadata, externalGroups, container.getCommand(), expandCommand, analysis);
                }
                ResolverVisitor.resolveLanguageObject(ifCrit, null, externalGroups, metadata);
            	resolveBlock(command, ifStmt.getIfBlock(), externalGroups, metadata, expandCommand, isUpdateProcedure, analysis);
                if(ifStmt.hasElseBlock()) {
                    resolveBlock(command, ifStmt.getElseBlock(), externalGroups, metadata, expandCommand, isUpdateProcedure, analysis);
                }
                break;
            case Statement.TYPE_COMMAND:
                CommandStatement cmdStmt = (CommandStatement) statement;
                Command subCommand = cmdStmt.getCommand();
                
                TempMetadataStore discoveredMetadata = resolveEmbeddedCommand(metadata, externalGroups, subCommand, expandCommand, analysis);
                
                if (discoveredMetadata != null) {
                    metadata.getMetadataStore().getData().putAll(discoveredMetadata.getData());
                }
                
                //dynamic commands need to be updated as to their implicitly expected projected symbols 
                if (subCommand instanceof DynamicCommand) {
                    DynamicCommand dynCommand = (DynamicCommand)subCommand;
                    
                    if(dynCommand.getIntoGroup() == null && !command.isUpdateProcedure() && !dynCommand.isAsClauseSet()) {
                        if (command.getProjectedSymbols().size() > 0) {
                            dynCommand.setAsColumns(command.getProjectedSymbols());
                        } else if (command.getParentProjectSymbols() != null) {
                            dynCommand.setAsColumns(command.getParentProjectSymbols());
                        }
                    }
                }
                
                if(!command.isUpdateProcedure()){
                    //don't bother using the metadata when it doesn't matter
                    if (command.getResultsCommand() != null && command.getResultsCommand().getType() == Command.TYPE_DYNAMIC) {
                        DynamicCommand dynamicCommand = (DynamicCommand)command.getResultsCommand();
                        if (!dynamicCommand.isAsClauseSet()) {
                            dynamicCommand.setAsColumns(Collections.EMPTY_LIST);
                        }
                    }
                    //this could be the last select statement, set the projected symbol
                    //on the virtual procedure command
                    command.setResultsCommand(subCommand);
                }

                break;
            case Statement.TYPE_ERROR:
            case Statement.TYPE_ASSIGNMENT:
            case Statement.TYPE_DECLARE:
				AssignmentStatement assStmt = (AssignmentStatement) statement;
                //first resolve the value.  this ensures the value cannot use the variable being defined
            	if (assStmt.getValue() != null) {
					if (assStmt.hasCommand()) {
						Command cmd = assStmt.getCommand();
						resolveEmbeddedCommand(metadata, externalGroups, cmd, expandCommand, analysis);
					} else if (assStmt.hasExpression()) {
                        Expression expr = assStmt.getExpression();
                        for (SubqueryContainer container : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(expr)) {
                        	resolveEmbeddedCommand(metadata, externalGroups, container.getCommand(), expandCommand, analysis);
                        }
                        ResolverVisitor.resolveLanguageObject(expr, null, externalGroups, metadata);
                    }
            	}
                
                //second resolve the variable
                if(statement.getType() == Statement.TYPE_DECLARE) {
                    collectDeclareVariable((DeclareStatement)statement, variables, metadata, externalGroups);
                } else {
                    ResolverVisitor.resolveLanguageObject(assStmt.getVariable(), null, externalGroups, metadata);
                    if (statement.getType() == Statement.TYPE_ASSIGNMENT && !assStmt.getVariable().getGroupSymbol().getCanonicalName().equals(ProcedureReservedWords.VARIABLES)) {
                        throw new QueryResolverException(QueryPlugin.Util.getString("UpdateProcedureResolver.only_variables", assStmt.getVariable())); //$NON-NLS-1$
                    }
                    //don't allow variable assignments to be external
                    assStmt.getVariable().setIsExternalReference(false);
                }
                
                //third ensure the type matches
                if (assStmt.hasExpression()) {
                    Expression expr = assStmt.getExpression();
                    Class varType = assStmt.getVariable().getType();
                    Class exprType = expr.getType();
                    
                    if (exprType == null) {
                        throw new QueryResolverException(QueryPlugin.Util.getString("ResolveVariablesVisitor.datatype_for_the_expression_not_resolvable")); //$NON-NLS-1$
                    }
                    String varTypeName = DataTypeManager.getDataTypeName(varType);
                    assStmt.setExpression(ResolverUtil.convertExpression(expr, varTypeName));                    
                }
                
                break;
            case Statement.TYPE_WHILE:
                WhileStatement whileStmt = (WhileStatement) statement;
                Criteria whileCrit = whileStmt.getCondition();
                for (SubqueryContainer container : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(whileCrit)) {
                	resolveEmbeddedCommand(metadata, externalGroups, container.getCommand(), expandCommand, analysis);
                }
                ResolverVisitor.resolveLanguageObject(whileCrit, null, externalGroups, metadata);
                resolveBlock(command, whileStmt.getBlock(), externalGroups, metadata, expandCommand, isUpdateProcedure, analysis);
                break;
            case Statement.TYPE_LOOP:
                LoopStatement loopStmt = (LoopStatement) statement;
                String groupName = loopStmt.getCursorName();

                if (metadata.getMetadataStore().getTempGroupID(groupName) != null) {
                    throw new QueryResolverException(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0065));
                }
                
	        	//check - cursor name should not start with #
	        	if(GroupSymbol.isTempGroupName(loopStmt.getCursorName())){
	        		String errorMsg = QueryPlugin.Util.getString("ResolveVariablesVisitor.reserved_word_for_temporary_used", loopStmt.getCursorName()); //$NON-NLS-1$
	        		throw new QueryResolverException(errorMsg);
	        	}
                Command cmd = loopStmt.getCommand();
                resolveEmbeddedCommand(metadata, externalGroups, cmd, expandCommand, analysis);
                List symbols = cmd.getProjectedSymbols();
                
                //add the loop cursor group into its own context
                TempMetadataStore store = new TempMetadataStore(new HashMap(metadata.getMetadataStore().getData()));
                metadata = new TempMetadataAdapter(metadata.getMetadata(), store);
                externalGroups = new GroupContext(externalGroups, null);
                
                ProcedureContainerResolver.addScalarGroup(groupName, store, externalGroups, symbols);
                
                resolveBlock(command, loopStmt.getBlock(), externalGroups, metadata, expandCommand, isUpdateProcedure, analysis);
                break;
            case Statement.TYPE_BREAK:
            case Statement.TYPE_CONTINUE:
                break;
            default:
                throw new QueryResolverException(ErrorMessageKeys.RESOLVER_0015, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0015, statement.getType()));
        }
    }

    private TempMetadataStore resolveEmbeddedCommand(TempMetadataAdapter metadata, GroupContext groupContext,
                                Command cmd, boolean expandCommand, AnalysisRecord analysis) throws MetaMatrixComponentException,
                                            QueryResolverException {
        QueryResolver.setChildMetadata(cmd, metadata.getMetadataStore().getData(), groupContext);
        
        return QueryResolver.resolveCommand(cmd, Collections.EMPTY_MAP, expandCommand, metadata.getMetadata(), analysis);
    }
        
    private void collectDeclareVariable(DeclareStatement obj, GroupSymbol variables, TempMetadataAdapter metadata, GroupContext externalGroups) throws QueryResolverException, MetaMatrixComponentException {
        ElementSymbol variable = obj.getVariable();
        String typeName = obj.getVariableType();
        String varName = variable.getName();
        int sepIndex = varName.indexOf(ElementSymbol.SEPARATOR);
        if(sepIndex < 0) {
            String outputName = varName;
            varName = ProcedureReservedWords.VARIABLES + ElementSymbol.SEPARATOR+ varName;
            variable.setName(varName);
            variable.setOutputName(outputName);
        } else {
            sepIndex = varName.lastIndexOf(ElementSymbol.SEPARATOR);
            String groupName = varName.substring(0, sepIndex);
            if(!groupName.equals(ProcedureReservedWords.VARIABLES)) {
                handleUnresolvableDeclaration(variable, QueryPlugin.Util.getString(ErrorMessageKeys.SQL_0031, new Object[]{ProcedureReservedWords.VARIABLES, variable}));
            }
        }
        boolean exists = false;
        try {
        	ResolverVisitor.resolveLanguageObject(variable, null, externalGroups, metadata);
        	exists = true;
        } catch (QueryResolverException e) {
        	//ignore, not already defined
        }
        if (exists) {
        	handleUnresolvableDeclaration(variable, QueryPlugin.Util.getString(ErrorMessageKeys.SQL_0032, variable.getOutputName()));
        }
        variable.setType(DataTypeManager.getDataTypeClass(typeName));
        variable.setGroupSymbol(variables);
        variable.setMetadataID(new TempMetadataID(variable.getName(), variable.getType()));
        //TODO: this will cause the variables group to loose it's cache of resolved symbols
        metadata.getMetadataStore().addElementToTempGroup(ProcedureReservedWords.VARIABLES, (ElementSymbol)variable.clone());
    }

    private void handleUnresolvableDeclaration(ElementSymbol variable, String description) throws QueryResolverException {
        UnresolvedSymbolDescription symbol = new UnresolvedSymbolDescription(variable.toString(), description);
        QueryResolverException e = new QueryResolverException(symbol.getDescription());
        e.setUnresolvedSymbols(Arrays.asList(new Object[] {symbol}));
        throw e;
    }

}
