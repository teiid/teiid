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

package org.teiid.query.rewriter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.query.QueryPlugin;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.function.FunctionMethods;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.relational.rules.NewCalculateCostUtil;
import org.teiid.query.optimizer.relational.rules.RuleMergeCriteria;
import org.teiid.query.optimizer.relational.rules.RulePlaceAccess;
import org.teiid.query.optimizer.relational.rules.RuleMergeCriteria.PlannedResult;
import org.teiid.query.processor.relational.RelationalNodeUtil;
import org.teiid.query.resolver.ProcedureContainerResolver;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.ProcedureReservedWords;
import org.teiid.query.sql.LanguageObject.Util;
import org.teiid.query.sql.lang.AbstractSetCriteria;
import org.teiid.query.sql.lang.ArrayTable;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.BetweenCriteria;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.ExpressionCriteria;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.GroupBy;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Into;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.JoinPredicate;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.MatchCriteria;
import org.teiid.query.sql.lang.NotCriteria;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SetClause;
import org.teiid.query.sql.lang.SetClauseList;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.lang.SubqueryFromClause;
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.lang.TextTable;
import org.teiid.query.sql.lang.TranslatableProcedureContainer;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.lang.WithQueryCommand;
import org.teiid.query.sql.lang.XMLTable;
import org.teiid.query.sql.lang.PredicateCriteria.Negatable;
import org.teiid.query.sql.navigator.DeepPostOrderNavigator;
import org.teiid.query.sql.navigator.PostOrderNavigator;
import org.teiid.query.sql.proc.AssignmentStatement;
import org.teiid.query.sql.proc.Block;
import org.teiid.query.sql.proc.CommandStatement;
import org.teiid.query.sql.proc.CreateUpdateProcedureCommand;
import org.teiid.query.sql.proc.CriteriaSelector;
import org.teiid.query.sql.proc.ExpressionStatement;
import org.teiid.query.sql.proc.HasCriteria;
import org.teiid.query.sql.proc.IfStatement;
import org.teiid.query.sql.proc.LoopStatement;
import org.teiid.query.sql.proc.Statement;
import org.teiid.query.sql.proc.TranslateCriteria;
import org.teiid.query.sql.proc.TriggerAction;
import org.teiid.query.sql.proc.WhileStatement;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.CaseExpression;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.symbol.SearchedCaseExpression;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.symbol.AggregateSymbol.Type;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.AggregateSymbolCollectorVisitor;
import org.teiid.query.sql.visitor.CorrelatedReferenceCollectorVisitor;
import org.teiid.query.sql.visitor.CriteriaTranslatorVisitor;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.sql.visitor.FunctionCollectorVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.sql.visitor.EvaluatableVisitor.EvaluationLevel;
import org.teiid.query.util.CommandContext;
import org.teiid.query.validator.UpdateValidator.UpdateInfo;
import org.teiid.query.validator.UpdateValidator.UpdateMapping;
import org.teiid.translator.SourceSystemFunctions;


/**
 * Rewrites commands and command fragments to a form that is better for planning and execution.  There is a current limitation that
 * command objects themselves cannot change type, since the same object is always used. 
 */
public class QueryRewriter {

    public static final CompareCriteria TRUE_CRITERIA = new CompareCriteria(new Constant(1, DataTypeManager.DefaultDataClasses.INTEGER), CompareCriteria.EQ, new Constant(1, DataTypeManager.DefaultDataClasses.INTEGER));
    public static final CompareCriteria FALSE_CRITERIA = new CompareCriteria(new Constant(1, DataTypeManager.DefaultDataClasses.INTEGER), CompareCriteria.EQ, new Constant(0, DataTypeManager.DefaultDataClasses.INTEGER));
    public static final CompareCriteria UNKNOWN_CRITERIA = new CompareCriteria(new Constant(null, DataTypeManager.DefaultDataClasses.STRING), CompareCriteria.NE, new Constant(null, DataTypeManager.DefaultDataClasses.STRING));
    
    private static final Map<String, String> ALIASED_FUNCTIONS = new HashMap<String, String>();
    
    static {
    	ALIASED_FUNCTIONS.put("lower", SourceSystemFunctions.LCASE); //$NON-NLS-1$
    	ALIASED_FUNCTIONS.put("upper", SourceSystemFunctions.UCASE); //$NON-NLS-1$
    	ALIASED_FUNCTIONS.put("cast", SourceSystemFunctions.CONVERT); //$NON-NLS-1$
    	ALIASED_FUNCTIONS.put("nvl", SourceSystemFunctions.IFNULL); //$NON-NLS-1$
    	ALIASED_FUNCTIONS.put("||", SourceSystemFunctions.CONCAT); //$NON-NLS-1$
    	ALIASED_FUNCTIONS.put("chr", SourceSystemFunctions.CHAR); //$NON-NLS-1$
    }
    
    private static final Set<String> PARSE_FORMAT_TYPES = new HashSet<String>(Arrays.asList(DataTypeManager.DefaultDataTypes.TIME, 
    		DataTypeManager.DefaultDataTypes.DATE, DataTypeManager.DefaultDataTypes.TIMESTAMP, DataTypeManager.DefaultDataTypes.BIG_DECIMAL, 
    		DataTypeManager.DefaultDataTypes.BIG_INTEGER, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.LONG, 
    		DataTypeManager.DefaultDataTypes.FLOAT, DataTypeManager.DefaultDataTypes.DOUBLE));
    
    private QueryMetadataInterface metadata;
    private CommandContext context;
    private CreateUpdateProcedureCommand procCommand;
    
    private boolean rewriteSubcommands;
    private boolean processing;
    private Evaluator evaluator;
    private Map<ElementSymbol, Expression> variables; //constant propagation
    private int commandType;
    
    private QueryRewriter(QueryMetadataInterface metadata,
			CommandContext context, CreateUpdateProcedureCommand procCommand) {
		this.metadata = metadata;
		this.context = context;
		this.procCommand = procCommand;
		this.evaluator = new Evaluator(Collections.emptyMap(), null, context);
	}
    
    public static Command evaluateAndRewrite(Command command, Evaluator eval, CommandContext context, QueryMetadataInterface metadata) throws TeiidProcessingException, TeiidComponentException {
    	QueryRewriter queryRewriter = new QueryRewriter(metadata, context, null);
    	queryRewriter.evaluator = eval;
    	queryRewriter.rewriteSubcommands = true;
    	queryRewriter.processing = true;
		return queryRewriter.rewriteCommand(command, false);
    }

	public static Command rewrite(Command command, CreateUpdateProcedureCommand procCommand, QueryMetadataInterface metadata, CommandContext context, Map<ElementSymbol, Expression> variableValues, int commandType) throws TeiidComponentException, TeiidProcessingException{
		QueryRewriter rewriter = new QueryRewriter(metadata, context, procCommand);
		rewriter.rewriteSubcommands = true;
		rewriter.variables = variableValues;
		rewriter.commandType = commandType;
		return rewriter.rewriteCommand(command, false);
	}
    
	public static Command rewrite(Command command, QueryMetadataInterface metadata, CommandContext context) throws TeiidComponentException, TeiidProcessingException{
		return rewrite(command, null, metadata, context, null, Command.TYPE_UNKNOWN);
    }

    /**
     * Rewrites the command and all of its subcommands (both embedded and non-embedded)
     *  
     * @param command
     * @param removeOrderBy
     * @return
     * @throws QueryValidatorException
     */
	private Command rewriteCommand(Command command, boolean removeOrderBy) throws TeiidComponentException, TeiidProcessingException{
		QueryMetadataInterface oldMetadata = metadata;
		CreateUpdateProcedureCommand oldProcCommand = procCommand;
        
		Map tempMetadata = command.getTemporaryMetadata();
        if(tempMetadata != null) {
        	metadata = new TempMetadataAdapter(metadata, new TempMetadataStore(tempMetadata));
        }
        
        switch(command.getType()) {
			case Command.TYPE_QUERY:
                QueryCommand queryCommand = (QueryCommand)command;
            	if (removeOrderBy && queryCommand.getLimit() == null) {
            		queryCommand.setOrderBy(null);
                }
            	if (queryCommand.getWith() != null) {
            		for (WithQueryCommand withQueryCommand : queryCommand.getWith()) {
            			if (withQueryCommand.getColumns() == null) {
            				List<ElementSymbol> columns = ResolverUtil.resolveElementsInGroup(withQueryCommand.getGroupSymbol(), metadata);
            				withQueryCommand.setColumns(LanguageObject.Util.deepClone(columns, ElementSymbol.class));
            			}
						rewriteSubqueryContainer(withQueryCommand, true);
					}
            	}
                if(command instanceof Query) {
                    command = rewriteQuery((Query) command);
                }else {
                    command = rewriteSetQuery((SetQuery) command);
                }
                break;
            case Command.TYPE_STORED_PROCEDURE:
                command = rewriteExec((StoredProcedure) command);
                break;
    		case Command.TYPE_INSERT:
                command = rewriteInsert((Insert) command);
                break;
			case Command.TYPE_UPDATE:
                command = rewriteUpdate((Update) command);
                break;
			case Command.TYPE_DELETE:
                command = rewriteDelete((Delete) command);
                break;
            case Command.TYPE_UPDATE_PROCEDURE:
                procCommand = (CreateUpdateProcedureCommand) command;
                command = rewriteUpdateProcedure((CreateUpdateProcedureCommand) command);
                break;
            case Command.TYPE_BATCHED_UPDATE:
            	List subCommands = ((BatchedUpdateCommand)command).getUpdateCommands();
                for (int i = 0; i < subCommands.size(); i++) {
                    Command subCommand = (Command)subCommands.get(i);
                    subCommand = rewriteCommand(subCommand, false);
                    subCommands.set(i, subCommand);
                }
                break;
            case Command.TYPE_TRIGGER_ACTION:
            	TriggerAction ta = (TriggerAction)command;
            	ta.setBlock(rewriteBlock(ta.getBlock()));
            	break;
		}
        
        this.metadata = oldMetadata;
        this.procCommand = oldProcCommand;
        return command;
	}
    
	private Command rewriteUpdateProcedure(CreateUpdateProcedureCommand command)
								 throws TeiidComponentException, TeiidProcessingException{
        Map<ElementSymbol, Expression> oldVariables = variables;
    	if (command.getUserCommand() != null) {
            variables = QueryResolver.getVariableValues(command.getUserCommand(), false, metadata);                        
            commandType = command.getUserCommand().getType();
    	}

		Block block = rewriteBlock(command.getBlock());
        command.setBlock(block);

        variables = oldVariables;
        
        return command;
	}

	private Block rewriteBlock(Block block)
								 throws TeiidComponentException, TeiidProcessingException{
		List statements = block.getStatements();
        Iterator stmtIter = statements.iterator();

		List newStmts = new ArrayList(statements.size());
		// plan each statement in the block
        while(stmtIter.hasNext()) {
			Statement stmnt = (Statement) stmtIter.next();
			Object newStmt = rewriteStatement(stmnt);
			if(newStmt instanceof Statement) {
				newStmts.add(newStmt);
			} else if (newStmt instanceof List) {
			    newStmts.addAll((List)newStmt);
            }
        }

        block.setStatements(newStmts);

        return block;
	 }

	private Object rewriteStatement(Statement statement)
								 throws TeiidComponentException, TeiidProcessingException{

        // evaluate the HAS Criteria on the procedure and rewrite
		int stmtType = statement.getType();
		switch(stmtType) {
			case Statement.TYPE_IF:
				IfStatement ifStmt = (IfStatement) statement;
				Criteria ifCrit = ifStmt.getCondition();
				Criteria evalCrit = rewriteCriteria(ifCrit);
                
				ifStmt.setCondition(evalCrit);
				if(evalCrit.equals(TRUE_CRITERIA)) {
					Block ifblock = rewriteBlock(ifStmt.getIfBlock());
					return ifblock.getStatements();
				} else if(evalCrit.equals(FALSE_CRITERIA) || evalCrit.equals(UNKNOWN_CRITERIA)) {
					if(ifStmt.hasElseBlock()) {
						Block elseBlock = rewriteBlock(ifStmt.getElseBlock());
						return elseBlock.getStatements();
					} 
                    return null;
				} else {
					Block ifblock = rewriteBlock(ifStmt.getIfBlock());
					ifStmt.setIfBlock(ifblock);
					if(ifStmt.hasElseBlock()) {
						Block elseBlock = rewriteBlock(ifStmt.getElseBlock());
						ifStmt.setElseBlock(elseBlock);
					}
				}
				return ifStmt;
            case Statement.TYPE_ERROR: 
            case Statement.TYPE_DECLARE:
            case Statement.TYPE_ASSIGNMENT:
				ExpressionStatement exprStmt = (ExpressionStatement) statement;
				// replace variables to references, these references are later
				// replaced in the processor with variable values
				Expression expr = exprStmt.getExpression();
				if (expr != null) {
					expr = rewriteExpressionDirect(expr);
	                exprStmt.setExpression(expr);
				}
				return exprStmt;
            case Statement.TYPE_COMMAND:
				CommandStatement cmdStmt = (CommandStatement) statement;
                rewriteSubqueryContainer(cmdStmt, false);
                
				if(cmdStmt.getCommand().getType() == Command.TYPE_UPDATE) {
                    Update update = (Update)cmdStmt.getCommand();
                    if (update.getChangeList().isEmpty()) {
                        return null;
                    }
				}
                return statement;
            case Statement.TYPE_LOOP: 
                LoopStatement loop = (LoopStatement)statement; 
                
                rewriteSubqueryContainer(loop, false);
                
                rewriteBlock(loop.getBlock());
                
                if (loop.getBlock().getStatements().isEmpty()) {
                    return null;
                }
                
                return loop;
            case Statement.TYPE_WHILE:
                WhileStatement whileStatement = (WhileStatement) statement;
                Criteria crit = whileStatement.getCondition();
                crit = rewriteCriteria(crit);
                
                whileStatement.setCondition(crit);
                if(crit.equals(TRUE_CRITERIA)) {
                    throw new QueryValidatorException(QueryPlugin.Util.getString("QueryRewriter.infinite_while")); //$NON-NLS-1$
                } else if(crit.equals(FALSE_CRITERIA) || crit.equals(UNKNOWN_CRITERIA)) {
                    return null;
                } 
                whileStatement.setBlock(rewriteBlock(whileStatement.getBlock()));
                
                if (whileStatement.getBlock().getStatements().isEmpty()) {
                    return null;
                }
                
                return whileStatement;
			default:
				return statement;
		}
	}
    
    /** 
     * @param removeOrderBy
     * @param assStmt
     * @throws QueryValidatorException
     */
    private void rewriteSubqueryContainer(SubqueryContainer container, boolean removeOrderBy) throws TeiidComponentException, TeiidProcessingException{
        if (rewriteSubcommands && container.getCommand() != null && (container.getCommand().getProcessorPlan() == null || processing)) {
        	container.setCommand(rewriteCommand(container.getCommand(), removeOrderBy));
        }
    }
    
	/**
	 * <p>The HasCriteria evaluates to a TRUE_CRITERIA or a FALSE_CRITERIA, it checks to see
	 * if type of criteria on the elements specified by the CriteriaSelector is specified on
	 * the user's command.</p>
	 */
	private Criteria rewriteCriteria(HasCriteria hasCrit) {
		Criteria userCrit = null;
		Command userCommand = procCommand.getUserCommand();
		int cmdType = userCommand.getType();
		switch(cmdType) {
			case Command.TYPE_DELETE:
				userCrit = ((Delete)userCommand).getCriteria();
				break;
			case Command.TYPE_UPDATE:
				userCrit = ((Update)userCommand).getCriteria();
				break;
			default:
				return FALSE_CRITERIA;
		}

		if(userCrit == null) {
			return FALSE_CRITERIA;
		}

		// get the CriteriaSelector, elements on the selector and the selector type
		CriteriaSelector selector = hasCrit.getSelector();

		Collection hasCritElmts = null;
		if(selector.hasElements()) {
			hasCritElmts = selector.getElements();
			// collect elements present on the user's criteria and check if
			// all of the hasCriteria elements are among them
			Collection<ElementSymbol> userElmnts = ElementCollectorVisitor.getElements(userCrit, true);
			if(!userElmnts.containsAll(hasCritElmts)) {
				return FALSE_CRITERIA;
			}
		}

		int selectorType = selector.getSelectorType();
		// if no selector type specified return true
		// already checked all HAS elements present on user criteria
		if(selectorType == CriteriaSelector.NO_TYPE) {
			return TRUE_CRITERIA;
		}

		// collect all predicate criteria present on the user's criteria
		for (Criteria predicateCriteria : Criteria.separateCriteriaByAnd(userCrit)) {
    		// atleast one of the hasElemnets should be on this predicate else
    		// proceed to the next predicate
			Collection<ElementSymbol> predElmnts = ElementCollectorVisitor.getElements(predicateCriteria, true);
			if(selector.hasElements()) {
				Iterator hasIter = hasCritElmts.iterator();
				boolean containsElmnt = false;
				while(hasIter.hasNext()) {
					ElementSymbol hasElmnt = (ElementSymbol) hasIter.next();
					if(predElmnts.contains(hasElmnt)) {
						containsElmnt = true;
					}
				}

				if(!containsElmnt) {
					continue;
				}
			}

			// check if the predicate criteria type maches the type specified
			// by the criteria selector
    		switch(selectorType) {
	    		case CriteriaSelector.IN:
		    		if(predicateCriteria instanceof SetCriteria) {
	    				return TRUE_CRITERIA;
		    		}
                    break;
	    		case CriteriaSelector.LIKE:
		    		if(predicateCriteria instanceof MatchCriteria) {
	    				return TRUE_CRITERIA;
		    		}
                    break;
                case CriteriaSelector.IS_NULL:
                    if(predicateCriteria instanceof IsNullCriteria) {
                        return TRUE_CRITERIA;
                    }
                    break;
                case CriteriaSelector.BETWEEN:
                    if(predicateCriteria instanceof BetweenCriteria) {
                        return TRUE_CRITERIA;
                    }
                    break;
	    		default: // EQ, GT, LT, GE, LE criteria
		    		if(predicateCriteria instanceof CompareCriteria) {
		    			CompareCriteria compCrit = (CompareCriteria) predicateCriteria;
		    			if(compCrit.getOperator() == selectorType) {
		    				return TRUE_CRITERIA;
		    			}
		    		}
                    break;
			}
		}

		return FALSE_CRITERIA;
	}

	/**
	 * <p>TranslateCriteria is evaluated by translating elements on parts(restricted by the type
	 * of criteria and elements specified on the CriteriaSelector) the user's criteria
	 * using the translations provided on the TranslateCriteria and symbol mapping between
	 * virtual group elements and the expressions on the query transformation defining the
	 * virtual group.</p>
	 */
	private Criteria rewriteCriteria(TranslateCriteria transCrit)
			 throws TeiidComponentException, TeiidProcessingException{

		// criteria translated
		Criteria translatedCriteria = null;

		// get the user's command from the procedure
		Command userCmd = procCommand.getUserCommand();

		if (!(userCmd instanceof TranslatableProcedureContainer)) {
			return FALSE_CRITERIA;
		}
		
		Criteria userCriteria = ((TranslatableProcedureContainer)userCmd).getCriteria();
		
		if (userCriteria == null) {
			return TRUE_CRITERIA;
		}

		// get the symbolmap between virtual elements and theie counterpart expressions
		// from the virtual group's query transform
		CriteriaTranslatorVisitor translateVisitor = new CriteriaTranslatorVisitor(procCommand.getSymbolMap());

		translateVisitor.setCriteriaSelector(transCrit.getSelector());
		if(transCrit.hasTranslations()) {
			translateVisitor.setTranslations(transCrit.getTranslations());
		}

		// create a clone of user's criteria that is then translated
		Criteria userClone = (Criteria) userCriteria.clone();

		userClone = translateVisitor.translate(userClone);

		// translated criteria
		((TranslatableProcedureContainer)userCmd).addImplicitParameters(translateVisitor.getImplicitParams());
		
		translatedCriteria = rewriteCriteria(userClone);

		// apply any implicit conversions
		try {
            ResolverVisitor.resolveLanguageObject(translatedCriteria, metadata);
		} catch(TeiidException ex) {
            throw new QueryValidatorException(ex, "ERR.015.009.0002", QueryPlugin.Util.getString("ERR.015.009.0002", translatedCriteria)); //$NON-NLS-1$ //$NON-NLS-2$
		}

		return translatedCriteria;
	}

	private Command rewriteQuery(Query query)
             throws TeiidComponentException, TeiidProcessingException{
        
        // Rewrite from clause
        From from = query.getFrom();
        if(from != null){
            List<FromClause> clauses = new ArrayList<FromClause>(from.getClauses().size());
            Iterator clauseIter = from.getClauses().iterator();
            while(clauseIter.hasNext()) {
                clauses.add( rewriteFromClause(query, (FromClause) clauseIter.next()) );
            }
            from.setClauses(clauses);
        } else {
            query.setOrderBy(null);
        }

        // Rewrite criteria
        Criteria crit = query.getCriteria();
        if(crit != null) {
        	if (query.getIsXML()) {
        		//only rewrite expressions, removing whole predicates could change the nature of the query, 
        		//because we aren't considering the xml pseudo-functions context, row limit, etc. 
        		rewriteExpressions(crit);
        	} else {
        		crit = rewriteCriteria(crit);
        	}
            if(crit == TRUE_CRITERIA) {
                query.setCriteria(null);
            } else if (crit == UNKNOWN_CRITERIA) {
            	query.setCriteria(FALSE_CRITERIA);
            } else {
                query.setCriteria(crit);
            } 
        }
        
        if (from != null && !query.getIsXML()) {
        	writeSubqueriesAsJoins(query);
        }

        query = rewriteGroupBy(query);

        // Rewrite having
        Criteria having = query.getHaving();
        if(having != null) {
            crit = rewriteCriteria(having);
            if(crit == TRUE_CRITERIA) {
                query.setHaving(null);
            } else {
                query.setHaving(crit);
            } 
        }
        
        rewriteExpressions(query.getSelect());

        if (!query.getIsXML()) {
            query = (Query)rewriteOrderBy(query);
        }
        
        if (query.getLimit() != null) {
            query.setLimit(rewriteLimitClause(query.getLimit()));
        }
        
        if (query.getInto() != null) {
            return rewriteSelectInto(query);
        }
        
        return query;
    }

	private void writeSubqueriesAsJoins(Query query)
			throws TeiidComponentException, QueryMetadataException,
			QueryResolverException {
		if (query.getCriteria() == null) {
			return;
		}
		RuleMergeCriteria rmc = new RuleMergeCriteria(null, null, null, this.context, this.metadata);
		List<Criteria> current = Criteria.separateCriteriaByAnd(query.getCriteria());
		query.setCriteria(null);
		List<GroupSymbol> groups = query.getFrom().getGroups();
		HashSet<String> names = new HashSet<String>();
		for (GroupSymbol gs : groups) {
			names.add(gs.getCanonicalName());
		}
		for (Iterator<Criteria> crits = current.iterator(); crits.hasNext();) {
			PlannedResult plannedResult = rmc.findSubquery(crits.next());
			if (plannedResult.not || plannedResult.query == null || plannedResult.query.getProcessorPlan() != null 
					|| plannedResult.query.getWith() != null) {
				continue;
			}
			if (plannedResult.query.getCorrelatedReferences() == null) {
				//create the correlated refs if they exist
				//there is a little bit of a design problem here that null usually means no refs.
				ArrayList<Reference> correlatedReferences = new ArrayList<Reference>();
				CorrelatedReferenceCollectorVisitor.collectReferences(plannedResult.query, groups, correlatedReferences);
				if (!correlatedReferences.isEmpty()) {
		            SymbolMap map = new SymbolMap();
		            for (Reference reference : correlatedReferences) {
						map.addMapping(reference.getExpression(), reference.getExpression());
					}
		            plannedResult.query.setCorrelatedReferences(map);
		        }	
			}
			boolean requiresDistinct = requiresDistinctRows(query);
			if (!rmc.planQuery(groups, requiresDistinct, plannedResult)) {
				continue;
			}
			if (requiresDistinct) {
				//check for key preservation
				HashSet<GroupSymbol> keyPreservingGroups = new HashSet<GroupSymbol>();
				ResolverUtil.findKeyPreserved(query, keyPreservingGroups, metadata);
				if (NewCalculateCostUtil.usesKey(plannedResult.leftExpressions, keyPreservingGroups, metadata, true) && plannedResult.madeDistinct) {
					//if key perserved then the semi-join will remain in-tact without make the other query distinct
					plannedResult.madeDistinct = false;
					plannedResult.query.getSelect().setDistinct(false);
				}
			}
			crits.remove();
			
			GroupSymbol viewName = RulePlaceAccess.recontextSymbol(new GroupSymbol("X"), names); //$NON-NLS-1$
			viewName.setName(viewName.getCanonicalName());
			viewName.setDefinition(null);
			Query q = createInlineViewQuery(viewName, plannedResult.query, metadata, plannedResult.query.getSelect().getProjectedSymbols());
			
			Iterator<SingleElementSymbol> iter = q.getSelect().getProjectedSymbols().iterator();
		    HashMap<Expression, SingleElementSymbol> expressionMap = new HashMap<Expression, SingleElementSymbol>();
		    for (SingleElementSymbol symbol : plannedResult.query.getSelect().getProjectedSymbols()) {
		        expressionMap.put(SymbolMap.getExpression(symbol), iter.next());
		    }
			for (int i = 0; i < plannedResult.leftExpressions.size(); i++) {
				plannedResult.nonEquiJoinCriteria.add(new CompareCriteria(SymbolMap.getExpression((Expression)plannedResult.leftExpressions.get(i)), CompareCriteria.EQ, (Expression)plannedResult.rightExpressions.get(i)));
			}
			Criteria mappedCriteria = Criteria.combineCriteria(plannedResult.nonEquiJoinCriteria);
			ExpressionMappingVisitor.mapExpressions(mappedCriteria, expressionMap);
			query.setCriteria(Criteria.combineCriteria(query.getCriteria(), mappedCriteria));
		    query.getFrom().addClause((FromClause) q.getFrom().getClauses().get(0));
		    query.getTemporaryMetadata().putAll(q.getTemporaryMetadata());
			//transform the query into an inner join 
		}
		query.setCriteria(Criteria.combineCriteria(query.getCriteria(), Criteria.combineCriteria(current)));
	}

	private boolean requiresDistinctRows(Query query) {
		Set<AggregateSymbol> aggs = new HashSet<AggregateSymbol>();
		aggs.addAll(AggregateSymbolCollectorVisitor.getAggregates(query.getSelect(), false));
		aggs.addAll(AggregateSymbolCollectorVisitor.getAggregates(query.getHaving(), false));
		if (!aggs.isEmpty() || query.getGroupBy() != null) {
			if (!AggregateSymbol.areAggregatesCardinalityDependent(aggs)) {
				return false;
			}
		} else if (query.getSelect().isDistinct()) {
			for (SingleElementSymbol projectSymbol : query.getSelect().getProjectedSymbols()) {
				Expression ex = SymbolMap.getExpression(projectSymbol);
	            if (FunctionCollectorVisitor.isNonDeterministic(ex)) {
	            	return true;
	            }
	           	if (!ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(ex).isEmpty()) {
	           		return true;
	           	}
			}
			return false;
		}
		return true;
	}

	/**
	 * Converts a group by with expressions into a group by with only element symbols and an inline view
	 * @param query
	 * @return
	 * @throws QueryValidatorException
	 */
	private Query rewriteGroupBy(Query query) throws TeiidComponentException, TeiidProcessingException{
		if (query.getGroupBy() == null) {
			return query;
		}
		if (isDistinctWithGroupBy(query)) {
			query.getSelect().setDistinct(false);
		}
        // we check for group by expressions here to create an ANSI SQL plan
        boolean hasExpression = false;
        for (final Iterator iterator = query.getGroupBy().getSymbols().iterator(); !hasExpression && iterator.hasNext();) {
            hasExpression = iterator.next() instanceof ExpressionSymbol;
        } 
        if (!hasExpression) {
        	return query;
        }
        Select select = query.getSelect();
        GroupBy groupBy = query.getGroupBy();
        query.setGroupBy(null);
        Criteria having = query.getHaving();
        query.setHaving(null);
        OrderBy orderBy = query.getOrderBy();
        query.setOrderBy(null);
        Limit limit = query.getLimit();
        query.setLimit(null);
        Into into = query.getInto();
        query.setInto(null);
        Set<Expression> newSelectColumns = new HashSet<Expression>();
        for (final Iterator iterator = groupBy.getSymbols().iterator(); iterator.hasNext();) {
            newSelectColumns.add(SymbolMap.getExpression((SingleElementSymbol)iterator.next()));
        }
        Set<AggregateSymbol> aggs = new HashSet<AggregateSymbol>();
        aggs.addAll(AggregateSymbolCollectorVisitor.getAggregates(select, true));
        if (having != null) {
            aggs.addAll(AggregateSymbolCollectorVisitor.getAggregates(having, true));
        }
        for (AggregateSymbol aggregateSymbol : aggs) {
            if (aggregateSymbol.getExpression() != null) {
                Expression expr = aggregateSymbol.getExpression();
                newSelectColumns.add(SymbolMap.getExpression(expr));
            }
        }
        Select innerSelect = new Select();
        int index = 0;
        for (Expression expr : newSelectColumns) {
            if (expr instanceof SingleElementSymbol) {
                innerSelect.addSymbol((SingleElementSymbol)expr);
            } else {
                innerSelect.addSymbol(new ExpressionSymbol("EXPR" + index++ , expr)); //$NON-NLS-1$
            }
        }
        query.setSelect(innerSelect);
        Query outerQuery = null;
        try {
            outerQuery = QueryRewriter.createInlineViewQuery(new GroupSymbol("X"), query, metadata, query.getSelect().getProjectedSymbols()); //$NON-NLS-1$
        } catch (TeiidException err) {
            throw new TeiidRuntimeException(err);
        }
        Iterator<SingleElementSymbol> iter = outerQuery.getSelect().getProjectedSymbols().iterator();
        HashMap<Expression, SingleElementSymbol> expressionMap = new HashMap<Expression, SingleElementSymbol>();
        for (SingleElementSymbol symbol : query.getSelect().getProjectedSymbols()) {
            expressionMap.put(SymbolMap.getExpression(symbol), iter.next());
        }
        ExpressionMappingVisitor.mapExpressions(groupBy, expressionMap);
        outerQuery.setGroupBy(groupBy);
        ExpressionMappingVisitor.mapExpressions(having, expressionMap);
        outerQuery.setHaving(having);
        ExpressionMappingVisitor.mapExpressions(orderBy, expressionMap);
        outerQuery.setOrderBy(orderBy);
        outerQuery.setLimit(limit);
        ExpressionMappingVisitor.mapExpressions(select, expressionMap);
        outerQuery.setSelect(select);
        outerQuery.setInto(into);
        outerQuery.setOption(query.getOption());
        query = outerQuery;
        rewriteExpressions(innerSelect);
		return query;
	}
	
	public static boolean isDistinctWithGroupBy(Query query) {
		GroupBy groupBy = query.getGroupBy();
		if (groupBy == null) {
			return false;
		}
		HashSet<Expression> selectExpressions = new HashSet<Expression>();
		for (SingleElementSymbol selectExpr : query.getSelect().getProjectedSymbols()) {
			selectExpressions.add(SymbolMap.getExpression(selectExpr));
		}
		for (SingleElementSymbol groupByExpr :  (List<SingleElementSymbol>)groupBy.getSymbols()) {
			if (!selectExpressions.contains(groupByExpr)) {
				return false;
			}
		}
		return true;
	}
    
    private void rewriteExpressions(LanguageObject obj) throws TeiidComponentException, TeiidProcessingException{
        if (obj == null) {
            return;
        }
        ExpressionMappingVisitor visitor = new ExpressionMappingVisitor(null) {
            /** 
             * @see org.teiid.query.sql.visitor.ExpressionMappingVisitor#replaceExpression(org.teiid.query.sql.symbol.Expression)
             */
            @Override
            public Expression replaceExpression(Expression element) {
                try {
                    return rewriteExpressionDirect(element);
                } catch (TeiidException err) {
                    throw new TeiidRuntimeException(err);
                }
            }
        };
        try {
            PostOrderNavigator.doVisit(obj, visitor);
        } catch (TeiidRuntimeException err) {
            if (err.getChild() instanceof TeiidComponentException) {
                throw (TeiidComponentException)err.getChild();
            } 
            if (err.getChild() instanceof TeiidProcessingException) {
                throw (TeiidProcessingException)err.getChild();
            } 
            throw err;
        }
    }
	
    /**
     * Rewrite the order by clause.
     * Unrelated order by expressions will cause the creation of nested inline views.
     *  
     * @param query
     * @throws TeiidComponentException, MetaMatrixProcessingException
     */
    public QueryCommand rewriteOrderBy(QueryCommand queryCommand) throws TeiidComponentException {
    	final OrderBy orderBy = queryCommand.getOrderBy();
        if (orderBy == null) {
            return queryCommand;
        }
        Select select = queryCommand.getProjectedQuery().getSelect();
        final List<SingleElementSymbol> projectedSymbols = select.getProjectedSymbols();
        
        LinkedList<OrderByItem> unrelatedItems = new LinkedList<OrderByItem>();
        
        boolean hasUnrelatedExpression = rewriteOrderBy(queryCommand, orderBy,
				projectedSymbols, unrelatedItems);
        
        if (orderBy.getVariableCount() == 0 || !hasUnrelatedExpression) {
        	return queryCommand;
        } 
        
        int originalSymbolCount = select.getProjectedSymbols().size();

        //add unrelated to select
        for (OrderByItem orderByItem : unrelatedItems) {
            select.addSymbol(orderByItem.getSymbol());				
		}
        makeSelectUnique(select, false);
        
        Query query = queryCommand.getProjectedQuery();
        
        Into into = query.getInto();
        query.setInto(null);
        Limit limit = query.getLimit();
        query.setLimit(null);
        query.setOrderBy(null);
        
        Query top = null;
        
        try {
        	top = createInlineViewQuery(new GroupSymbol("X"), query, metadata, select.getProjectedSymbols()); //$NON-NLS-1$
			Iterator<SingleElementSymbol> iter = top.getSelect().getProjectedSymbols().iterator();
		    HashMap<Expression, SingleElementSymbol> expressionMap = new HashMap<Expression, SingleElementSymbol>();
		    for (SingleElementSymbol symbol : select.getProjectedSymbols()) {
		    	SingleElementSymbol ses = iter.next();
		        expressionMap.put(SymbolMap.getExpression(symbol), ses);
		        expressionMap.put(new ElementSymbol(symbol.getName()), ses);
		    }
		    ExpressionMappingVisitor.mapExpressions(orderBy, expressionMap);
		    //now the order by should only contain element symbols
		} catch (TeiidException err) {
            throw new TeiidRuntimeException(err);
        }
		List symbols = top.getSelect().getSymbols();
		top.getSelect().setSymbols(symbols.subList(0, originalSymbolCount));
		top.setInto(into);
		top.setLimit(limit);
		top.setOrderBy(orderBy);
		return top;
    }

	public static boolean rewriteOrderBy(QueryCommand queryCommand,
			final OrderBy orderBy, final List projectedSymbols,
			LinkedList<OrderByItem> unrelatedItems) {
		boolean hasUnrelatedExpression = false;
		HashSet<Expression> previousExpressions = new HashSet<Expression>();
        for (int i = 0; i < orderBy.getVariableCount(); i++) {
        	SingleElementSymbol querySymbol = orderBy.getVariable(i);
        	int index = orderBy.getExpressionPosition(i);
    		boolean isUnrelated = false;
        	if (index == -1) {
    			unrelatedItems.add(orderBy.getOrderByItems().get(i));
    			isUnrelated = (querySymbol instanceof ExpressionSymbol);
        	} else {
        		querySymbol = (SingleElementSymbol)projectedSymbols.get(index);
        	}
        	Expression expr = SymbolMap.getExpression(querySymbol);
        	if (!previousExpressions.add(expr) || (queryCommand instanceof Query && EvaluatableVisitor.willBecomeConstant(expr))) {
                orderBy.removeOrderByItem(i--);
        	} else if (!isUnrelated) {
        		orderBy.getOrderByItems().get(i).setSymbol((SingleElementSymbol)querySymbol.clone());
        	} else {
        		hasUnrelatedExpression = true;
        	}
        }
        if (orderBy.getVariableCount() == 0) {
        	queryCommand.setOrderBy(null);
        } 
		return hasUnrelatedExpression;
	}
    
    /**
     * This method will alias each of the select into elements to the corresponding column name in the 
     * target table.  This ensures that they will all be uniquely named.
     *  
     * @param query
     * @throws QueryValidatorException
     */
    private Insert rewriteSelectInto(Query query) throws TeiidProcessingException{
        Into into = query.getInto();
        try {
            List<ElementSymbol> allIntoElements = Util.deepClone(ResolverUtil.resolveElementsInGroup(into.getGroup(), metadata), ElementSymbol.class);
            Insert insert = new Insert(into.getGroup(), allIntoElements, Collections.emptyList());
            query.setInto(null);
            insert.setQueryExpression(query);
            return rewriteInsert(correctDatatypes(insert));
        } catch (QueryMetadataException err) {
            throw new QueryValidatorException(err, err.getMessage());
        } catch (TeiidComponentException err) {
            throw new QueryValidatorException(err, err.getMessage());
		}
    }

	private Insert correctDatatypes(Insert insert) {
		boolean needsView = false;
		for (int i = 0; !needsView && i < insert.getVariables().size(); i++) {
		    SingleElementSymbol ses = insert.getVariables().get(i);
		    if (ses.getType() != insert.getQueryExpression().getProjectedSymbols().get(i).getType()) {
		        needsView = true;
		    }
		}
		if (needsView) {
		    try {
				insert.setQueryExpression(createInlineViewQuery(new GroupSymbol("X"), insert.getQueryExpression(), metadata, insert.getVariables())); //$NON-NLS-1$
			} catch (TeiidException err) {
	            throw new TeiidRuntimeException(err);
	        }
		}
		return insert;
	}

    private void correctProjectedTypes(List actualSymbolTypes, Query query) {
        
        List symbols = query.getSelect().getProjectedSymbols();
        
        List newSymbols = SetQuery.getTypedProjectedSymbols(symbols, actualSymbolTypes, this.metadata);
        
        query.getSelect().setSymbols(newSymbols);
    } 
    
	private SetQuery rewriteSetQuery(SetQuery setQuery)
				 throws TeiidComponentException, TeiidProcessingException{
        
        if (setQuery.getProjectedTypes() != null) {
            for (QueryCommand command : setQuery.getQueryCommands()) {
                if (!(command instanceof Query)) {
                    continue;
                }
                correctProjectedTypes(setQuery.getProjectedTypes(), (Query)command);
            }
            setQuery.setProjectedTypes(null, null);
        }
        
        setQuery.setLeftQuery((QueryCommand)rewriteCommand(setQuery.getLeftQuery(), true));
        setQuery.setRightQuery((QueryCommand)rewriteCommand(setQuery.getRightQuery(), true));

        rewriteOrderBy(setQuery);
        
        if (setQuery.getLimit() != null) {
            setQuery.setLimit(rewriteLimitClause(setQuery.getLimit()));
        }
        
        return setQuery;
    }

	private FromClause rewriteFromClause(Query parent, FromClause clause)
			 throws TeiidComponentException, TeiidProcessingException{
		if(clause instanceof JoinPredicate) {
			return rewriteJoinPredicate(parent, (JoinPredicate) clause);
        } else if (clause instanceof SubqueryFromClause) {
            rewriteSubqueryContainer((SubqueryFromClause)clause, true);
        } else if (clause instanceof TextTable) {
        	TextTable tt = (TextTable)clause;
        	tt.setFile(rewriteExpressionDirect(tt.getFile()));
        } else if (clause instanceof XMLTable) {
        	XMLTable xt = (XMLTable)clause;
        	xt.rewriteDefaultColumn();
        	rewriteExpressions(clause);
        } else if (clause instanceof ArrayTable) {
        	ArrayTable at = (ArrayTable)clause;
        	at.setArrayValue(rewriteExpressionDirect(at.getArrayValue()));
        }
        return clause;
	}

	private JoinPredicate rewriteJoinPredicate(Query parent, JoinPredicate predicate)
			 throws TeiidComponentException, TeiidProcessingException{
		List joinCrits = predicate.getJoinCriteria();
		if(joinCrits != null && joinCrits.size() > 0) {
			//rewrite join crits by rewriting a compound criteria
			Criteria criteria = new CompoundCriteria(new ArrayList(joinCrits));
            joinCrits.clear();
            criteria = rewriteCriteria(criteria);
            if (criteria instanceof CompoundCriteria && ((CompoundCriteria)criteria).getOperator() == CompoundCriteria.AND) {
                joinCrits.addAll(((CompoundCriteria)criteria).getCriteria());
            } else {
                joinCrits.add(criteria);
            }
			predicate.setJoinCriteria(joinCrits);
		}

        if (predicate.getJoinType() == JoinType.JOIN_UNION) {
            predicate.setJoinType(JoinType.JOIN_FULL_OUTER);
            predicate.setJoinCriteria(new ArrayList<Criteria>(Arrays.asList(FALSE_CRITERIA)));
        } else if (predicate.getJoinType() == JoinType.JOIN_RIGHT_OUTER) {
            predicate.setJoinType(JoinType.JOIN_LEFT_OUTER);
            FromClause leftClause = predicate.getLeftClause();
            predicate.setLeftClause(predicate.getRightClause());
            predicate.setRightClause(leftClause);
        }

        predicate.setLeftClause( rewriteFromClause(parent, predicate.getLeftClause() ));
        predicate.setRightClause( rewriteFromClause(parent, predicate.getRightClause() ));
    
		return predicate;
	}
    
    /**
     * Rewrite the criteria by evaluating some trivial cases.
     * @param criteria The criteria to rewrite
     * @param metadata
     * @param userCriteria The criteria on user's command, used in rewriting HasCriteria
     * in the procedural language.
     * @return The re-written criteria
     */
    public static Criteria rewriteCriteria(Criteria criteria, CreateUpdateProcedureCommand procCommand, CommandContext context, QueryMetadataInterface metadata) throws TeiidComponentException, TeiidProcessingException{
    	return new QueryRewriter(metadata, context, procCommand).rewriteCriteria(criteria);
    }

	/**
	 * Rewrite the criteria by evaluating some trivial cases.
	 * @param criteria The criteria to rewrite
	 * @param userCriteria The criteria on user's command, used in rewriting HasCriteria
	 * in the procedural language.
	 * @return The re-written criteria
	 */
    private Criteria rewriteCriteria(Criteria criteria) throws TeiidComponentException, TeiidProcessingException{
    	if(criteria instanceof CompoundCriteria) {
            return rewriteCriteria((CompoundCriteria)criteria, true);
		} else if(criteria instanceof NotCriteria) {
			criteria = rewriteCriteria((NotCriteria)criteria);
		} else if(criteria instanceof CompareCriteria) {
            criteria = rewriteCriteria((CompareCriteria)criteria);
        } else if(criteria instanceof SubqueryCompareCriteria) {
            criteria = rewriteCriteria((SubqueryCompareCriteria)criteria);
		} else if(criteria instanceof MatchCriteria) {
            criteria = rewriteCriteria((MatchCriteria)criteria);
		} else if(criteria instanceof SetCriteria) {
            criteria = rewriteCriteria((SetCriteria)criteria);
        } else if(criteria instanceof IsNullCriteria) {
            criteria = rewriteCriteria((IsNullCriteria)criteria);
        } else if(criteria instanceof BetweenCriteria) {
            criteria = rewriteCriteria((BetweenCriteria)criteria);
		} else if(criteria instanceof HasCriteria) {
            criteria = rewriteCriteria((HasCriteria)criteria);
		} else if(criteria instanceof TranslateCriteria) {
            criteria = rewriteCriteria((TranslateCriteria)criteria);
		} else if (criteria instanceof ExistsCriteria) {
			ExistsCriteria exists = (ExistsCriteria)criteria;
			if (exists.shouldEvaluate() && processing) {
        		return getCriteria(evaluator.evaluate(exists, null));
        	}
		    rewriteSubqueryContainer((SubqueryContainer)criteria, true);
			if (!RelationalNodeUtil.shouldExecute(exists.getCommand(), false, true)) {
            	return exists.isNegated()?TRUE_CRITERIA:FALSE_CRITERIA;
            }
		    if (exists.getCommand().getProcessorPlan() == null) {
		    	if (exists.getCommand() instanceof Query) {
					Query query = (Query)exists.getCommand();
					if ((query.getLimit() == null || query.getOrderBy() == null) && query.getSelect().getProjectedSymbols().size() > 1) {
						query.getSelect().clearSymbols();
						query.getSelect().addSymbol(new ExpressionSymbol("x", new Constant(1))); //$NON-NLS-1$
					}
				}
	            addImplicitLimit(exists, 1);
		    }
		} else if (criteria instanceof SubquerySetCriteria) {
		    SubquerySetCriteria sub = (SubquerySetCriteria)criteria;
		    if (rewriteLeftExpression(sub)) {
		    	return UNKNOWN_CRITERIA;
		    }
		    rewriteSubqueryContainer(sub, true);
		    if (!RelationalNodeUtil.shouldExecute(sub.getCommand(), false, true)) {
		    	return getSimpliedCriteria(criteria, sub.getExpression(), !sub.isNegated(), true);
		    }
        } else if (criteria instanceof DependentSetCriteria) {
            criteria = rewriteDependentSetCriteria((DependentSetCriteria)criteria);
        } else if (criteria instanceof ExpressionCriteria) {
        	return new CompareCriteria(((ExpressionCriteria) criteria).getExpression(), CompareCriteria.EQ, new Constant(Boolean.TRUE));
        }
    	
        return evaluateCriteria(criteria);
	}

	private void addImplicitLimit(SubqueryContainer<QueryCommand> container, int rowLimit) {
		if (container.getCommand().getLimit() != null) {
			Limit lim = container.getCommand().getLimit();
			if (lim.getRowLimit() instanceof Constant) {
				Constant c = (Constant)lim.getRowLimit();
				if (!c.isMultiValued() && Integer.valueOf(rowLimit).compareTo((Integer) c.getValue()) <= 0) {
					lim.setRowLimit(new Constant(rowLimit));
					if (lim.getRowLimit() == null) {
						lim.setImplicit(true);
						container.getCommand().setOrderBy(null);
					}
				}
			}
			return;
		}
		boolean addLimit = true;
		if (container.getCommand() instanceof Query) {
			Query query = (Query)container.getCommand();
			addLimit = !(query.hasAggregates() && query.getGroupBy() == null);
		}
		if (addLimit) {
			Limit lim = new Limit(null, new Constant(rowLimit));
			lim.setImplicit(true);
			container.getCommand().setLimit(lim);
		}
	}

	private Criteria rewriteDependentSetCriteria(DependentSetCriteria dsc)
			throws TeiidComponentException, TeiidProcessingException{
		if (!processing) {
			if (rewriteLeftExpression(dsc)) {
				return UNKNOWN_CRITERIA;
			}
		}
		return dsc;
	}
    
    /**
     * Performs simple expression flattening
     *  
     * @param criteria
     * @return
     */
    public static Criteria optimizeCriteria(CompoundCriteria criteria, QueryMetadataInterface metadata) {
        try {
            return new QueryRewriter(metadata, null, null).rewriteCriteria(criteria, false);
        } catch (TeiidException err) {
            //shouldn't happen
            return criteria;
        }
    }
    
    /** May be simplified if this is an AND and a sub criteria is always
     * false or if this is an OR and a sub criteria is always true
     */
    private Criteria rewriteCriteria(CompoundCriteria criteria, boolean rewrite) throws TeiidComponentException, TeiidProcessingException{
        List<Criteria> crits = criteria.getCriteria();
        int operator = criteria.getOperator();

        // Walk through crits and collect converted ones
        LinkedHashSet<Criteria> newCrits = new LinkedHashSet<Criteria>(crits.size());
        HashMap<Expression, Criteria> exprMap = new HashMap<Expression, Criteria>();
        for (Criteria converted : crits) {
            if (rewrite) {
                converted = rewriteCriteria(converted);
            } else if (converted instanceof CompoundCriteria) {
                converted = rewriteCriteria((CompoundCriteria)converted, false);
            }
            List<Criteria> critList = null;
            if (converted instanceof CompoundCriteria) {
                CompoundCriteria other = (CompoundCriteria)converted;
                if (other.getOperator() == criteria.getOperator()) {
                	critList = other.getCriteria(); 
                }
            }
            if (critList == null) {
            	critList = Arrays.asList(converted);
            }
        	for (Criteria criteria2 : critList) {
        		converted = criteria2;
                //begin boolean optimizations
                if(TRUE_CRITERIA.equals(converted)) {
                    if(operator == CompoundCriteria.OR) {
                        // this OR must be true as at least one branch is always true
                        return converted;
                    }
                } else if(FALSE_CRITERIA.equals(converted)) {
                    if(operator == CompoundCriteria.AND) {
                        // this AND must be false as at least one branch is always false
                        return converted;
                    }
                } else if (UNKNOWN_CRITERIA.equals(converted)) {
                    if (operator == CompoundCriteria.AND) {
                        return FALSE_CRITERIA;
                    } 
                } else { 
                    if (operator == CompoundCriteria.AND) {
	                	 converted = rewriteAndConjunct(converted, exprMap, newCrits);
	                	 if (converted != null) {
	                		 return converted;
	                	 }
                    } else {
                    	//or
                    	if (converted instanceof SetCriteria) {
                    		SetCriteria sc = (SetCriteria)converted;
                    		if (!sc.isNegated() && sc.isAllConstants()) {
			                	Criteria crit = exprMap.get(sc.getExpression());
			                	if (crit == null) {
			                		exprMap.put(sc.getExpression(), sc);
			                	} else if (crit instanceof SetCriteria) {
			                		SetCriteria other = (SetCriteria)crit;
			                		other.getValues().addAll(sc.getValues());
			                		continue;
			                	} else {
				                	newCrits.remove(crit);
			                		CompareCriteria cc = (CompareCriteria)crit;
			                		sc.getValues().add(cc.getRightExpression());
			                	}
		                	}
                    	} else if (converted instanceof CompareCriteria) {
                    		CompareCriteria cc = (CompareCriteria)converted;
                    		if (cc.getOperator() == CompareCriteria.EQ && cc.getRightExpression() instanceof Constant) {
			                	Criteria crit = exprMap.get(cc.getLeftExpression());
			                	if (crit == null) {
			                		exprMap.put(cc.getLeftExpression(), cc);
			                	} else if (crit instanceof SetCriteria) {
			                		SetCriteria other = (SetCriteria)crit;
			                		other.getValues().add(cc.getRightExpression());
			                		continue;
			                	} else {
				                	newCrits.remove(crit);
			                		CompareCriteria other = (CompareCriteria)crit;
			                		SetCriteria sc = new SetCriteria(cc.getLeftExpression(), new LinkedHashSet<Expression>());
			                		sc.setAllConstants(true);
			                		sc.getValues().add(cc.getRightExpression());
			                		sc.getValues().add(other.getRightExpression());
			                		exprMap.put(sc.getExpression(), sc);
			                		converted = sc;
			                	}
                    		}
                    	}
                        newCrits.add(converted);
                    }
                }            
            }
		}

        if(newCrits.size() == 0) {
            if(operator == CompoundCriteria.AND) {
                return TRUE_CRITERIA;
            }
            return FALSE_CRITERIA;
        } else if(newCrits.size() == 1) {
            // Only one sub crit now, so just return it
            return newCrits.iterator().next();
        } else {
            criteria.getCriteria().clear();
            criteria.getCriteria().addAll(newCrits);
            return criteria;
        }
	}
    
    /**
     * Rewrite the given conjunct
     * @return null if the conjunct was internally handled 
     */
    private Criteria rewriteAndConjunct(Criteria converted, Map<Expression, Criteria> exprMap, LinkedHashSet<Criteria> newCrits) throws ExpressionEvaluationException {
    	if (converted instanceof IsNullCriteria) {
        	IsNullCriteria inc = (IsNullCriteria)converted;
        	if (!inc.isNegated()) {
            	Criteria crit = exprMap.get(inc.getExpression());
            	if (crit == null) {
            		exprMap.put(inc.getExpression(), converted);
            	} else if (!(crit instanceof IsNullCriteria)) {
            		return FALSE_CRITERIA;
            	}
        	}
        } else if (converted instanceof SetCriteria) {
        	SetCriteria sc = (SetCriteria)converted;
        	Criteria crit = exprMap.get(sc.getExpression());
        	if (crit instanceof IsNullCriteria) {
        		return FALSE_CRITERIA;
        	}
        	if (!sc.isNegated() && sc.isAllConstants()) {
            	if (crit == null) {
            		exprMap.put(sc.getExpression(), converted);
            	} else if (crit instanceof SetCriteria) {
            		SetCriteria sc1 = (SetCriteria)crit;
            		newCrits.remove(sc1);
            		sc1.getValues().retainAll(sc.getValues());
            		if (sc1.getValues().isEmpty()) {
            			return FALSE_CRITERIA;
            		}
            		//TODO: single value as compare criteria
            		newCrits.add(sc1);
            		exprMap.put(sc1.getExpression(), sc1);
            		return null;
            	} else {
            		CompareCriteria cc = (CompareCriteria)crit;
            		for (Iterator<Constant> exprIter = sc.getValues().iterator(); exprIter.hasNext();) {
						if (!Evaluator.compare(cc, exprIter.next().getValue(), ((Constant)cc.getRightExpression()).getValue())) {
							exprIter.remove();
						}
					}
            		if (sc.getValues().isEmpty()) {
            			return FALSE_CRITERIA;
            		}
            		if (cc.getOperator() != CompareCriteria.EQ) {
                		newCrits.remove(cc);
                		//TODO: single value as compare criteria
                		exprMap.put(sc.getExpression(), sc);
            		} else {
            			return null;
            		}
            	}
        	}
        } else if (converted instanceof CompareCriteria) {
        	CompareCriteria cc = (CompareCriteria)converted;
        	Criteria crit = exprMap.get(cc.getLeftExpression());
        	if (crit instanceof IsNullCriteria) {
        		return FALSE_CRITERIA;
        	}
        	if (cc.getRightExpression() instanceof Constant) {
            	if (crit == null) {
            		exprMap.put(cc.getLeftExpression(), cc);
            	} else if (crit instanceof SetCriteria) {
            		SetCriteria sc = (SetCriteria)crit;
            		boolean modified = false;
            		for (Iterator<Constant> exprIter = sc.getValues().iterator(); exprIter.hasNext();) {
						if (!Evaluator.compare(cc, exprIter.next().getValue(), ((Constant)cc.getRightExpression()).getValue())) {
							if (!modified) {
								modified = true;
								newCrits.remove(sc);
							}
							exprIter.remove();
						}
					}
            		//TODO: single value as compare criteria
            		if (sc.getValues().isEmpty()) {
            			return FALSE_CRITERIA;
            		}
            		if (cc.getOperator() == CompareCriteria.EQ) {
                		exprMap.put(cc.getLeftExpression(), cc);
            		} else if (modified) {
            			newCrits.add(sc);
            			exprMap.put(sc.getExpression(), sc);
                		return null;
            		}
            	} else {
            		CompareCriteria cc1 = (CompareCriteria)crit;
            		if (cc1.getOperator() == CompareCriteria.NE) {
                		exprMap.put(cc.getLeftExpression(), cc);
            		} else if (cc1.getOperator() == CompareCriteria.EQ) {
            			if (!Evaluator.compare(cc1, ((Constant)cc1.getRightExpression()).getValue(), ((Constant)cc.getRightExpression()).getValue())) {
							return FALSE_CRITERIA;
						}
            			return null;
            		} 
            		if (cc.getOperator() == CompareCriteria.EQ) {
            			if (!Evaluator.compare(cc1, ((Constant)cc.getRightExpression()).getValue(), ((Constant)cc1.getRightExpression()).getValue())) {
            				return FALSE_CRITERIA;
            			}
            			exprMap.put(cc.getLeftExpression(), cc);
            			newCrits.remove(cc1);
            		}
            	}
        	}
        }
    	newCrits.add(converted);
    	return null;
    }
    
    private Criteria evaluateCriteria(Criteria crit) throws TeiidComponentException, TeiidProcessingException{
        if(EvaluatableVisitor.isFullyEvaluatable(crit, !processing)) {
            try {
            	Boolean eval = evaluator.evaluateTVL(crit, Collections.emptyList());
                
                return getCriteria(eval);                
                
            } catch(ExpressionEvaluationException e) {
                throw new QueryValidatorException(e, "ERR.015.009.0001", QueryPlugin.Util.getString("ERR.015.009.0001", crit)); //$NON-NLS-1$ //$NON-NLS-2$
            }
        }
        
        return crit;
    }

	private Criteria getCriteria(Boolean eval) {
		if (eval == null) {
		    return UNKNOWN_CRITERIA;
		}
		
		if(Boolean.TRUE.equals(eval)) {
		    return TRUE_CRITERIA;
		}
		
		return FALSE_CRITERIA;
	}

	private Criteria rewriteCriteria(NotCriteria criteria) throws TeiidComponentException, TeiidProcessingException{
		Criteria innerCrit = criteria.getCriteria(); 
        if (innerCrit instanceof CompoundCriteria) {
        	//reduce to only negation of predicates, so that the null/unknown handling criteria is applied appropriately
    		return rewriteCriteria(Criteria.applyDemorgan(innerCrit));
        } 
        if (innerCrit instanceof Negatable) {
        	((Negatable) innerCrit).negate();
        	return rewriteCriteria(innerCrit);
        }
        if (innerCrit instanceof NotCriteria) {
        	return rewriteCriteria(((NotCriteria)innerCrit).getCriteria());
        }
        innerCrit = rewriteCriteria(innerCrit);
        if(innerCrit == TRUE_CRITERIA) {
            return FALSE_CRITERIA;
        } else if(innerCrit == FALSE_CRITERIA) {
            return TRUE_CRITERIA;
        } else if (innerCrit == UNKNOWN_CRITERIA) {
            return UNKNOWN_CRITERIA;
        }
        criteria.setCriteria(innerCrit);
        return criteria;
	}

    /**
     * Rewrites "a [NOT] BETWEEN b AND c" as "a &gt;= b AND a &lt;= c", or as "a &lt;= b OR a&gt;= c"
     * @param criteria
     * @return
     * @throws QueryValidatorException
     */
    private Criteria rewriteCriteria(BetweenCriteria criteria) throws TeiidComponentException, TeiidProcessingException{
        CompareCriteria lowerCriteria = new CompareCriteria(criteria.getExpression(),
                                                            criteria.isNegated() ? CompareCriteria.LT: CompareCriteria.GE,
                                                            criteria.getLowerExpression());
        CompareCriteria upperCriteria = new CompareCriteria(criteria.getExpression(),
                                                            criteria.isNegated() ? CompareCriteria.GT: CompareCriteria.LE,
                                                            criteria.getUpperExpression());
        CompoundCriteria newCriteria = new CompoundCriteria(criteria.isNegated() ? CompoundCriteria.OR : CompoundCriteria.AND,
                                                            lowerCriteria,
                                                            upperCriteria);

        return rewriteCriteria(newCriteria);
    }

	private Criteria rewriteCriteria(CompareCriteria criteria) throws TeiidComponentException, TeiidProcessingException{
		Expression leftExpr = rewriteExpressionDirect(criteria.getLeftExpression());
		Expression rightExpr = rewriteExpressionDirect(criteria.getRightExpression());
		criteria.setLeftExpression(leftExpr);
		criteria.setRightExpression(rightExpr);

        if (isNull(leftExpr) || isNull(rightExpr)) {
            return UNKNOWN_CRITERIA;
        }

        boolean rightConstant = false;
        if(EvaluatableVisitor.willBecomeConstant(rightExpr)) {
        	rightConstant = true;
        } else if (EvaluatableVisitor.willBecomeConstant(leftExpr)) {
            // Swap in this particular case for connectors
            criteria.setLeftExpression(rightExpr);
            criteria.setRightExpression(leftExpr);

            // Check for < or > operator as we have to switch it
            switch(criteria.getOperator()) {
                case CompareCriteria.LT:    criteria.setOperator(CompareCriteria.GT);   break;
                case CompareCriteria.LE:    criteria.setOperator(CompareCriteria.GE);   break;
                case CompareCriteria.GT:    criteria.setOperator(CompareCriteria.LT);   break;
                case CompareCriteria.GE:    criteria.setOperator(CompareCriteria.LE);   break;
            }
            rightConstant = true;
		} 
        
    	Function f = null;
    	while (rightConstant && f != criteria.getLeftExpression() && criteria.getLeftExpression() instanceof Function) {
            f = (Function)criteria.getLeftExpression();
        	Criteria result = simplifyWithInverse(criteria);
        	if (!(result instanceof CompareCriteria)) {
        		return result;
        	}
        	criteria = (CompareCriteria)result;
    	}
        
        Criteria modCriteria = simplifyTimestampMerge(criteria);
        if(modCriteria instanceof CompareCriteria) {
            modCriteria = simplifyTimestampMerge2((CompareCriteria)modCriteria);
        }
        return modCriteria;
    }

    public static boolean isNull(Expression expr) {
        return expr instanceof Constant && ((Constant)expr).isNull();
    }

    /*
     * The thing of primary importance here is that the use of the 'ANY' predicate
     * quantifier is replaced with the canonical and equivalent 'SOME'
     */
    private Criteria rewriteCriteria(SubqueryCompareCriteria criteria) throws TeiidComponentException, TeiidProcessingException{
    	
    	if (criteria.getCommand().getProcessorPlan() == null && criteria.getPredicateQuantifier() != SubqueryCompareCriteria.ALL) {
    		if (criteria.getOperator() == CompareCriteria.EQ || criteria.getOperator() == CompareCriteria.NE) {
    			SubquerySetCriteria result = new SubquerySetCriteria(criteria.getLeftExpression(), criteria.getCommand());
    			result.setNegated(criteria.getOperator() == CompareCriteria.NE);
    			return rewriteCriteria(result);
    		}
    		CompareCriteria cc = new CompareCriteria();
    		cc.setLeftExpression(criteria.getLeftExpression());
    		Query q = createInlineViewQuery(new GroupSymbol("X"), criteria.getCommand(), metadata, criteria.getCommand().getProjectedSymbols()); //$NON-NLS-1$
    		SingleElementSymbol ses = q.getProjectedSymbols().get(0);
    		Expression expr = SymbolMap.getExpression(ses);
    		q.getSelect().clearSymbols();
    		AggregateSymbol.Type type = Type.MAX;
    		if (criteria.getOperator() == CompareCriteria.GT || criteria.getOperator() == CompareCriteria.GE) {
    			type = Type.MIN;
    		}
    		q.getSelect().addSymbol(new AggregateSymbol(ses.getName(), type.name(), false, expr));
    		cc.setRightExpression(new ScalarSubquery(q));
			cc.setOperator(criteria.getOperator());
    		return rewriteCriteria(cc);
    	}

        Expression leftExpr = rewriteExpressionDirect(criteria.getLeftExpression());
        
        if (isNull(leftExpr)) {
            return UNKNOWN_CRITERIA;
        }
        
        criteria.setLeftExpression(leftExpr);

        if (criteria.getPredicateQuantifier() == SubqueryCompareCriteria.ANY){
            criteria.setPredicateQuantifier(SubqueryCompareCriteria.SOME);
        }
        
        rewriteSubqueryContainer(criteria, true);
        
        if (!RelationalNodeUtil.shouldExecute(criteria.getCommand(), false, true)) {
	    	return getSimpliedCriteria(criteria, criteria.getLeftExpression(), criteria.getPredicateQuantifier()==SubqueryCompareCriteria.ALL, true);
	    }

        return criteria;
    }
    
    private Criteria simplifyWithInverse(CompareCriteria criteria) throws TeiidComponentException, TeiidProcessingException{
        Expression leftExpr = criteria.getLeftExpression();
        
        Function leftFunction = (Function) leftExpr;
        if(isSimpleMathematicalFunction(leftFunction)) {
            return simplifyMathematicalCriteria(criteria);
        }   
        if (FunctionLibrary.isConvert(leftFunction)) {
        	return simplifyConvertFunction(criteria);
        }
        return simplifyParseFormatFunction(criteria);
    }
    
    private boolean isSimpleMathematicalFunction(Function function) {
        String funcName = function.getName();
        if(funcName.equals("+") || funcName.equals("-") || funcName.equals("*") || funcName.equals("/")) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            Expression[] args = function.getArgs();
            if(args[0] instanceof Constant || args[1] instanceof Constant) {
                return true;
            }
        }

        // fall through - not simple mathematical
        return false;
    }

    // Constants used in simplifying mathematical criteria
    private Integer INTEGER_ZERO = new Integer(0);
    private Double DOUBLE_ZERO = new Double(0);
    private Float FLOAT_ZERO = new Float(0);
    private Long LONG_ZERO = new Long(0);
    private BigInteger BIG_INTEGER_ZERO = new BigInteger("0"); //$NON-NLS-1$
    private BigDecimal BIG_DECIMAL_ZERO = new BigDecimal("0"); //$NON-NLS-1$
    private Short SHORT_ZERO = new Short((short)0);
    private Byte BYTE_ZERO = new Byte((byte)0);

    /**
     * @param criteria
     * @return CompareCriteria
     */
    private CompareCriteria simplifyMathematicalCriteria(CompareCriteria criteria)
    throws TeiidComponentException, TeiidProcessingException{

        Expression leftExpr = criteria.getLeftExpression();
        Expression rightExpr = criteria.getRightExpression();

        // Identify all the pieces of this criteria
        Function function = (Function) leftExpr;
        String funcName = function.getName();
        Expression[] args = function.getArgs();
        Constant const1 = null;
        Expression expr = null;
        if(args[1] instanceof Constant) {
            const1 = (Constant) args[1];
            expr = args[0];
        } else {
            if(funcName.equals("+") || funcName.equals("*")) { //$NON-NLS-1$ //$NON-NLS-2$
                const1 = (Constant) args[0];
                expr = args[1];
            } else {
                // If we have "5 - x = 10" or "5 / x = 10", abort!
                return criteria;
            }
        }
        int operator = criteria.getOperator();

        // Determine opposite function
        String oppFunc = null;
        switch(funcName.charAt(0)) {
            case '+':   oppFunc = "-";  break; //$NON-NLS-1$
            case '-':   oppFunc = "+";  break; //$NON-NLS-1$
            case '*':   oppFunc = "/";  break; //$NON-NLS-1$
            case '/':   oppFunc = "*";  break; //$NON-NLS-1$
        }

        // Create a function of the two constants and evaluate it
        Expression combinedConst = null;
        FunctionLibrary funcLib = this.metadata.getFunctionLibrary();
        FunctionDescriptor descriptor = funcLib.findFunction(oppFunc, new Class[] { rightExpr.getType(), const1.getType() });
        if (descriptor == null){
            //See defect 9380 - this can be caused by const2 being a null Constant, for example (? + 1) < null
            return criteria;
        }

        
        if (rightExpr instanceof Constant) {
            Constant const2 = (Constant)rightExpr;
            try {
                Object result = descriptor.invokeFunction(new Object[] { const2.getValue(), const1.getValue() } );
                combinedConst = new Constant(result, descriptor.getReturnType());
            } catch(FunctionExecutionException e) {
            	throw new QueryValidatorException(e, "ERR.015.009.0003", QueryPlugin.Util.getString("ERR.015.009.0003", e.getMessage())); //$NON-NLS-1$ //$NON-NLS-2$
        	}
        } else {
            Function conversion = new Function(descriptor.getName(), new Expression[] { rightExpr, const1 });
            conversion.setType(leftExpr.getType());
            conversion.setFunctionDescriptor(descriptor);
            combinedConst = conversion;
            
        }

        // Flip operator if necessary
        if(! (operator == CompareCriteria.EQ || operator == CompareCriteria.NE) &&
             (oppFunc.equals("*") || oppFunc.equals("/")) ) { //$NON-NLS-1$ //$NON-NLS-2$

            Object value = const1.getValue();
            if(value != null) {
                Class type = const1.getType();
                Comparable comparisonObject = null;
                if(type.equals(DataTypeManager.DefaultDataClasses.INTEGER)) {
                    comparisonObject = INTEGER_ZERO;
                } else if(type.equals(DataTypeManager.DefaultDataClasses.DOUBLE)) {
                    comparisonObject = DOUBLE_ZERO;
                } else if(type.equals(DataTypeManager.DefaultDataClasses.FLOAT)) {
                    comparisonObject = FLOAT_ZERO;
                } else if(type.equals(DataTypeManager.DefaultDataClasses.LONG)) {
                    comparisonObject = LONG_ZERO;
                } else if(type.equals(DataTypeManager.DefaultDataClasses.BIG_INTEGER)) {
                    comparisonObject = BIG_INTEGER_ZERO;
                } else if(type.equals(DataTypeManager.DefaultDataClasses.BIG_DECIMAL)) {
                    comparisonObject = BIG_DECIMAL_ZERO;
                } else if(type.equals(DataTypeManager.DefaultDataClasses.SHORT)) {
                    comparisonObject = SHORT_ZERO;
                } else if(type.equals(DataTypeManager.DefaultDataClasses.BYTE)) {
                    comparisonObject = BYTE_ZERO;
                } else {
                    // Unknown type
                    return criteria;
                }

                // If value is less than comparison object (which is zero),
                // then need to switch operator.
                if(comparisonObject.compareTo(value) > 0) {
                    switch(operator) {
                        case CompareCriteria.LE:    operator = CompareCriteria.GE;  break;
                        case CompareCriteria.LT:    operator = CompareCriteria.GT;  break;
                        case CompareCriteria.GE:    operator = CompareCriteria.LE;  break;
                        case CompareCriteria.GT:    operator = CompareCriteria.LT;  break;
                    }
                }
            }
        }
        
        criteria.setLeftExpression(expr);
        criteria.setRightExpression(combinedConst);
        criteria.setOperator(operator);

        // Return new simplified criteria
        return criteria;
    }

    /**
     * This method attempts to rewrite compare criteria of the form 
     * 
     * <code>convert(typedColumn, string) = '5'</code>
     * 
     * into 
     * 
     * <code>typedColumn = convert('5', typeOfColumn)</code>
     * where 'typeOfColumn' is the type of 'typedColumn'
     * 
     * if, for example, the type of the column is integer, than the above
     * can be pre-evaluated to
     * 
     * <code>typedColumn = 5 </code> 
     * 
     * Right expression has already been checked to be a Constant, left expression to be
     * a function.  Function is known to be "convert" or "cast".
     * 
     * @param crit CompareCriteria
     * @return same Criteria instance (possibly optimized)
     * @throws QueryValidatorException
     * @since 4.2
     */
    private Criteria simplifyConvertFunction(CompareCriteria crit) {
        Function leftFunction = (Function) crit.getLeftExpression();
        Expression leftExpr = leftFunction.getArgs()[0];
        
        if(!(crit.getRightExpression() instanceof Constant) 
        		//TODO: this can be relaxed for order preserving operations
        		|| !(crit.getOperator() == CompareCriteria.EQ || crit.getOperator() == CompareCriteria.NE)) { 
        	return crit;
        } 

        Constant rightConstant = (Constant) crit.getRightExpression();
        
        String leftExprTypeName = DataTypeManager.getDataTypeName(leftExpr.getType());

        Constant result = ResolverUtil.convertConstant(DataTypeManager.getDataTypeName(rightConstant.getType()), leftExprTypeName, rightConstant);
        if (result == null) {
        	return getSimpliedCriteria(crit, leftExpr, crit.getOperator() != CompareCriteria.EQ, true);
        }
        Constant other = ResolverUtil.convertConstant(leftExprTypeName, DataTypeManager.getDataTypeName(rightConstant.getType()), result);
        if (other == null || ((Comparable)rightConstant.getValue()).compareTo(other.getValue()) != 0) {
        	return getSimpliedCriteria(crit, leftExpr, crit.getOperator() != CompareCriteria.EQ, true);
        }
        
        if (!DataTypeManager.isImplicitConversion(leftExprTypeName, DataTypeManager.getDataTypeName(rightConstant.getType()))) {
        	return crit;
        }
                
    	crit.setRightExpression(result);
        crit.setLeftExpression(leftExpr);

        return crit;
    }


    /**
     * This method attempts to rewrite set criteria of the form 
     * 
     * <code>convert(typedColumn, string) in  ('5', '6')</code>
     * 
     * into 
     * 
     * <code>typedColumn in (convert('5', typeOfColumn), convert('6', typeOfColumn)) </code>
     * where 'typeOfColumn' is the type of 'typedColumn'
     * 
     * if, for example, the type of the column is integer, than the above
     * can be pre-evaluated to
     * 
     * <code>typedColumn in (5,6)  </code> 
     * 
     * Right expression has already been checked to be a Constant, left expression to be
     * a function.  Function is known to be "convert" or "cast".  The scope of this change
     * will be limited to the case where the left expression is attempting to convert to
     * 'string'.  
     * 
     * @param crit CompareCriteria
     * @return same Criteria instance (possibly optimized)
     * @throws QueryValidatorException
     * @since 4.2
     */
	private Criteria simplifyConvertFunction(SetCriteria crit) throws TeiidComponentException, TeiidProcessingException{
        Function leftFunction = (Function) crit.getExpression();
        Expression leftExpr = leftFunction.getArgs()[0];
        String leftExprTypeName = DataTypeManager.getDataTypeName(leftExpr.getType());
        
        Iterator i = crit.getValues().iterator();
        Collection newValues = new ArrayList(crit.getNumberOfValues());
        
        boolean convertedAll = true;
        boolean removedSome = false;
        while (i.hasNext()) {
            Object next = i.next();
            if (!(next instanceof Constant)) {
            	convertedAll = false;
            	continue;
            }

            Constant rightConstant = (Constant) next;
            
            Constant result = ResolverUtil.convertConstant(DataTypeManager.getDataTypeName(rightConstant.getType()), leftExprTypeName, rightConstant);
            if (result != null) {
                Constant other = ResolverUtil.convertConstant(leftExprTypeName, DataTypeManager.getDataTypeName(rightConstant.getType()), result);
                if (other == null || ((Comparable)rightConstant.getValue()).compareTo(other.getValue()) != 0) {
                	result = null;
                }   
            }
            
            if (result == null) {
            	removedSome = true;
            	i.remove();
            } else if (DataTypeManager.isImplicitConversion(leftExprTypeName, DataTypeManager.getDataTypeName(rightConstant.getType()))) {
            	newValues.add(result);
            } else {
            	convertedAll = false;
            }
        }
        
        if (!convertedAll) {
        	if (!removedSome) {
        		return crit; //just return as is
        	}
        } else {
        	if (newValues.isEmpty()) {
        		return getSimpliedCriteria(crit, leftExpr, !crit.isNegated(), true);
        	}
	        crit.setExpression(leftExpr);
	        crit.setValues(newValues);
        }
        return rewriteCriteria(crit);
    }
        
    private Criteria simplifyParseFormatFunction(CompareCriteria crit) {
    	//TODO: this can be relaxed for order preserving operations
        if(!(crit.getOperator() == CompareCriteria.EQ || crit.getOperator() == CompareCriteria.NE)) {
        	return crit;
        }
    	boolean isFormat = false;
    	Function leftFunction = (Function) crit.getLeftExpression();
        String funcName = leftFunction.getName().toLowerCase();
        String inverseFunction = null;
        if(funcName.startsWith("parse")) { //$NON-NLS-1$
            String type = funcName.substring(5);
            if (!PARSE_FORMAT_TYPES.contains(type)) {
                return crit;
            }
            inverseFunction = "format" + type; //$NON-NLS-1$
        } else if(funcName.startsWith("format")) { //$NON-NLS-1$
            String type = funcName.substring(6);
            if (!PARSE_FORMAT_TYPES.contains(type)) {
                return crit;
            }
            inverseFunction = "parse" + type; //$NON-NLS-1$
            isFormat = true;
        } else {
            return crit;
        }
        Expression rightExpr = crit.getRightExpression();
        if (!(rightExpr instanceof Constant)) {
        	return crit;
        }
        Expression leftExpr = leftFunction.getArgs()[0];
        Expression formatExpr = leftFunction.getArgs()[1];
        if(!(formatExpr instanceof Constant)) {
            return crit;
        }
        String format = (String)((Constant)formatExpr).getValue();
        FunctionLibrary funcLib = this.metadata.getFunctionLibrary();
        FunctionDescriptor descriptor = funcLib.findFunction(inverseFunction, new Class[] { rightExpr.getType(), formatExpr.getType() });
        if(descriptor == null){
            return crit;
        }
    	Object value = ((Constant)rightExpr).getValue();
    	try {
    		Object result = descriptor.invokeFunction(new Object[] {((Constant)rightExpr).getValue(), format});
    		result = leftFunction.getFunctionDescriptor().invokeFunction(new Object[] { result, format } );
    		if (((Comparable)value).compareTo(result) != 0) {
    			return getSimpliedCriteria(crit, leftExpr, crit.getOperator() != CompareCriteria.EQ, true);
    		}
    	} catch(FunctionExecutionException e) {
            //Not all numeric formats are invertable, so just return the criteria as it may still be valid
            return crit;
        }
        //parseFunctions are all potentially narrowing
        if (!isFormat) {
        	return crit;
        }
        //TODO: if format is not lossy, then invert the function
        return crit;
    }
      
    /**
     * This method applies a similar simplification as the previous method for Case 1829.  This is conceptually 
     * the same thing but done using the timestampCreate system function.  
     *     
     * TIMESTAMPCREATE(rpcolli_physical.RPCOLLI.Table_B.date_field, rpcolli_physical.RPCOLLI.Table_B.time_field)
     *    = {ts'1969-09-20 18:30:45.0'}
     *  
     *  -------------
     *  
     *   rpcolli_physical.RPCOLLI.Table_B.date_field = {d'1969-09-20'} 
     *   AND 
     *   rpcolli_physical.RPCOLLI.Table_B.time_field = {t'18:30:45'}
     * 
     * 
     * @param criteria Compare criteria
     * @return Simplified criteria, if possible
     */
    private Criteria simplifyTimestampMerge2(CompareCriteria criteria) {
        if(criteria.getOperator() != CompareCriteria.EQ) {
            return criteria;
        }
        
        Expression leftExpr = criteria.getLeftExpression();
        Expression rightExpr = criteria.getRightExpression();
        
        // Allow for concat and string literal to be on either side
        Function tsCreateFunction = null;
        Constant timestampConstant = null;        
        if(leftExpr instanceof Function && rightExpr instanceof Constant) {
            tsCreateFunction = (Function) leftExpr;
            timestampConstant = (Constant) rightExpr;            
        } else {
            return criteria;
        }

        // Verify data type of constant and that constant has a value
        if(! timestampConstant.getType().equals(DataTypeManager.DefaultDataClasses.TIMESTAMP)) {
            return criteria;        
        }
        
        // Verify function is timestampCreate function
        if(! (tsCreateFunction.getName().equalsIgnoreCase("timestampCreate"))) { //$NON-NLS-1$ 
            return criteria;
        }
        
        // Get timestamp literal and break into pieces
        Timestamp ts = (Timestamp) timestampConstant.getValue();
        String tsStr = ts.toString();
        Date date = Date.valueOf(tsStr.substring(0, 10)); 
        Time time = Time.valueOf(tsStr.substring(11, 19));
        
        // Get timestampCreate args
        Expression[] args = tsCreateFunction.getArgs();
        
        // Rebuild the function
        CompareCriteria dateCrit = new CompareCriteria(args[0], CompareCriteria.EQ, new Constant(date, DataTypeManager.DefaultDataClasses.DATE));
        CompareCriteria timeCrit = new CompareCriteria(args[1], CompareCriteria.EQ, new Constant(time, DataTypeManager.DefaultDataClasses.TIME));        
        CompoundCriteria compCrit = new CompoundCriteria(CompoundCriteria.AND, dateCrit, timeCrit);
        return compCrit;                     
    }

   /**
    * This method also applies the same simplification for Case 1829.  This is conceptually 
    * the same thing but done  using the timestampCreate system function.  
    *     
    * formatDate(rpcolli_physical.RPCOLLI.Table_B.date_field, 'yyyy-MM-dd') 
    *    || formatTime(rpcolli_physical.RPCOLLI.Table_B.time_field, ' HH:mm:ss') = '1969-09-20 18:30:45'
    *  
    *  -------------
    *  
    *   rpcolli_physical.RPCOLLI.Table_B.date_field = {d'1969-09-20'} 
    *   AND 
    *   rpcolli_physical.RPCOLLI.Table_B.time_field = {t'18:30:45'}
    * 
    * 
    * @param criteria Compare criteria
    * @return Simplified criteria, if possible
    */

   private Criteria simplifyTimestampMerge(CompareCriteria criteria) {
       if(criteria.getOperator() != CompareCriteria.EQ) {
           return criteria;
       }
       
       Expression leftExpr = criteria.getLeftExpression();
       Expression rightExpr = criteria.getRightExpression();
       
       // Allow for concat and string literal to be on either side
       Function concatFunction = null;
       Constant timestampConstant = null;        
       if(leftExpr instanceof Function && rightExpr instanceof Constant) {
           concatFunction = (Function) leftExpr;
           timestampConstant = (Constant) rightExpr;            
       } else {
           return criteria;
       }

       // Verify data type of string constant and that constant has a value
       if(! timestampConstant.getType().equals(DataTypeManager.DefaultDataClasses.STRING)) {
           return criteria;        
       }
       
       // Verify function is concat function
       if(! (concatFunction.getName().equalsIgnoreCase("concat") || concatFunction.getName().equals("||"))) { //$NON-NLS-1$ //$NON-NLS-2$
           return criteria;
       }
       
       // Verify concat has formatdate and formattime functions
       Expression[] args = concatFunction.getArgs();
       if(! (args[0] instanceof Function && args[1] instanceof Function)) {
           return criteria;
       }
       Function formatDateFunction = (Function) args[0];
       Function formatTimeFunction = (Function) args[1];
       if(! (formatDateFunction.getName().equalsIgnoreCase("formatdate") && formatTimeFunction.getName().equalsIgnoreCase("formattime"))) { //$NON-NLS-1$ //$NON-NLS-2$
           return criteria;
       }
       
       // Verify format functions have constants
       if(! (formatDateFunction.getArgs()[1] instanceof Constant && formatTimeFunction.getArgs()[1] instanceof Constant)) {
           return criteria;
       }
       
       // Verify length of combined date/time constants == timestamp constant
       String dateFormat = (String) ((Constant)formatDateFunction.getArgs()[1]).getValue();        
       String timeFormat = (String) ((Constant)formatTimeFunction.getArgs()[1]).getValue();        
       String timestampValue = (String) timestampConstant.getValue();
       
       // Passed all the checks, so build the optimized version
       try {
    	   Timestamp ts = FunctionMethods.parseTimestamp(timestampValue, dateFormat + timeFormat);
           Constant dateConstant = new Constant(TimestampWithTimezone.createDate(ts));
           CompareCriteria dateCompare = new CompareCriteria(formatDateFunction.getArgs()[0], CompareCriteria.EQ, dateConstant);

           Constant timeConstant = new Constant(TimestampWithTimezone.createTime(ts));
           CompareCriteria timeCompare = new CompareCriteria(formatTimeFunction.getArgs()[0], CompareCriteria.EQ, timeConstant);
           
           CompoundCriteria compCrit = new CompoundCriteria(CompoundCriteria.AND, dateCompare, timeCompare);
           return compCrit;
           
       } catch(FunctionExecutionException e) {
           return criteria;        
       }
    }
    
    private Criteria rewriteCriteria(MatchCriteria criteria) throws TeiidComponentException, TeiidProcessingException{
		criteria.setLeftExpression( rewriteExpressionDirect(criteria.getLeftExpression()));
		criteria.setRightExpression( rewriteExpressionDirect(criteria.getRightExpression()));
        
        if (isNull(criteria.getLeftExpression()) || isNull(criteria.getRightExpression())) {
            return UNKNOWN_CRITERIA;
        }

        Expression rightExpr = criteria.getRightExpression();
        if(rightExpr instanceof Constant && rightExpr.getType().equals(DataTypeManager.DefaultDataClasses.STRING)) {
            Constant constant = (Constant) rightExpr;
            String value = (String) constant.getValue();

            char escape = criteria.getEscapeChar();

            // Check whether escape char is unnecessary and remove it
            if(escape != MatchCriteria.NULL_ESCAPE_CHAR && value.indexOf(escape) < 0) {
                criteria.setEscapeChar(MatchCriteria.NULL_ESCAPE_CHAR);
            }

            // if the value of this string constant is '%', then we know the crit will 
            // always be true                    
            if ( value.equals( String.valueOf(MatchCriteria.WILDCARD_CHAR)) ) { 
                return getSimpliedCriteria(criteria, criteria.getLeftExpression(), !criteria.isNegated(), true);                                        
            } 
            
            // if both left and right expressions are strings, and the LIKE match characters ('*', '_') are not present 
            //  in the right expression, rewrite the criteria as EQUALs rather than LIKE
            if(DataTypeManager.DefaultDataClasses.STRING.equals(criteria.getLeftExpression().getType()) && value.indexOf(escape) < 0 && value.indexOf(MatchCriteria.MATCH_CHAR) < 0 && value.indexOf(MatchCriteria.WILDCARD_CHAR) < 0) {
            	return rewriteCriteria(new CompareCriteria(criteria.getLeftExpression(), criteria.isNegated()?CompareCriteria.NE:CompareCriteria.EQ, criteria.getRightExpression()));
            }
        }

		return criteria;
	}
    
	private Criteria getSimpliedCriteria(Criteria crit, Expression a, boolean outcome, boolean nullPossible) {
		if (nullPossible) {
			if (outcome) {
				if (processing) {
					return crit;
				}
				IsNullCriteria inc = new IsNullCriteria(a);
				inc.setNegated(true);
				return inc;
			}
		} else if (outcome) {
			return TRUE_CRITERIA;
		}
		return FALSE_CRITERIA;
	}
	
    private boolean rewriteLeftExpression(AbstractSetCriteria criteria) throws TeiidComponentException, TeiidProcessingException{
        criteria.setExpression(rewriteExpressionDirect(criteria.getExpression()));
        
        if (isNull(criteria.getExpression())) {
            return true;
        }

        return false;
    }

	private Criteria rewriteCriteria(SetCriteria criteria) throws TeiidComponentException, TeiidProcessingException{
		if (criteria.isAllConstants() && criteria.getValues().size() > 1) {
			return criteria;
		}
		
		criteria.setExpression(rewriteExpressionDirect(criteria.getExpression()));
        
        if (rewriteLeftExpression(criteria)) {
            return UNKNOWN_CRITERIA;
        }

		Collection vals = criteria.getValues();

        LinkedHashSet newVals = new LinkedHashSet(vals.size());
        Iterator valIter = vals.iterator();
        boolean allConstants = true;
        boolean hasNull = false;
        while(valIter.hasNext()) {
            Expression value = rewriteExpressionDirect( (Expression) valIter.next());
            if (isNull(value)) {
            	hasNull = true;
            	continue;
            }
            allConstants &= value instanceof Constant;
            newVals.add(value);
        }
        
        int size = newVals.size();
        if (size == 1) {
            Expression value = (Expression)newVals.iterator().next();
            return rewriteCriteria(new CompareCriteria(criteria.getExpression(), criteria.isNegated()?CompareCriteria.NE:CompareCriteria.EQ, value));
        } 
        
        criteria.setValues(newVals);
        if (allConstants) {
        	criteria.setAllConstants(true);
        }        
        
        if (size == 0) {
        	if (hasNull) {
        		return UNKNOWN_CRITERIA;
        	}
        	return getSimpliedCriteria(criteria, criteria.getExpression(), !criteria.isNegated(), true);
        }
        
        if(criteria.getExpression() instanceof Function ) {
            Function leftFunction = (Function)criteria.getExpression();
            if(FunctionLibrary.isConvert(leftFunction)) {
                return simplifyConvertFunction(criteria);        
            }
        }

		return criteria;
	}

	private Criteria rewriteCriteria(IsNullCriteria criteria) throws TeiidComponentException, TeiidProcessingException{
		criteria.setExpression(rewriteExpressionDirect(criteria.getExpression()));
		return criteria;
	}
	
	public static Expression rewriteExpression(Expression expression, CreateUpdateProcedureCommand procCommand, CommandContext context, QueryMetadataInterface metadata) throws TeiidComponentException, TeiidProcessingException{
		return new QueryRewriter(metadata, context, procCommand).rewriteExpressionDirect(expression);
	}

    private Expression rewriteExpressionDirect(Expression expression) throws TeiidComponentException, TeiidProcessingException{
    	if (expression instanceof Constant) {
    		return expression;
    	}
    	if (expression instanceof ElementSymbol) {
    		ElementSymbol es = (ElementSymbol)expression;
    		Class<?> type  = es.getType();
            if (!processing && es.isExternalReference()) {
                if (variables == null) {
                	return new Reference(es);
                }
                Expression value = variables.get(es);

                if (value == null) {
                	if (es.getGroupSymbol().getSchema() == null) {
                        String grpName = es.getGroupSymbol().getShortCanonicalName();
		                if (grpName.equals(ProcedureReservedWords.INPUTS)) {
		                	return new Constant(null, es.getType());
		                } 
		                if (grpName.equals(ProcedureReservedWords.CHANGING)) {
		                    Assertion.failed("Changing value should not be null"); //$NON-NLS-1$
		                } 
                	}
                } else if (value instanceof Constant) {
                	if (value.getType() == type) {
                		return value;
                	}
                	try {
						return new Constant(FunctionMethods.convert(((Constant)value).getValue(), DataTypeManager.getDataTypeName(type)), es.getType());
					} catch (FunctionExecutionException e) {
						throw new QueryValidatorException(e, e.getMessage());
					}
                }
                return new Reference(es);
            }
            return expression;
    	}
    	boolean isBindEligible = true;
    	if(expression instanceof Function) {
    		isBindEligible = !isConstantConvert(expression);
    		expression = rewriteFunction((Function) expression);
		} else if (expression instanceof CaseExpression) {
			expression = rewriteCaseExpression((CaseExpression)expression);
        } else if (expression instanceof SearchedCaseExpression) {
        	expression = rewriteCaseExpression((SearchedCaseExpression)expression);
        } else if (expression instanceof ScalarSubquery) {
        	ScalarSubquery subquery = (ScalarSubquery)expression;
        	if (subquery.shouldEvaluate() && processing) {
        		return new Constant(evaluator.evaluate(subquery, null), subquery.getType());
        	}
            rewriteSubqueryContainer(subquery, true);
            if (!RelationalNodeUtil.shouldExecute(subquery.getCommand(), false, true)) {
            	return new Constant(null, subquery.getType());
            }
            if (subquery.getCommand().getProcessorPlan() == null) {
            	addImplicitLimit(subquery, 2);
            }
            return expression;
        } else if (expression instanceof ExpressionSymbol) {
        	if (expression instanceof AggregateSymbol) {
        		expression = rewriteExpression((AggregateSymbol)expression);	
        	} else {
            	expression = rewriteExpressionDirect(((ExpressionSymbol)expression).getExpression());
        	}
        } else if (expression instanceof Criteria) {
        	expression = rewriteCriteria((Criteria)expression);
        } else {
        	rewriteExpressions(expression);
        } 
    	
        if(!processing) {
        	if (!EvaluatableVisitor.isFullyEvaluatable(expression, true)) {
        		return expression;
        	}
		} else if (!(expression instanceof Reference) && !EvaluatableVisitor.isEvaluatable(expression, EvaluationLevel.PROCESSING)) {
			return expression;
		}
    	
		return evaluate(expression, isBindEligible);
	}

	private Constant evaluate(Expression expression, boolean isBindEligible)
			throws ExpressionEvaluationException, BlockedException,
			TeiidComponentException {
		Object value = evaluator.evaluate(expression, Collections.emptyList());
        if (value instanceof Constant) {
        	return (Constant)value; //multi valued substitution
        }
		Constant result = new Constant(value, expression.getType());
		result.setBindEligible(isBindEligible);
		return result;
	}
    
    private boolean isConstantConvert(Expression ex) {
    	if (ex instanceof Constant) {
    		return true;
    	}
    	if (!(ex instanceof Function)) {
    		return false;
    	}
    	Function f = (Function)ex;
		if (!FunctionLibrary.isConvert(f)) {
			return false;
		}
		return isConstantConvert(f.getArg(0));
    }
    
    private Expression rewriteExpression(AggregateSymbol expression) {
    	if (expression.isBoolean()) {
    		if (expression.getAggregateFunction() == Type.EVERY) {
    			expression.setAggregateFunction(Type.MIN);
    		} else {
    			expression.setAggregateFunction(Type.MAX);
    		}
    	}
    	if ((expression.getAggregateFunction() == Type.MAX || expression.getAggregateFunction() == Type.MIN)
				&& EvaluatableVisitor.willBecomeConstant(expression.getExpression())) {
			try {
				return new ExpressionSymbol(expression.getName(), ResolverUtil
						.convertExpression(expression.getExpression(),DataTypeManager.getDataTypeName(expression.getType()), metadata));
			} catch (QueryResolverException e) {
				//should not happen, so throw as a runtime
				throw new TeiidRuntimeException(e);
			}
		}
		return expression;
	}
   
    private static Map<String, Integer> FUNCTION_MAP = new HashMap<String, Integer>();
    
    static {
    	FUNCTION_MAP.put(FunctionLibrary.SPACE.toLowerCase(), 0);
    	FUNCTION_MAP.put(FunctionLibrary.FROM_UNIXTIME.toLowerCase(), 1);
    	FUNCTION_MAP.put(FunctionLibrary.NULLIF.toLowerCase(), 2);
    	FUNCTION_MAP.put(FunctionLibrary.COALESCE.toLowerCase(), 3);
    	FUNCTION_MAP.put(FunctionLibrary.CONCAT2.toLowerCase(), 4);
    	FUNCTION_MAP.put(FunctionLibrary.TIMESTAMPADD.toLowerCase(), 5);
    	FUNCTION_MAP.put(FunctionLibrary.PARSEDATE.toLowerCase(), 6);
    	FUNCTION_MAP.put(FunctionLibrary.PARSETIME.toLowerCase(), 7);
    	FUNCTION_MAP.put(FunctionLibrary.FORMATDATE.toLowerCase(), 8);
    	FUNCTION_MAP.put(FunctionLibrary.FORMATTIME.toLowerCase(), 9);
    }
    
	private Expression rewriteFunction(Function function) throws TeiidComponentException, TeiidProcessingException{
		//rewrite alias functions
		String functionLowerName = function.getName().toLowerCase();
		String actualName =ALIASED_FUNCTIONS.get(functionLowerName);
		FunctionLibrary funcLibrary = this.metadata.getFunctionLibrary();

		if (actualName != null) {
			function.setName(actualName);
			Expression[] args = function.getArgs();
		    Class<?>[] types = new Class[args.length];
		    for(int i=0; i<args.length; i++) {
		        types[i] = args[i].getType();
		    }
			FunctionDescriptor descriptor = funcLibrary.findFunction(actualName, types);
			function.setFunctionDescriptor(descriptor);
		}
		
		Integer code = FUNCTION_MAP.get(functionLowerName);
		if (code != null) {
			switch (code) {
			case 0: { //space(x) => repeat(' ', x)
				Function result = new Function(SourceSystemFunctions.REPEAT,
						new Expression[] {new Constant(" "), function.getArg(0)}); //$NON-NLS-1$
				//resolve the function
				FunctionDescriptor descriptor = 
					funcLibrary.findFunction(SourceSystemFunctions.REPEAT, new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.INTEGER});
				result.setFunctionDescriptor(descriptor);
				result.setType(DataTypeManager.DefaultDataClasses.STRING);
				function = result;
				break;
			}
			case 1: {//from_unixtime(a) => timestampadd(SQL_TSI_SECOND, a, new Timestamp(0)) 
				Function result = new Function(FunctionLibrary.TIMESTAMPADD,
						new Expression[] {new Constant(NonReserved.SQL_TSI_SECOND), function.getArg(0), new Constant(new Timestamp(0)) });
				//resolve the function
				FunctionDescriptor descriptor = 
					funcLibrary.findFunction(FunctionLibrary.TIMESTAMPADD, new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.INTEGER, DataTypeManager.DefaultDataClasses.TIMESTAMP });
				result.setFunctionDescriptor(descriptor);
				result.setType(DataTypeManager.DefaultDataClasses.TIMESTAMP);
				function = result;
				break;
			}
			case 2: {  //rewrite nullif(a, b) => case when (a = b) then null else a
				List when = Arrays.asList(new Criteria[] {new CompareCriteria(function.getArg(0), CompareCriteria.EQ, function.getArg(1))});
				Constant nullConstant = new Constant(null, function.getType());
				List then = Arrays.asList(new Expression[] {nullConstant});
				SearchedCaseExpression caseExpr = new SearchedCaseExpression(when, then);
				caseExpr.setElseExpression(function.getArg(0));
				caseExpr.setType(function.getType());
				return rewriteExpressionDirect(caseExpr);
			}
			case 3: {
				Expression[] args = function.getArgs();
				if (args.length == 2) {
					Function result = new Function(SourceSystemFunctions.IFNULL,
							new Expression[] {function.getArg(0), function.getArg(1) });
					//resolve the function
					FunctionDescriptor descriptor = 
						funcLibrary.findFunction(SourceSystemFunctions.IFNULL, new Class[] { function.getType(), function.getType()  });
					result.setFunctionDescriptor(descriptor);
					result.setType(function.getType());
					function = result;
				}
				break;
			}
			case 4: { //rewrite concat2 - CONCAT2(a, b) ==> CASE WHEN (a is NULL AND b is NULL) THEN NULL ELSE CONCAT( NVL(a, ''), NVL(b, '') )
				Expression[] args = function.getArgs();
				Function[] newArgs = new Function[args.length];

				for(int i=0; i<args.length; i++) {
					newArgs[i] = new Function(SourceSystemFunctions.IFNULL, new Expression[] {args[i], new Constant("")}); //$NON-NLS-1$
					newArgs[i].setType(args[i].getType());
					Assertion.assertTrue(args[i].getType() == DataTypeManager.DefaultDataClasses.STRING);
			        FunctionDescriptor descriptor = 
			        	funcLibrary.findFunction(SourceSystemFunctions.IFNULL, new Class[] { args[i].getType(), DataTypeManager.DefaultDataClasses.STRING });
			        newArgs[i].setFunctionDescriptor(descriptor);
				}
				
				Function concat = new Function(SourceSystemFunctions.CONCAT, newArgs);
				concat.setType(DataTypeManager.DefaultDataClasses.STRING);
				FunctionDescriptor descriptor = 
					funcLibrary.findFunction(SourceSystemFunctions.CONCAT, new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.STRING });
				concat.setFunctionDescriptor(descriptor);
				
				List when = Arrays.asList(new Criteria[] {new CompoundCriteria(CompoundCriteria.AND, new IsNullCriteria(args[0]), new IsNullCriteria(args[1]))});
				Constant nullConstant = new Constant(null, DataTypeManager.DefaultDataClasses.STRING);
				List then = Arrays.asList(new Expression[] {nullConstant});
				SearchedCaseExpression caseExpr = new SearchedCaseExpression(when, then);
				caseExpr.setElseExpression(concat);
				caseExpr.setType(DataTypeManager.DefaultDataClasses.STRING);
				return rewriteExpressionDirect(caseExpr);
			}
			case 5: {
				if (function.getType() != DataTypeManager.DefaultDataClasses.TIMESTAMP) {
					FunctionDescriptor descriptor = 
						funcLibrary.findFunction(SourceSystemFunctions.TIMESTAMPADD, new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.INTEGER, DataTypeManager.DefaultDataClasses.TIMESTAMP });
					function.setFunctionDescriptor(descriptor);
					Class<?> type = function.getType();
					function.setType(DataTypeManager.DefaultDataClasses.TIMESTAMP);
					function.getArgs()[2] = ResolverUtil.getConversion(function.getArg(2), DataTypeManager.getDataTypeName(type), DataTypeManager.DefaultDataTypes.TIMESTAMP, false, funcLibrary);
					function = ResolverUtil.getConversion(function, DataTypeManager.DefaultDataTypes.TIMESTAMP, DataTypeManager.getDataTypeName(type), false, funcLibrary);
				}
				break;
			}
			case 6:
			case 7: {
				FunctionDescriptor descriptor = 
					funcLibrary.findFunction(SourceSystemFunctions.PARSETIMESTAMP, new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.STRING });
				function.setName(SourceSystemFunctions.PARSETIMESTAMP);
				function.setFunctionDescriptor(descriptor);
				Class<?> type = function.getType();
				function.setType(DataTypeManager.DefaultDataClasses.TIMESTAMP);
				function = ResolverUtil.getConversion(function, DataTypeManager.DefaultDataTypes.TIMESTAMP, DataTypeManager.getDataTypeName(type), false, funcLibrary);
				break;
			}
			case 8:
			case 9: {
				FunctionDescriptor descriptor = 
					funcLibrary.findFunction(SourceSystemFunctions.FORMATTIMESTAMP, new Class[] { DataTypeManager.DefaultDataClasses.TIMESTAMP, DataTypeManager.DefaultDataClasses.STRING });
				function.setName(SourceSystemFunctions.FORMATTIMESTAMP);
				function.setFunctionDescriptor(descriptor);
				function.getArgs()[0] = ResolverUtil.getConversion(function.getArg(0), DataTypeManager.getDataTypeName(function.getArg(0).getType()), DataTypeManager.DefaultDataTypes.TIMESTAMP, false, funcLibrary);
				break;
			}
			}
		}
						
		Expression[] args = function.getArgs();
		Expression[] newArgs = new Expression[args.length];
		        
        // Rewrite args
		for(int i=0; i<args.length; i++) {
			newArgs[i] = rewriteExpressionDirect(args[i]);
            if (isNull(newArgs[i]) && !function.getFunctionDescriptor().isNullDependent()) {
                return new Constant(null, function.getType());
            }
        }
        function.setArgs(newArgs);

        if( FunctionLibrary.isConvert(function) &&
            newArgs[1] instanceof Constant) {
            
            Class srcType = newArgs[0].getType();
            String tgtTypeName = (String) ((Constant)newArgs[1]).getValue();
            Class tgtType = DataTypeManager.getDataTypeClass(tgtTypeName);

            if(srcType != null && tgtType != null && srcType.equals(tgtType)) {
                return newArgs[0];
            }

        }

        //convert DECODESTRING function to CASE expression
        if( function.getName().equalsIgnoreCase(FunctionLibrary.DECODESTRING) 
                || function.getName().equalsIgnoreCase(FunctionLibrary.DECODEINTEGER)) { 
            return convertDecodeFunction(function);
        }
        
        return function;
	}

	private Expression convertDecodeFunction(Function function){
    	Expression exprs[] = function.getArgs();
    	String decodeString = (String)((Constant)exprs[1]).getValue();
    	String decodeDelimiter = ","; //$NON-NLS-1$
    	if(exprs.length == 3){
    		decodeDelimiter = (String)((Constant)exprs[2]).getValue();
    	}
    	List<Criteria> newWhens = new ArrayList<Criteria>();
    	List<Constant> newThens = new ArrayList<Constant>();
        Constant elseConst = null;
        StringTokenizer tokenizer = new StringTokenizer(decodeString, decodeDelimiter);
        while (tokenizer.hasMoreTokens()) {
            String resultString;
            String compareString =
            	convertString(tokenizer.nextToken().trim());
            if (tokenizer.hasMoreTokens()) {
                resultString = convertString(tokenizer.nextToken().trim());
                Criteria crit;
                if (compareString == null) {
                	crit = new IsNullCriteria((Expression) exprs[0].clone());
                } else {
                	crit = new CompareCriteria((Expression) exprs[0].clone(), CompareCriteria.EQ, new Constant(compareString));
                }
                newWhens.add(crit);
                newThens.add(new Constant(resultString));
            }else {
                elseConst = new Constant(compareString);
            }
        }
        SearchedCaseExpression newCaseExpr = new SearchedCaseExpression(newWhens, newThens);
        if(elseConst != null) {
            newCaseExpr.setElseExpression(elseConst);
        }else {
            newCaseExpr.setElseExpression(exprs[0]);
        }
        
        newCaseExpr.setType(function.getType());
        return newCaseExpr;
	}
	
    private static String convertString(String string) {
        /*
         * if there are no characters in the compare string we designate that as
         * an indication of null.  ie if the decode string looks like this:
         *
         * "'this', 1,,'null'"
         *
         * Then if the value in the first argument is null then the String 'null' is
         * returned from the function.
         */
        if (string.equals("")) { //$NON-NLS-1$
            return null;
        }

        /*
         * we also allow the use of the keyword null in the decode string.  if it
         * wished to match on the string 'null' then the string must be qualified by
         * ' designators.
         */
         if(string.equalsIgnoreCase("null")){ //$NON-NLS-1$
            return null;
         }

        /*
         * Here we check to see if the String in the decode String submitted
         * was surrounded by String literal characters. In this case we strip
         * these literal characters from the String.
         */
        if ((string.startsWith("\"") && string.endsWith("\"")) //$NON-NLS-1$ //$NON-NLS-2$
            || (string.startsWith("'") && string.endsWith("'"))) { //$NON-NLS-1$ //$NON-NLS-2$
            if (string.length() == 2) {
                /*
                 * This is an indication that the desired string to be compared is
                 * the "" empty string, so we return it as such.
                 */
                string = ""; //$NON-NLS-1$
            } else if (!string.equalsIgnoreCase("'") && !string.equalsIgnoreCase("\"")){ //$NON-NLS-1$ //$NON-NLS-2$
                string = string.substring(1);
                string = string.substring(0, string.length()-1);
            }
        }

        return string;
    }
	
    private Expression rewriteCaseExpression(CaseExpression expr)
        throws TeiidComponentException, TeiidProcessingException{
    	List<CompareCriteria> whens = new ArrayList<CompareCriteria>(expr.getWhenCount());
    	for (Expression expression: (List<Expression>)expr.getWhen()) {
    		whens.add(new CompareCriteria((Expression)expr.getExpression().clone(), CompareCriteria.EQ, expression));
    	}
    	SearchedCaseExpression sce = new SearchedCaseExpression(whens, expr.getThen());
    	sce.setElseExpression(expr.getElseExpression());
    	sce.setType(expr.getType());
    	return rewriteCaseExpression(sce);
    }

    private Expression rewriteCaseExpression(SearchedCaseExpression expr)
        throws TeiidComponentException, TeiidProcessingException{
        int whenCount = expr.getWhenCount();
        ArrayList<Criteria> whens = new ArrayList<Criteria>(whenCount);
        ArrayList<Expression> thens = new ArrayList<Expression>(whenCount);

        for (int i = 0; i < whenCount; i++) {
            
            // Check the when to see if this CASE can be rewritten due to an always true/false when
            Criteria rewrittenWhen = rewriteCriteria(expr.getWhenCriteria(i));
            if(rewrittenWhen == TRUE_CRITERIA) {
                // WHEN is always true, so just return the THEN
                return rewriteExpressionDirect(expr.getThenExpression(i));
            }
            if (rewrittenWhen == FALSE_CRITERIA || rewrittenWhen == UNKNOWN_CRITERIA) {
            	continue;
            }
            
            whens.add(rewrittenWhen);
            thens.add(rewriteExpressionDirect(expr.getThenExpression(i)));
        }

        if (expr.getElseExpression() != null) {
        	expr.setElseExpression(rewriteExpressionDirect(expr.getElseExpression()));
        }
        
        Expression elseExpr = expr.getElseExpression();
        if(whens.size() == 0) {
            // No WHENs left, so just return the ELSE
            if(elseExpr == null) {
                // No else, no valid whens, just return null constant typed same as CASE
                return new Constant(null, expr.getType());
            } 

            // Rewrite the else and return
            return elseExpr;
        }
        
        expr.setWhen(whens, thens);
        
        /* optimization for case 5413: 
         *   If all of the remaining 'thens' and the 'else' evaluate to the same value, 
         *     just return the 'else' expression.
         */
        
        if ( elseExpr != null ) {
            boolean bAllMatch = true;

            for (int i = 0; i < whenCount; i++) {
                if ( !thens.get( i ).equals(elseExpr) ) {
                    bAllMatch = false;
                    break;
                }
            }
            
            if ( bAllMatch ) {
                return elseExpr;
            }
        }
        
        return expr;
    }
        
    private Command rewriteExec(StoredProcedure storedProcedure) throws TeiidComponentException, TeiidProcessingException{
        //After this method, no longer need to display named parameters
        storedProcedure.setDisplayNamedParameters(false);
        
        for (SPParameter param : storedProcedure.getInputParameters()) {
            if (!processing) {
            	param.setExpression(rewriteExpressionDirect(param.getExpression()));
            } else if (!(param.getExpression() instanceof Constant)) {
            	boolean isBindEligible = !isConstantConvert(param.getExpression());
            	param.setExpression(evaluate(param.getExpression(), isBindEligible));
            }
        }
        return storedProcedure;
    }

	private Insert rewriteInsert(Insert insert) throws TeiidComponentException, TeiidProcessingException{
		UpdateInfo info = insert.getUpdateInfo();
		if (info != null && info.isInherentInsert()) {
			//TODO: update error messages
			UpdateMapping mapping = info.findInsertUpdateMapping(insert, true);
			if (mapping == null) {
				throw new QueryValidatorException(QueryPlugin.Util.getString("ValidationVisitor.nonUpdatable", insert.getVariables())); //$NON-NLS-1$
			}
			Map<ElementSymbol, ElementSymbol> symbolMap = mapping.getUpdatableViewSymbols();
			List<ElementSymbol> mappedSymbols = new ArrayList<ElementSymbol>(insert.getVariables().size());
			for (ElementSymbol symbol : insert.getVariables()) {
				mappedSymbols.add(symbolMap.get(symbol));
			}
			insert.setVariables(mappedSymbols);
			insert.setGroup(mapping.getGroup().clone());
			insert.setUpdateInfo(ProcedureContainerResolver.getUpdateInfo(insert.getGroup(), metadata, Command.TYPE_INSERT));
			return rewriteInsert(insert);
		}

        if ( insert.getQueryExpression() != null ) {
        	insert.setQueryExpression((QueryCommand)rewriteCommand(insert.getQueryExpression(), true));
        	return correctDatatypes(insert);
        }
        // Evaluate any function / constant trees in the insert values
        List expressions = insert.getValues();
        List evalExpressions = new ArrayList(expressions.size());
        Iterator expIter = expressions.iterator();
        while(expIter.hasNext()) {
            Expression exp = (Expression) expIter.next();
            evalExpressions.add( rewriteExpressionDirect( exp ));
        }

        insert.setValues(evalExpressions);        
		return insert;
	}

    public static Query createInlineViewQuery(GroupSymbol inlineGroup,
                                               Command nested,
                                               QueryMetadataInterface metadata,
                                               List<? extends SingleElementSymbol> actualSymbols) throws QueryMetadataException,
                                                                  QueryResolverException,
                                                                  TeiidComponentException {
        Query query = new Query();
        Select select = new Select();
        query.setSelect(select);
        From from = new From();
        from.addClause(new UnaryFromClause(inlineGroup)); 
        TempMetadataStore store = new TempMetadataStore();
        TempMetadataAdapter tma = new TempMetadataAdapter(metadata, store);
        if (nested instanceof QueryCommand) {
	        Query firstProject = ((QueryCommand)nested).getProjectedQuery(); 
	        makeSelectUnique(firstProject.getSelect(), false);
        }
        TempMetadataID gid = store.addTempGroup(inlineGroup.getName(), nested.getProjectedSymbols());
        inlineGroup.setMetadataID(gid);
        
        List<Class<?>> actualTypes = new ArrayList<Class<?>>(nested.getProjectedSymbols().size());
        for (SingleElementSymbol ses : actualSymbols) {
            actualTypes.add(ses.getType());
        }
        List<SingleElementSymbol> selectSymbols = SetQuery.getTypedProjectedSymbols(ResolverUtil.resolveElementsInGroup(inlineGroup, tma), actualTypes, tma);
        Iterator<? extends SingleElementSymbol> iter = actualSymbols.iterator();
        for (SingleElementSymbol ses : selectSymbols) {
        	ses = (SingleElementSymbol)ses.clone();
        	SingleElementSymbol actual = iter.next();
        	if (!ses.getShortCanonicalName().equals(actual.getShortCanonicalName())) {
	        	if (ses instanceof AliasSymbol) {
	        		((AliasSymbol)ses).setShortName(actual.getShortName());
	        	} else {
	        		ses = new AliasSymbol(actual.getShortName(), ses);
	        	}
        	}
			select.addSymbol(ses);
		}
        query.setFrom(from); 
        QueryResolver.resolveCommand(query, tma);
        query.setOption(nested.getOption());
        from.getClauses().clear();
        SubqueryFromClause sqfc = new SubqueryFromClause(inlineGroup.getName());
        sqfc.setCommand(nested);
        sqfc.getGroupSymbol().setMetadataID(inlineGroup.getMetadataID());
        from.addClause(sqfc);
        //copy the metadata onto the new query so that temp metadata adapters will be used in later calls
        query.getTemporaryMetadata().putAll(store.getData()); 
        return query;
    }    
    
    public static void makeSelectUnique(Select select, boolean expressionSymbolsOnly) {
        
        select.setSymbols(select.getProjectedSymbols());
        
        List symbols = select.getSymbols();
        
        HashSet<String> uniqueNames = new HashSet<String>();
        
        for(int i = 0; i < symbols.size(); i++) {
            
            SingleElementSymbol symbol = (SingleElementSymbol)symbols.get(i);
            
            String baseName = symbol.getShortCanonicalName(); 
            String name = baseName;

            int exprID = 0;
            while (true) {
                if (uniqueNames.add(name)) {
                    break;
                }
                name = baseName + '_' + (exprID++);
            }
            
            if (expressionSymbolsOnly && !(symbol instanceof ExpressionSymbol)) {
                continue;
            }
            
            boolean hasAlias = false;
            // Strip alias if it exists
            if(symbol instanceof AliasSymbol) {
                symbol = ((AliasSymbol)symbol).getSymbol();
                hasAlias = true;
            }
            
            if (((symbol instanceof ExpressionSymbol) && !hasAlias) || !name.equalsIgnoreCase(baseName)) {
                symbols.set(i, new AliasSymbol(name, symbol));
            }
        }
    }

	private Command rewriteUpdate(Update update) throws TeiidComponentException, TeiidProcessingException{
		UpdateInfo info = update.getUpdateInfo();
		if (info != null && info.isInherentUpdate()) {
			if (!info.getUnionBranches().isEmpty()) {
				List<Command> batchedUpdates = new ArrayList<Command>(info.getUnionBranches().size() + 1);
				for (UpdateInfo branchInfo : info.getUnionBranches()) {
					batchedUpdates.add(rewriteInherentUpdate((Update)update.clone(), branchInfo));
				}
				batchedUpdates.add(0, rewriteInherentUpdate(update, info));
				return new BatchedUpdateCommand(batchedUpdates);
			}
			return rewriteInherentUpdate(update, info);
		}
		
		if (commandType == Command.TYPE_UPDATE && variables != null) {
	        SetClauseList newChangeList = new SetClauseList();
	        for (SetClause entry : update.getChangeList().getClauses()) {
	            Expression rightExpr = entry.getValue();
	            boolean retainChange = checkInputVariables(rightExpr);
	            if (retainChange) {
	                newChangeList.addClause(entry.getSymbol(), entry.getValue());
	            }
	        }
	        update.setChangeList(newChangeList);
        }

		// Evaluate any function on the right side of set clauses
        for (SetClause entry : update.getChangeList().getClauses()) {
        	entry.setValue(rewriteExpressionDirect(entry.getValue()));
        }

		// Rewrite criteria
		Criteria crit = update.getCriteria();
		if(crit != null) {
			update.setCriteria(rewriteCriteria(crit));
		}

		return update;
	}

	private Command rewriteInherentUpdate(Update update, UpdateInfo info)
			throws QueryValidatorException, QueryMetadataException,
			TeiidComponentException, QueryResolverException,
			TeiidProcessingException {
		UpdateMapping mapping = info.findUpdateMapping(update.getChangeList().getClauseMap().keySet(), false);
		if (mapping == null) {
			throw new QueryValidatorException(QueryPlugin.Util.getString("ValidationVisitor.nonUpdatable", update.getChangeList().getClauseMap().keySet())); //$NON-NLS-1$
		}
		Map<ElementSymbol, ElementSymbol> symbolMap = mapping.getUpdatableViewSymbols();
		if (info.isSimple()) {
			update.setGroup(mapping.getGroup().clone());
			for (SetClause clause : update.getChangeList().getClauses()) {
				clause.setSymbol(symbolMap.get(clause.getSymbol()));
			}
			//TODO: properly handle correlated references
			DeepPostOrderNavigator.doVisit(update, new ExpressionMappingVisitor(symbolMap, true));
			if (info.getViewDefinition().getCriteria() != null) {
				update.setCriteria(Criteria.combineCriteria(update.getCriteria(), (Criteria)info.getViewDefinition().getCriteria().clone()));
			}
			//resolve
			update.setUpdateInfo(ProcedureContainerResolver.getUpdateInfo(update.getGroup(), metadata, Command.TYPE_UPDATE));
			return rewriteUpdate(update);
		} 
		Query query = (Query)info.getViewDefinition().clone();
		query.setOrderBy(null);
		SymbolMap expressionMapping = SymbolMap.createSymbolMap(update.getGroup(), query.getProjectedSymbols(), metadata);
		
		ArrayList<SingleElementSymbol> selectSymbols = mapChangeList(update, symbolMap);
		query.setSelect(new Select(selectSymbols));
		ExpressionMappingVisitor emv = new ExpressionMappingVisitor(expressionMapping.asMap(), true);
		PostOrderNavigator.doVisit(query.getSelect(), emv);
		
		Criteria crit = update.getCriteria();
		if (crit != null) {
			PostOrderNavigator.doVisit(crit, emv);
			query.setCriteria(Criteria.combineCriteria(query.getCriteria(), crit));
		}
		GroupSymbol group = mapping.getGroup();
		String correlationName = mapping.getCorrelatedName().getName();
		
		return createUpdateProcedure(update, query, group, correlationName);
	}

	private ArrayList<SingleElementSymbol> mapChangeList(Update update,
			Map<ElementSymbol, ElementSymbol> symbolMap) {
		ArrayList<SingleElementSymbol> selectSymbols = new ArrayList<SingleElementSymbol>(update.getChangeList().getClauses().size());
		int i = 0;
		for (SetClause clause : update.getChangeList().getClauses()) {
			Expression ex = clause.getValue();
			SingleElementSymbol selectSymbol = null;
			if (!EvaluatableVisitor.willBecomeConstant(ex)) {
				if (!(ex instanceof SingleElementSymbol)) {
					selectSymbol = new ExpressionSymbol("expr", ex); //$NON-NLS-1$
				} else {
					selectSymbol = (SingleElementSymbol)ex;
				}
				selectSymbols.add(new AliasSymbol("s_" +i, selectSymbol)); //$NON-NLS-1$
				ex = new ElementSymbol("s_" +i); //$NON-NLS-1$
			}
			if (symbolMap != null) {
				clause.setSymbol(symbolMap.get(clause.getSymbol()));
			}
			i++;
		}
		return selectSymbols;
	}

	private Command createUpdateProcedure(Update update, Query query,
			GroupSymbol group, String correlationName)
			throws TeiidComponentException, QueryMetadataException,
			QueryResolverException, TeiidProcessingException {
		Update newUpdate = new Update();
		newUpdate.setChangeList(update.getChangeList());
		newUpdate.setGroup(group.clone());
		List<Criteria> pkCriteria = createPkCriteria(group, correlationName, query);
		newUpdate.setCriteria(new CompoundCriteria(pkCriteria));
		return asLoopProcedure(update.getGroup(), query, newUpdate);
	}

	/**
	 * rewrite as loop on (query) as X begin newupdate; rows_updated = rows_updated + 1;
	 */
	private Command asLoopProcedure(GroupSymbol group, Query query,
			ProcedureContainer newUpdate) throws QueryResolverException,
			TeiidComponentException, TeiidProcessingException {
		Block b = new Block();
		b.addStatement(new CommandStatement(newUpdate));
		CreateUpdateProcedureCommand cupc = new CreateUpdateProcedureCommand();
		Block parent = new Block();
		LoopStatement ls = new LoopStatement(b, query, "X"); //$NON-NLS-1$
		parent.addStatement(ls);
		AssignmentStatement as = new AssignmentStatement();
		ElementSymbol rowsUpdate = new ElementSymbol(ProcedureReservedWords.VARIABLES+ElementSymbol.SEPARATOR+ProcedureReservedWords.ROWS_UPDATED);
		as.setVariable(rowsUpdate);
		as.setExpression(new Function("+", new Expression[] {rowsUpdate, new Constant(1)})); //$NON-NLS-1$
		b.addStatement(as);
		cupc.setBlock(parent);
		cupc.setVirtualGroup(group);
		QueryResolver.resolveCommand(cupc, metadata);
		return rewrite(cupc, metadata, context);
	}

	private List<Criteria> createPkCriteria(GroupSymbol group, String correlationName, Query query) throws TeiidComponentException, QueryMetadataException {
		Object pk = metadata.getPrimaryKey(group.getMetadataID());
		if (pk == null) {
			pk = metadata.getUniqueKeysInGroup(group.getMetadataID()).iterator().next();
		}
		int i = query.getSelect().getSymbols().size();
		List<Object> ids = metadata.getElementIDsInKey(pk);
		List<Criteria> pkCriteria = new ArrayList<Criteria>(ids.size());
		for (Object object : ids) {
			ElementSymbol es = new ElementSymbol(correlationName + ElementSymbol.SEPARATOR + metadata.getName(object));
			query.getSelect().addSymbol(new AliasSymbol("s_" +i, es)); //$NON-NLS-1$
			es = new ElementSymbol(group.getName() + ElementSymbol.SEPARATOR + metadata.getName(object));
			pkCriteria.add(new CompareCriteria(es, CompareCriteria.EQ, new ElementSymbol("X.s_" + i))); //$NON-NLS-1$
			i++;
		}
		return pkCriteria;
	}
	
    /**
     * Checks variables in an expression, if the variables are INPUT variables and if
     * none of them are changing, then this method returns a false, if all of them
     * are changing this returns a true, if some are changing and some are not, then
     * that is an invalid case and the method adds to the list of invalid variables.
     * @throws TeiidComponentException, MetaMatrixProcessingException
     */
    private boolean checkInputVariables(Expression expr) throws TeiidComponentException, TeiidProcessingException{
        Boolean result = null;
        for (ElementSymbol var : ElementCollectorVisitor.getElements(expr, false)) {
            if (var.isExternalReference() && var.getGroupSymbol().getSchema() == null && var.getGroupSymbol().getShortName().equalsIgnoreCase(ProcedureReservedWords.INPUTS)) {
                
                var = var.clone();
                var.getGroupSymbol().setShortName(ProcedureReservedWords.CHANGING);
                
                Boolean changingValue = (Boolean)((Constant)variables.get(var)).getValue();
                
                if (result == null) {
                    result = changingValue;
                } else if (!result.equals(changingValue)) {
                	throw new QueryValidatorException(QueryPlugin.Util.getString("VariableSubstitutionVisitor.Input_vars_should_have_same_changing_state", expr)); //$NON-NLS-1$
                }
            }
        }
        
        if (result != null) {
            return result.booleanValue();
        }
        
        return true;
    }

	private Command rewriteDelete(Delete delete) throws TeiidComponentException, TeiidProcessingException{
		UpdateInfo info = delete.getUpdateInfo();
		if (info != null && info.isInherentDelete()) {
			if (!info.getUnionBranches().isEmpty()) {
				List<Command> batchedUpdates = new ArrayList<Command>(info.getUnionBranches().size() + 1);
				for (UpdateInfo branchInfo : info.getUnionBranches()) {
					batchedUpdates.add(rewriteInherentDelete((Delete)delete.clone(), branchInfo));
				}
				batchedUpdates.add(0, rewriteInherentDelete(delete, info));
				return new BatchedUpdateCommand(batchedUpdates);
			}
			return rewriteInherentDelete(delete, info);
		}
		// Rewrite criteria
		Criteria crit = delete.getCriteria();
		if(crit != null) {
			delete.setCriteria(rewriteCriteria(crit));
		}

		return delete;
	}

	private Command rewriteInherentDelete(Delete delete, UpdateInfo info)
			throws QueryMetadataException, TeiidComponentException,
			QueryResolverException, TeiidProcessingException {
		UpdateMapping mapping = info.getDeleteTarget();
		if (info.isSimple()) {
			delete.setGroup(mapping.getGroup().clone());
			//TODO: properly handle correlated references
			DeepPostOrderNavigator.doVisit(delete, new ExpressionMappingVisitor(mapping.getUpdatableViewSymbols(), true));
			delete.setUpdateInfo(ProcedureContainerResolver.getUpdateInfo(delete.getGroup(), metadata, Command.TYPE_DELETE));
			if (info.getViewDefinition().getCriteria() != null) {
				delete.setCriteria(Criteria.combineCriteria(delete.getCriteria(), (Criteria)info.getViewDefinition().getCriteria().clone()));
			}
			return rewriteDelete(delete);
		}
		
		Query query = (Query)info.getViewDefinition().clone();
		query.setOrderBy(null);
		SymbolMap expressionMapping = SymbolMap.createSymbolMap(delete.getGroup(), query.getProjectedSymbols(), metadata);
		
		query.setSelect(new Select());
		ExpressionMappingVisitor emv = new ExpressionMappingVisitor(expressionMapping.asMap(), true);
		
		Criteria crit = delete.getCriteria();
		if (crit != null) {
			PostOrderNavigator.doVisit(crit, emv);
			query.setCriteria(Criteria.combineCriteria(query.getCriteria(), crit));
		}
		GroupSymbol group = mapping.getGroup();
		String correlationName = mapping.getCorrelatedName().getName();
		return createDeleteProcedure(delete, query, group, correlationName);
	}
	
	public static Command createDeleteProcedure(Delete delete, QueryMetadataInterface metadata, CommandContext context) throws QueryResolverException, QueryMetadataException, TeiidComponentException, TeiidProcessingException {
		QueryRewriter rewriter = new QueryRewriter(metadata, context, null);
		Criteria crit = delete.getCriteria();
		Query query = new Query(new Select(), new From(Arrays.asList(new UnaryFromClause(delete.getGroup()))), crit, null, null);
		return rewriter.createDeleteProcedure(delete, query, delete.getGroup(), delete.getGroup().getName());
	}
	
	public static Command createUpdateProcedure(Update update, QueryMetadataInterface metadata, CommandContext context) throws QueryResolverException, QueryMetadataException, TeiidComponentException, TeiidProcessingException {
		QueryRewriter rewriter = new QueryRewriter(metadata, context, null);
		Criteria crit = update.getCriteria();
		ArrayList<SingleElementSymbol> selectSymbols = rewriter.mapChangeList(update, null);
		Query query = new Query(new Select(selectSymbols), new From(Arrays.asList(new UnaryFromClause(update.getGroup()))), crit, null, null);
		return rewriter.createUpdateProcedure(update, query, update.getGroup(), update.getGroup().getName());
	}

	private Command createDeleteProcedure(Delete delete, Query query,
			GroupSymbol group, String correlationName)
			throws TeiidComponentException, QueryMetadataException,
			QueryResolverException, TeiidProcessingException {
		Delete newUpdate = new Delete();
		newUpdate.setGroup(group.clone());
		List<Criteria> pkCriteria = createPkCriteria(group, correlationName, query);
		newUpdate.setCriteria(new CompoundCriteria(pkCriteria));
		return asLoopProcedure(delete.getGroup(), query, newUpdate);
	}
    
    private Limit rewriteLimitClause(Limit limit) throws TeiidComponentException, TeiidProcessingException{
        if (limit.getOffset() != null) {
            limit.setOffset(rewriteExpressionDirect(limit.getOffset()));
            if (new Constant(0).equals(limit.getOffset())) {
            	limit.setOffset(null);
            }
        }
        if (limit.getRowLimit() != null) {
            limit.setRowLimit(rewriteExpressionDirect(limit.getRowLimit()));
        }
        return limit;
    }
}