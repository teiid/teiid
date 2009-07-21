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

package com.metamatrix.query.rewriter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.teiid.connector.api.SourceSystemFunctions;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.CriteriaEvaluationException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.api.exception.query.FunctionExecutionException;
import com.metamatrix.api.exception.query.InvalidFunctionException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.api.exception.query.QueryValidatorException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.util.TimestampWithTimezone;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.function.FunctionDescriptor;
import com.metamatrix.query.function.FunctionLibrary;
import com.metamatrix.query.function.FunctionLibraryManager;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.SupportConstants;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.metadata.TempMetadataStore;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.resolver.util.ResolverVisitor;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.ReservedWords;
import com.metamatrix.query.sql.lang.AbstractSetCriteria;
import com.metamatrix.query.sql.lang.BetweenCriteria;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.CommandContainer;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.CompoundCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.Delete;
import com.metamatrix.query.sql.lang.DependentSetCriteria;
import com.metamatrix.query.sql.lang.ExistsCriteria;
import com.metamatrix.query.sql.lang.From;
import com.metamatrix.query.sql.lang.FromClause;
import com.metamatrix.query.sql.lang.GroupBy;
import com.metamatrix.query.sql.lang.Insert;
import com.metamatrix.query.sql.lang.Into;
import com.metamatrix.query.sql.lang.IsNullCriteria;
import com.metamatrix.query.sql.lang.JoinPredicate;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.lang.Limit;
import com.metamatrix.query.sql.lang.MatchCriteria;
import com.metamatrix.query.sql.lang.NotCriteria;
import com.metamatrix.query.sql.lang.Option;
import com.metamatrix.query.sql.lang.OrderBy;
import com.metamatrix.query.sql.lang.ProcedureContainer;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.QueryCommand;
import com.metamatrix.query.sql.lang.SPParameter;
import com.metamatrix.query.sql.lang.Select;
import com.metamatrix.query.sql.lang.SetClause;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.lang.SubqueryCompareCriteria;
import com.metamatrix.query.sql.lang.SubqueryContainer;
import com.metamatrix.query.sql.lang.SubqueryFromClause;
import com.metamatrix.query.sql.lang.SubquerySetCriteria;
import com.metamatrix.query.sql.lang.TranslatableProcedureContainer;
import com.metamatrix.query.sql.lang.UnaryFromClause;
import com.metamatrix.query.sql.lang.Update;
import com.metamatrix.query.sql.navigator.PostOrderNavigator;
import com.metamatrix.query.sql.navigator.PreOrderNavigator;
import com.metamatrix.query.sql.proc.AssignmentStatement;
import com.metamatrix.query.sql.proc.Block;
import com.metamatrix.query.sql.proc.CommandStatement;
import com.metamatrix.query.sql.proc.CreateUpdateProcedureCommand;
import com.metamatrix.query.sql.proc.CriteriaSelector;
import com.metamatrix.query.sql.proc.HasCriteria;
import com.metamatrix.query.sql.proc.IfStatement;
import com.metamatrix.query.sql.proc.LoopStatement;
import com.metamatrix.query.sql.proc.Statement;
import com.metamatrix.query.sql.proc.TranslateCriteria;
import com.metamatrix.query.sql.proc.WhileStatement;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.AliasSymbol;
import com.metamatrix.query.sql.symbol.CaseExpression;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.ExpressionSymbol;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.ScalarSubquery;
import com.metamatrix.query.sql.symbol.SearchedCaseExpression;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.util.SymbolMap;
import com.metamatrix.query.sql.visitor.AggregateSymbolCollectorVisitor;
import com.metamatrix.query.sql.visitor.CorrelatedVariableSubstitutionVisitor;
import com.metamatrix.query.sql.visitor.CriteriaTranslatorVisitor;
import com.metamatrix.query.sql.visitor.ElementCollectorVisitor;
import com.metamatrix.query.sql.visitor.EvaluateExpressionVisitor;
import com.metamatrix.query.sql.visitor.ExpressionMappingVisitor;
import com.metamatrix.query.sql.visitor.PredicateCollectorVisitor;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * Rewrites commands and command fragments to a form that is better for planning and execution.  There is a current limitation that
 * command objects themselves cannot change type, since the same object is always used. 
 */
public class QueryRewriter {

    public static final CompareCriteria TRUE_CRITERIA = new CompareCriteria(new Constant(new Integer(1), DataTypeManager.DefaultDataClasses.INTEGER), CompareCriteria.EQ, new Constant(new Integer(1), DataTypeManager.DefaultDataClasses.INTEGER));
    public static final CompareCriteria FALSE_CRITERIA = new CompareCriteria(new Constant(new Integer(1), DataTypeManager.DefaultDataClasses.INTEGER), CompareCriteria.EQ, new Constant(new Integer(0), DataTypeManager.DefaultDataClasses.INTEGER));
    public static final CompareCriteria UNKNOWN_CRITERIA = new CompareCriteria(new Constant(null, DataTypeManager.DefaultDataClasses.STRING), CompareCriteria.NE, new Constant(null, DataTypeManager.DefaultDataClasses.STRING));
    
    private final static Timestamp EXAMPLE_TIMESTAMP = Timestamp.valueOf("2001-02-03 13:04:05.01"); //$NON-NLS-1$
    private final static Time EXAMPLE_TIME = Time.valueOf("13:04:05"); //$NON-NLS-1$
    private final static Date EXAMPLE_DATE = Date.valueOf("2001-02-03"); //$NON-NLS-1$
    
    private static final Map<String, String> ALIASED_FUNCTIONS = new HashMap<String, String>();
    
    static {
    	ALIASED_FUNCTIONS.put("lower", SourceSystemFunctions.LCASE); //$NON-NLS-1$
    	ALIASED_FUNCTIONS.put("upper", SourceSystemFunctions.UCASE); //$NON-NLS-1$
    	ALIASED_FUNCTIONS.put("cast", SourceSystemFunctions.CONVERT); //$NON-NLS-1$
    	ALIASED_FUNCTIONS.put("nvl", SourceSystemFunctions.IFNULL); //$NON-NLS-1$
    	ALIASED_FUNCTIONS.put("||", SourceSystemFunctions.CONCAT); //$NON-NLS-1$
    	ALIASED_FUNCTIONS.put("chr", SourceSystemFunctions.CHAR); //$NON-NLS-1$
    }

	private QueryRewriter() { }

    public static Command rewrite(Command command, Command procCommand, QueryMetadataInterface metadata, CommandContext context) throws QueryValidatorException {
        return rewriteCommand(command, procCommand, metadata, context, false);
    }

    /**
     * Rewrites the command and all of its subcommands (both embedded and non-embedded)
     *  
     * @param command
     * @param procCommand
     * @param metadata
     * @param context
     * @param removeOrderBy
     * @return
     * @throws QueryValidatorException
     */
	private static Command rewriteCommand(Command command, Command procCommand, final QueryMetadataInterface metadata, final CommandContext context, boolean removeOrderBy) throws QueryValidatorException {

        //TODO: this should be merged with the normal functioning of the rewriter
        CorrelatedVariableSubstitutionVisitor.substituteVariables(command);
        
        Map tempMetadata = command.getTemporaryMetadata();
        QueryMetadataInterface rewriteMetadata = metadata;
        if(tempMetadata != null) {
        	rewriteMetadata = new TempMetadataAdapter(metadata, new TempMetadataStore(tempMetadata));
        }
        
        switch(command.getType()) {
			case Command.TYPE_QUERY:
                if(command instanceof Query) {
                    command = rewriteQuery((Query) command, procCommand, rewriteMetadata, context);
                }else {
                    command = rewriteSetQuery((SetQuery) command, procCommand, rewriteMetadata, context);
                }
            	if (removeOrderBy) {
                	QueryCommand queryCommand = (QueryCommand)command;
                	if (queryCommand.getLimit() == null) {
                		queryCommand.setOrderBy(null);
                	}
                }
                break;
            case Command.TYPE_STORED_PROCEDURE:
                command = rewriteExec((StoredProcedure) command, procCommand, rewriteMetadata, context);
                break;
    		case Command.TYPE_INSERT:
                command = rewriteInsert((Insert) command, procCommand, context, rewriteMetadata);
                break;
			case Command.TYPE_UPDATE:
                command = rewriteUpdate((Update) command, procCommand, context, rewriteMetadata);
                break;
			case Command.TYPE_DELETE:
                command = rewriteDelete((Delete) command, procCommand, context, rewriteMetadata);
                break;
            case Command.TYPE_UPDATE_PROCEDURE:
                procCommand = command;
                command = rewriteUpdateProcedure((CreateUpdateProcedureCommand) command, rewriteMetadata, context);
                break;
		}
        
        //recursively rewrite simple containers - after the container itself was rewritten
        if (command instanceof CommandContainer) {
            List subCommands = ((CommandContainer)command).getContainedCommands();
            for (int i = 0; i < subCommands.size(); i++) {
                Command subCommand = (Command)subCommands.get(i);
                
                if (command instanceof ProcedureContainer) {
                       
                    try {
    	                Map variables = QueryResolver.getVariableValues(command, metadata);                        
                        VariableSubstitutionVisitor.substituteVariables(subCommand, variables, command.getType());

                    } catch (QueryMetadataException err) {
                        throw new QueryValidatorException(err, err.getMessage());
                    } catch (QueryResolverException err) {
                        throw new QueryValidatorException(err, err.getMessage());
                    } catch (MetaMatrixComponentException err) {
                        throw new QueryValidatorException(err, err.getMessage());
                    }
                }
                
                subCommand = rewriteCommand(subCommand, procCommand, metadata, context, false);
                subCommands.set(i, subCommand);
            }
        }

        return removeProceduralWrapper(command, metadata);
	}
    
    private static Option mergeOptions( Option sourceOption, Option targetOption ) {
        if ( sourceOption == null ) {
            return targetOption;
        }
        if ( sourceOption.getPlanOnly() == true ) {
            targetOption.setPlanOnly( true );
        }
        if ( sourceOption.getDebug() == true ) {
            targetOption.setDebug( true );
        }
        if ( sourceOption.getShowPlan() == true ) {
            targetOption.setShowPlan( true );
        }
        
        return targetOption;
    }
    
	
    private static Command removeProceduralWrapper(Command command, QueryMetadataInterface metadata) throws QueryValidatorException {
        
        if (!(command instanceof StoredProcedure)) {
            return command;
        }
        
        StoredProcedure container = (StoredProcedure)command;
        if (container.isProcedureRelational()) {
            return command;
        }
        
        if (!(container.getSubCommand() instanceof CreateUpdateProcedureCommand)) {
            return command;
        }
        
        CreateUpdateProcedureCommand subCommand = (CreateUpdateProcedureCommand)container.getSubCommand();
        
        if (subCommand == null) {
            return command;
        }
        
        //if all parameters can be evaluated, we need to validate their values before removing the procedure wrapper
        for (Iterator iter = container.getInputParameters().iterator(); iter.hasNext();) {
            SPParameter param = (SPParameter)iter.next();
            Expression expr = param.getExpression();
            if (!EvaluateExpressionVisitor.isFullyEvaluatable(expr, true)) {
                return command;
            }
            try {
                Object value = Evaluator.evaluate(expr);

                //check contraint
                if (value == null && !metadata.elementSupports(param.getMetadataID(), SupportConstants.Element.NULL)) {
                    throw new QueryValidatorException(QueryExecPlugin.Util.getString("ProcedurePlan.nonNullableParam", expr)); //$NON-NLS-1$
                }
            } catch (ExpressionEvaluationException err) {
            } catch (BlockedException err) {
            } catch (MetaMatrixComponentException err) {            
            }
        } 
        
        Block block = subCommand.getBlock();
        
        if (block.getStatements().size() != 1) {
            return command;
        }
        Statement statement = (Statement)block.getStatements().get(0);
        if (statement.getType() != Statement.TYPE_COMMAND) {
            return command;
        }
        
        Command child = (((CommandStatement)statement).getCommand());
        
        if (child != null && child.getType() != Command.TYPE_DYNAMIC) {
            if ( child.getOption() == null ) {
                child.setOption( command.getOption() );                
            } else {
                Option merged = mergeOptions( command.getOption(), child.getOption() );
                child.setOption(merged);
            }
            
            return child;        
        }
        return command;
    }

	private static Command rewriteUpdateProcedure(CreateUpdateProcedureCommand procCommand, QueryMetadataInterface metadata, CommandContext context)
								 throws QueryValidatorException {
		
		Block block = rewriteBlock(procCommand.getBlock(), procCommand, context, metadata);
        procCommand.setBlock(block);
        
        return procCommand;
	}

	private static Block rewriteBlock(Block block, Command procCommand, CommandContext context, QueryMetadataInterface metadata)
								 throws QueryValidatorException {
		List statements = block.getStatements();
        Iterator stmtIter = statements.iterator();

		List newStmts = new ArrayList(statements.size());
		// plan each statement in the block
        while(stmtIter.hasNext()) {
			Statement stmnt = (Statement) stmtIter.next();
			Object newStmt = rewriteStatement(stmnt, procCommand, context, metadata);
			if(newStmt instanceof Statement) {
				newStmts.add(newStmt);
			} else if (newStmt instanceof List) {
			    newStmts.addAll((List)newStmt);
            }
        }

        block.setStatements(newStmts);

        return block;
	 }

	private static Object rewriteStatement(Statement statement, Command procCommand, CommandContext context, QueryMetadataInterface metadata)
								 throws QueryValidatorException {

        // evaluate the HAS Criteria on the procedure and rewrite
		int stmtType = statement.getType();
		switch(stmtType) {
			case Statement.TYPE_IF:
				IfStatement ifStmt = (IfStatement) statement;
				Criteria ifCrit = ifStmt.getCondition();
				Criteria evalCrit = rewriteCriteria(ifCrit, procCommand, context, metadata);
                evalCrit = evaluateCriteria(evalCrit);
                
				ifStmt.setCondition(evalCrit);
				if(evalCrit.equals(TRUE_CRITERIA)) {
					Block ifblock = rewriteBlock(ifStmt.getIfBlock(), procCommand, context, metadata);
					return ifblock.getStatements();
				} else if(evalCrit.equals(FALSE_CRITERIA) || evalCrit.equals(UNKNOWN_CRITERIA)) {
					if(ifStmt.hasElseBlock()) {
						Block elseBlock = rewriteBlock(ifStmt.getElseBlock(), procCommand, context, metadata);
						return elseBlock.getStatements();
					} 
                    return null;
				} else {
					Block ifblock = rewriteBlock(ifStmt.getIfBlock(), procCommand, context, metadata);
					ifStmt.setIfBlock(ifblock);
					if(ifStmt.hasElseBlock()) {
						Block elseBlock = rewriteBlock(ifStmt.getElseBlock(), procCommand, context, metadata);
						ifStmt.setElseBlock(elseBlock);
					}
				}
				return ifStmt;
            case Statement.TYPE_ERROR: //treat error the same as expressions
            case Statement.TYPE_DECLARE:
            case Statement.TYPE_ASSIGNMENT:
				AssignmentStatement assStmt = (AssignmentStatement) statement;
				// replave variables to references, these references are later
				// replaced in the processor with variable values
                if (assStmt.hasExpression()) {
    				Expression expr = assStmt.getExpression();
    				expr = rewriteExpression(expr, procCommand, context, metadata);
                    assStmt.setExpression(expr);
                } else if (assStmt.hasCommand()) {
                    rewriteSubqueryContainer(assStmt, procCommand, context, metadata, false);
                    
                    if(assStmt.getCommand().getType() == Command.TYPE_UPDATE) {
                        Update update = (Update)assStmt.getCommand();
                        if (update.getChangeList().isEmpty()) {
                            assStmt.setExpression(new Constant(INTEGER_ZERO));
                        }
                    }
                }
				return assStmt;
			case Statement.TYPE_COMMAND:
				CommandStatement cmdStmt = (CommandStatement) statement;
                rewriteSubqueryContainer(cmdStmt, procCommand, context, metadata, false);
                
				if(cmdStmt.getCommand().getType() == Command.TYPE_UPDATE) {
                    Update update = (Update)cmdStmt.getCommand();
                    if (update.getChangeList().isEmpty()) {
                        return null;
                    }
				}
                return statement;
            case Statement.TYPE_LOOP: 
                LoopStatement loop = (LoopStatement)statement; 
                
                rewriteSubqueryContainer(loop, procCommand, context, metadata, false);
                
                rewriteBlock(loop.getBlock(), procCommand, context, metadata);
                
                if (loop.getBlock().getStatements().isEmpty()) {
                    return null;
                }
                
                return loop;
            case Statement.TYPE_WHILE:
                WhileStatement whileStatement = (WhileStatement) statement;
                Criteria crit = whileStatement.getCondition();
                evalCrit = evaluateCriteria(crit);
                
                whileStatement.setCondition(evalCrit);
                if(evalCrit.equals(TRUE_CRITERIA)) {
                    throw new QueryValidatorException(QueryExecPlugin.Util.getString("QueryRewriter.infinite_while")); //$NON-NLS-1$
                } else if(evalCrit.equals(FALSE_CRITERIA) || evalCrit.equals(UNKNOWN_CRITERIA)) {
                    return null;
                } 
                whileStatement.setBlock(rewriteBlock(whileStatement.getBlock(), procCommand, context, metadata));
                
                if (whileStatement.getBlock().getStatements().isEmpty()) {
                    return null;
                }
                
                return whileStatement;
			default:
				return statement;
		}
	}
    
    /** 
     * @param procCommand
     * @param context
     * @param metadata
     * @param removeOrderBy
     * @param assStmt
     * @throws QueryValidatorException
     */
    private static void rewriteSubqueryContainer(SubqueryContainer container, Command procCommand,
                                                 CommandContext context, QueryMetadataInterface metadata, boolean removeOrderBy) throws QueryValidatorException {
        if (container.getCommand() != null && container.getCommand().getProcessorPlan() == null && metadata != null) {
        	container.setCommand(rewriteCommand(container.getCommand(), procCommand, metadata, context, removeOrderBy));
        }
    }
    
	/**
	 * <p>The HasCriteria evaluates to a TRUE_CRITERIA or a FALSE_CRITERIA, it checks to see
	 * if type of criteria on the elements specified by the CriteriaSelector is specified on
	 * the user's command.</p>
	 */
	private static Criteria rewriteCriteria(HasCriteria hasCrit, Command procCommand, CommandContext context, QueryMetadataInterface metadata) {
		Criteria userCrit = null;
		Command userCommand = ((CreateUpdateProcedureCommand)procCommand).getUserCommand();
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
			Collection userElmnts = ElementCollectorVisitor.getElements(userCrit, true);
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
    	Iterator criteriaIter = PredicateCollectorVisitor.getPredicates(userCrit).iterator();
    	while(criteriaIter.hasNext()) {
    		Criteria predicateCriteria = (Criteria) criteriaIter.next();
    		// atleast one of the hasElemnets should be on this predicate else
    		// proceed to the next predicate
			Collection predElmnts = ElementCollectorVisitor.getElements(predicateCriteria, true);
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
	private static Criteria rewriteCriteria(TranslateCriteria transCrit, Command command, CommandContext context, QueryMetadataInterface metadata)
			 throws QueryValidatorException {

		// criteria translated
		Criteria translatedCriteria = null;

		// command received is the procedure
		CreateUpdateProcedureCommand procCommand = (CreateUpdateProcedureCommand) command;
		// get the user's command from the procedure
		Command userCmd = procCommand.getUserCommand();

		if (!(userCmd instanceof TranslatableProcedureContainer)) {
			return FALSE_CRITERIA;
		}

		Criteria userCriteria = ((TranslatableProcedureContainer)userCmd).getCriteria();

		if(userCriteria == null) {
			return FALSE_CRITERIA;
		}

		// get the symbolmap between virtual elements and theie counterpart expressions
		// from the virtual group's query transform
		CriteriaTranslatorVisitor translateVisitor = new CriteriaTranslatorVisitor(procCommand.getSymbolMap());

		// check if there is a CriteriaSelector specified to restrict
		// parts of user's criteria to be translated
		// get the CriteriaSelector, elements on the selector and the selector type
		CriteriaSelector selector = transCrit.getSelector();
		HasCriteria hasCrit = new HasCriteria(selector);

		// base on the selector evaluate Has criteria, if false
		// return a false criteria
		Criteria result = rewriteCriteria(hasCrit, procCommand, context, metadata);

		if(result.equals(FALSE_CRITERIA)) {
			return FALSE_CRITERIA;
		}
		translateVisitor.setCriteriaSelector(selector);
		if(transCrit.hasTranslations()) {
			translateVisitor.setTranslations(transCrit.getTranslations());
		}

		// create a clone of user's criteria that is then translated
		Criteria userClone = (Criteria) userCriteria.clone();

		// CriteriaTranslatorVisitor visits the user's criteria
        PreOrderNavigator.doVisit(userClone, translateVisitor);

		// translated criteria
		translatedCriteria = translateVisitor.getTranslatedCriteria();
		((TranslatableProcedureContainer)userCmd).addImplicitParameters(translateVisitor.getImplicitParams());
		
		translatedCriteria = rewriteCriteria(translatedCriteria, null, context, metadata);

		// apply any implicit conversions
		try {
            ResolverVisitor.resolveLanguageObject(translatedCriteria, metadata);
		} catch(Exception ex) {
            throw new QueryValidatorException(ex, ErrorMessageKeys.REWRITER_0002, QueryExecPlugin.Util.getString(ErrorMessageKeys.REWRITER_0002, translatedCriteria));
		}

		return translatedCriteria;
	}

	private static Query rewriteQuery(Query query, final Command procCommand, final QueryMetadataInterface metadata, final CommandContext context)
             throws QueryValidatorException {
        
        // Rewrite from clause
        From from = query.getFrom();
        if(from != null){
            List clauses = new ArrayList(from.getClauses().size());
            Iterator clauseIter = from.getClauses().iterator();
            while(clauseIter.hasNext()) {
                clauses.add( rewriteFromClause(query, (FromClause) clauseIter.next(), procCommand, metadata, context) );
            }
            from.setClauses(clauses);
        } else {
            query.setOrderBy(null);
        }

        // Rewrite criteria
        Criteria crit = query.getCriteria();
        if(crit != null && !query.getIsXML()) {
            crit = rewriteCriteria(crit, procCommand, context, metadata);
            if(crit == TRUE_CRITERIA) {
                query.setCriteria(null);
            } else {
                query.setCriteria(crit);
            } 
        }

        if (query.getGroupBy() != null) {
            // we check for group by expressions here to create an ANSI SQL plan
            boolean hasExpression = false;
            for (final Iterator iterator = query.getGroupBy().getSymbols().iterator(); !hasExpression && iterator.hasNext();) {
                hasExpression = iterator.next() instanceof ExpressionSymbol;
            } 
            if (hasExpression) {
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
                } catch (QueryMetadataException err) {
                    throw new QueryValidatorException(err, err.getMessage());
                } catch (QueryResolverException err) {
                    throw new QueryValidatorException(err, err.getMessage());
                } catch (MetaMatrixComponentException err) {
                    throw new QueryValidatorException(err, err.getMessage());
                }
                Iterator iter = outerQuery.getSelect().getProjectedSymbols().iterator();
                HashMap<Expression, SingleElementSymbol> expressionMap = new HashMap<Expression, SingleElementSymbol>();
                for (SingleElementSymbol symbol : (List<SingleElementSymbol>)query.getSelect().getProjectedSymbols()) {
                    expressionMap.put((Expression)SymbolMap.getExpression(symbol).clone(), (SingleElementSymbol)iter.next());
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
                rewriteExpressions(innerSelect, procCommand, metadata, context);
            }
        }

        // Rewrite having
        Criteria having = query.getHaving();
        if(having != null) {
            query.setHaving(rewriteCriteria(having, procCommand, context, metadata));
        }
                
        rewriteExpressions(query.getSelect(), procCommand, metadata, context);

        if (query.getOrderBy() != null && !query.getIsXML()) {
            makeSelectUnique(query.getSelect(), true);
            rewriteOrderBy(query, procCommand, metadata, context);
        }
        
        if (query.getLimit() != null) {
            query.setLimit(rewriteLimitClause(query.getLimit()));
        }
        
        if (query.getInto() != null) {
            query = rewriteSelectInto(query, metadata, procCommand, context);
        }
        
        return query;
    }
    
    private static void rewriteExpressions(LanguageObject obj,
                                           final Command procCommand,
                                           final QueryMetadataInterface metadata,
                                           final CommandContext context) throws QueryValidatorException {
        if (obj == null) {
            return;
        }
        ExpressionMappingVisitor visitor = new ExpressionMappingVisitor(null) {
            /** 
             * @see com.metamatrix.query.sql.visitor.ExpressionMappingVisitor#replaceExpression(com.metamatrix.query.sql.symbol.Expression)
             */
            @Override
            public Expression replaceExpression(Expression element) {
                try {
                    return rewriteExpression(element, procCommand, context, metadata);
                } catch (QueryValidatorException err) {
                    throw new MetaMatrixRuntimeException(err);
                }
            }
        };
        try {
            PostOrderNavigator.doVisit(obj, visitor);
        } catch (MetaMatrixRuntimeException err) {
            if (err.getChild() instanceof QueryValidatorException) {
                throw (QueryValidatorException)err.getChild();
            } 
            throw err;
        }
    }
	
    /**
     * Rewrite the order by clause.
     *  
     * @param query
     * @throws QueryValidatorException 
     */
    public static void rewriteOrderBy(QueryCommand query, Command procCommand, QueryMetadataInterface metadata, CommandContext context) throws QueryValidatorException {
        OrderBy orderBy = query.getOrderBy();
        if (orderBy == null) {
            return;
        }
        List projectedSymbols = null;
        if (query instanceof Query) {
            if (((Query)query).getIsXML()) {
                return;
            }
            projectedSymbols = ((Query)query).getSelect().getProjectedSymbols();
        } else {
            projectedSymbols = query.getProjectedSymbols();
        }
        if (orderBy.isInPlanForm()) {
            rewriteExpressions(orderBy, procCommand, metadata, context);
        }

        OrderBy newOrderBy = new OrderBy();
        newOrderBy.setUnrelated(orderBy.hasUnrelated());
        HashSet<Expression> previousExpressions = new HashSet<Expression>();
        
        for (int i = 0; i < orderBy.getVariableCount(); i++) {
            SingleElementSymbol querySymbol = orderBy.getVariable(i);
            if (!orderBy.isInPlanForm()) { 
                //get the order by item from the select clause, the variable must be an element symbol
            	//however we have a hack to determine the position...
            	Object id = ((ElementSymbol)querySymbol).getMetadataID();
            	if (id instanceof TempMetadataID) {
	                int index = ((TempMetadataID)((ElementSymbol)querySymbol).getMetadataID()).getPosition();
	                if (index != -1) {
	                	querySymbol = (SingleElementSymbol)((SingleElementSymbol)projectedSymbols.get(index)).clone();
	                }
            	} // else not a projected symbol
            } 
            Expression expr = SymbolMap.getExpression(querySymbol);
            if (!previousExpressions.add(expr)) {
                continue;
            }
            
            if (query instanceof Query && EvaluateExpressionVisitor.isFullyEvaluatable(expr, true)) {
                continue;
            }
            newOrderBy.addVariable((SingleElementSymbol)querySymbol.clone(), orderBy.getOrderType(i).booleanValue());
        }
        
        if (newOrderBy.getVariableCount() == 0) {
            query.setOrderBy(null);
        } else {
            newOrderBy.setInPlanForm(true);
            query.setOrderBy(newOrderBy);
        }
    }
    
    /**
     * This method will alias each of the select into elements to the corresponding column name in the 
     * target table.  This ensures that they will all be uniquely named.
     *  
     * @param query
     * @param metadata
     * @throws QueryValidatorException
     */
    private static Query rewriteSelectInto(Query query,
                                          QueryMetadataInterface metadata, Command procCommand, CommandContext context) throws QueryValidatorException {
        Into into = query.getInto();
        
        try {
            List allIntoElements = ResolverUtil.resolveElementsInGroup(into.getGroup(), metadata);
            boolean needsView = false;
            
            for (int i = 0; !needsView && i < allIntoElements.size(); i++) {
                SingleElementSymbol ses = (SingleElementSymbol)allIntoElements.get(i);
                if (ses.getType() != ((SingleElementSymbol)query.getSelect().getProjectedSymbols().get(i)).getType()) {
                    needsView = true;
                }
            }
            
            if (needsView) {
                query.setInto(null);
                query = createInlineViewQuery(into.getGroup(), query, metadata);
                query.setInto(into);
                return query;
            }
            return query;
        } catch (QueryMetadataException err) {
            throw new QueryValidatorException(err, err.getMessage());
        } catch (QueryResolverException err) {
            throw new QueryValidatorException(err, err.getMessage());
        } catch (MetaMatrixComponentException err) {
            throw new QueryValidatorException(err, err.getMessage());
        }
        
    }

    private static void correctProjectedTypes(List actualSymbolTypes,
                                              Query query) throws QueryValidatorException {
        
        List symbols = query.getSelect().getProjectedSymbols();
        
        List newSymbols = SetQuery.getTypedProjectedSymbols(symbols, actualSymbolTypes);
        
        query.getSelect().setSymbols(newSymbols);
    } 
    
	private static SetQuery rewriteSetQuery(SetQuery setQuery, Command procCommand, QueryMetadataInterface metadata, CommandContext context)
				 throws QueryValidatorException {
        
        if (setQuery.getProjectedTypes() != null) {
            for (QueryCommand command : setQuery.getQueryCommands()) {
                if (!(command instanceof Query)) {
                    continue;
                }
                correctProjectedTypes(setQuery.getProjectedTypes(), (Query)command);
            }
            setQuery.setProjectedTypes(null);
        }
        
        setQuery.setLeftQuery((QueryCommand)rewriteCommand(setQuery.getLeftQuery(), procCommand, metadata, context, true));
        setQuery.setRightQuery((QueryCommand)rewriteCommand(setQuery.getRightQuery(), procCommand, metadata, context, true));

        if (setQuery.getOrderBy() != null) {
            makeSelectUnique(setQuery.getProjectedQuery().getSelect(), true);
            rewriteOrderBy(setQuery, procCommand, metadata, context);
        }
        
        if (setQuery.getLimit() != null) {
            setQuery.setLimit(rewriteLimitClause(setQuery.getLimit()));
        }
        
        return setQuery;
    }

	private static FromClause rewriteFromClause(Query parent, FromClause clause, Command procCommand, QueryMetadataInterface metadata, CommandContext context)
			 throws QueryValidatorException {
		if(clause instanceof JoinPredicate) {
			return rewriteJoinPredicate(parent, (JoinPredicate) clause, procCommand, metadata, context);
        } else if (clause instanceof UnaryFromClause) {
            rewriteUnaryFromClause(parent, (UnaryFromClause)clause, metadata, context);
        } else if (clause instanceof SubqueryFromClause) {
            rewriteSubqueryContainer((SubqueryFromClause)clause, procCommand, context, metadata, true);
        }
        return clause;
	}

    private static void rewriteUnaryFromClause(Query parent, UnaryFromClause ufc, QueryMetadataInterface metadata,
                                               CommandContext context) throws QueryValidatorException {
        Command nestedCommand = ufc.getExpandedCommand();
        
        if (nestedCommand == null) {
            return;
        }
            
        ufc.setExpandedCommand(rewriteCommand(nestedCommand, null, metadata, context, true));
    }

	private static JoinPredicate rewriteJoinPredicate(Query parent, JoinPredicate predicate, Command procCommand,
        QueryMetadataInterface metadata, CommandContext context)
			 throws QueryValidatorException {
		List joinCrits = predicate.getJoinCriteria();
		if(joinCrits != null && joinCrits.size() > 0) {
			//rewrite join crits by rewriting a compound criteria
			Criteria criteria = new CompoundCriteria(new ArrayList(joinCrits));
            joinCrits.clear();
            criteria = rewriteCriteria(criteria, procCommand, context, metadata);
            if (criteria instanceof CompoundCriteria && ((CompoundCriteria)criteria).getOperator() == CompoundCriteria.AND) {
                joinCrits.addAll(((CompoundCriteria)criteria).getCriteria());
            } else {
                joinCrits.add(criteria);
            }
			predicate.setJoinCriteria(joinCrits);
		}

        if (predicate.getJoinType() == JoinType.JOIN_UNION) {
            predicate.setJoinType(JoinType.JOIN_FULL_OUTER);
            predicate.setJoinCriteria(Arrays.asList(new Object[] {FALSE_CRITERIA}));
        } else if (predicate.getJoinType() == JoinType.JOIN_RIGHT_OUTER) {
            predicate.setJoinType(JoinType.JOIN_LEFT_OUTER);
            FromClause leftClause = predicate.getLeftClause();
            predicate.setLeftClause(predicate.getRightClause());
            predicate.setRightClause(leftClause);
        }

        predicate.setLeftClause( rewriteFromClause(parent, predicate.getLeftClause(), procCommand, metadata, context ));
        predicate.setRightClause( rewriteFromClause(parent, predicate.getRightClause(), procCommand, metadata, context ));
    
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
    public static Criteria rewriteCriteria(Criteria criteria, Command procCommand, CommandContext context, QueryMetadataInterface metadata) throws QueryValidatorException {
       return rewriteCriteria(criteria, procCommand, context, false, metadata);
    }

	/**
	 * Rewrite the criteria by evaluating some trivial cases.
	 * @param criteria The criteria to rewrite
	 * @param metadata
	 * @param userCriteria The criteria on user's command, used in rewriting HasCriteria
	 * in the procedural language.
	 * @return The re-written criteria
	 */
    private static Criteria rewriteCriteria(Criteria criteria, Command procCommand, CommandContext context, boolean preserveUnknown, QueryMetadataInterface metadata) throws QueryValidatorException {
		if(criteria instanceof CompoundCriteria) {
            return rewriteCriteria((CompoundCriteria)criteria, procCommand, context, true, preserveUnknown, metadata);
		} else if(criteria instanceof NotCriteria) {
			criteria = rewriteCriteria((NotCriteria)criteria, procCommand, context, metadata);
		} else if(criteria instanceof CompareCriteria) {
            criteria = rewriteCriteria((CompareCriteria)criteria, procCommand, context, metadata);
        } else if(criteria instanceof SubqueryCompareCriteria) {
            criteria = rewriteCriteria((SubqueryCompareCriteria)criteria, procCommand, context, metadata);
		} else if(criteria instanceof MatchCriteria) {
            criteria = rewriteCriteria((MatchCriteria)criteria, procCommand, context, metadata);
		} else if(criteria instanceof SetCriteria) {
            criteria = rewriteCriteria((SetCriteria)criteria, procCommand, context, metadata);
        } else if(criteria instanceof IsNullCriteria) {
            criteria = rewriteCriteria((IsNullCriteria)criteria, procCommand, context, metadata);
        } else if(criteria instanceof BetweenCriteria) {
            criteria = rewriteCriteria((BetweenCriteria)criteria, procCommand, context, preserveUnknown, metadata);
		} else if(criteria instanceof HasCriteria) {
            criteria = rewriteCriteria((HasCriteria)criteria, procCommand, context, metadata);
		} else if(criteria instanceof TranslateCriteria) {
            criteria = rewriteCriteria((TranslateCriteria)criteria, procCommand, context, metadata);
		} else if (criteria instanceof ExistsCriteria) {
		    rewriteSubqueryContainer((SubqueryContainer)criteria, procCommand, context, metadata, true);
		} else if (criteria instanceof SubquerySetCriteria) {
		    SubquerySetCriteria sub = (SubquerySetCriteria)criteria;
		    if (isNull(sub.getExpression())) {
		        return UNKNOWN_CRITERIA;
		    }
		    rewriteSubqueryContainer((SubqueryContainer)criteria, procCommand, context, metadata, true);
        } else if (criteria instanceof DependentSetCriteria) {
            criteria = rewriteCriteria((AbstractSetCriteria)criteria, procCommand, context, metadata);
        }

        return evaluateCriteria(criteria);
	}
    
    /**
     * Performs simple expression flattening
     *  
     * @param criteria
     * @return
     */
    public static Criteria optimizeCriteria(CompoundCriteria criteria) {
        try {
            return rewriteCriteria(criteria, null, null, false, false, null);
        } catch (QueryValidatorException err) {
            //shouldn't happen
            return criteria;
        }
    }
    
    /** May be simplified if this is an AND and a sub criteria is always
     * false or if this is an OR and a sub criteria is always true
     */
    private static Criteria rewriteCriteria(CompoundCriteria criteria, Command procCommand, CommandContext context, boolean rewrite, boolean preserveUnknown, QueryMetadataInterface metadata) throws QueryValidatorException {
        List crits = criteria.getCriteria();
        int operator = criteria.getOperator();

        // Walk through crits and collect converted ones
        LinkedHashSet newCrits = new LinkedHashSet(crits.size());
        Iterator critIter = crits.iterator();
        while(critIter.hasNext()) {
            Criteria converted = (Criteria) critIter.next();
            if (rewrite) {
                converted = rewriteCriteria(converted, procCommand, context, preserveUnknown, metadata);
                converted = evaluateCriteria(converted);
            } else if (converted instanceof CompoundCriteria) {
                converted = rewriteCriteria((CompoundCriteria)converted, null, null, false, preserveUnknown, metadata);
            }

            //begin boolean optimizations
            if(converted == TRUE_CRITERIA) {
                if(operator == CompoundCriteria.OR) {
                    // this OR must be true as at least one branch is always true
                    return converted;
                }
            } else if(converted == FALSE_CRITERIA) {
                if(operator == CompoundCriteria.AND) {
                    // this AND must be false as at least one branch is always false
                    return converted;
                }
            } else {
                if (converted instanceof CompoundCriteria) {
                    CompoundCriteria other = (CompoundCriteria)converted;
                    if (other.getOperator() == criteria.getOperator()) {
                        Iterator i = other.getCriteria().iterator();
                        while (i.hasNext()) {
                            newCrits.add(i.next());
                        }
                        continue;
                    } 
                } 
                //if we're not interested in preserving unknowns, then treat unknown as false
                if (!preserveUnknown && converted == UNKNOWN_CRITERIA) {
                    if (operator == CompoundCriteria.AND) {
                        return FALSE_CRITERIA;
                    }
                } else {
                    newCrits.add(converted);
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
            return (Criteria) newCrits.iterator().next();
        } else {
            criteria.getCriteria().clear();
            criteria.getCriteria().addAll(newCrits);
            return criteria;
        }
	}
    
    private static Criteria evaluateCriteria(Criteria crit) throws QueryValidatorException {
        if(EvaluateExpressionVisitor.isFullyEvaluatable(crit, true)) {
            try {
            	Boolean eval = new Evaluator(Collections.emptyMap(), null, null).evaluateTVL(crit, Collections.emptyList());
                
                if (eval == null) {
                    return UNKNOWN_CRITERIA;
                }
                
                if(Boolean.TRUE.equals(eval)) {
                    return TRUE_CRITERIA;
                }
                
                return FALSE_CRITERIA;                
                
            } catch(CriteriaEvaluationException e) {
                throw new QueryValidatorException(e, ErrorMessageKeys.REWRITER_0001, QueryExecPlugin.Util.getString(ErrorMessageKeys.REWRITER_0001, crit));
            } catch(MetaMatrixComponentException e) {
                throw new QueryValidatorException(e, ErrorMessageKeys.REWRITER_0001, QueryExecPlugin.Util.getString(ErrorMessageKeys.REWRITER_0001, crit));
            }
        }
        
        return crit;
    }
    

	private static Criteria rewriteCriteria(NotCriteria criteria, Command procCommand, CommandContext context, QueryMetadataInterface metadata) throws QueryValidatorException {
        Criteria innerCrit = rewriteCriteria(criteria.getCriteria(), procCommand, context, true, metadata);
        
        innerCrit = evaluateCriteria(innerCrit);
        if(innerCrit == TRUE_CRITERIA) {
            return FALSE_CRITERIA;
        } else if(innerCrit == FALSE_CRITERIA) {
            return TRUE_CRITERIA;
        } else if (innerCrit == UNKNOWN_CRITERIA) {
            return UNKNOWN_CRITERIA;
        }
        
        if (innerCrit instanceof NotCriteria) {
            return ((NotCriteria)innerCrit).getCriteria();
        }
        
        criteria.setCriteria(innerCrit);
        return criteria;
	}

    /**
     * Rewrites "a [NOT] BETWEEN b AND c" as "a &gt;= b AND a &lt;= c", or as "a &lt;= b OR a&gt;= c"
     * @param criteria
     * @param procCommand
     * @return
     * @throws QueryValidatorException
     */
    private static Criteria rewriteCriteria(BetweenCriteria criteria, Command procCommand, CommandContext context, boolean preserveUnknown, QueryMetadataInterface metadata) throws QueryValidatorException {
        CompareCriteria lowerCriteria = new CompareCriteria(criteria.getExpression(),
                                                            criteria.isNegated() ? CompareCriteria.LT: CompareCriteria.GE,
                                                            criteria.getLowerExpression());
        CompareCriteria upperCriteria = new CompareCriteria(criteria.getExpression(),
                                                            criteria.isNegated() ? CompareCriteria.GT: CompareCriteria.LE,
                                                            criteria.getUpperExpression());
        CompoundCriteria newCriteria = new CompoundCriteria(criteria.isNegated() ? CompoundCriteria.OR : CompoundCriteria.AND,
                                                            lowerCriteria,
                                                            upperCriteria);

        return rewriteCriteria(newCriteria, procCommand, context, preserveUnknown, metadata);
    }

	private static Criteria rewriteCriteria(CompareCriteria criteria, Command procCommand, CommandContext context, QueryMetadataInterface metadata) throws QueryValidatorException {
		Expression leftExpr = rewriteExpression(criteria.getLeftExpression(), procCommand, context, metadata);
		Expression rightExpr = rewriteExpression(criteria.getRightExpression(), procCommand, context, metadata);

        if(!EvaluateExpressionVisitor.willBecomeConstant(rightExpr) && EvaluateExpressionVisitor.willBecomeConstant(leftExpr)) {
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

		} else {
			criteria.setLeftExpression(leftExpr);
			criteria.setRightExpression(rightExpr);
		}

        if(criteria.getLeftExpression() instanceof Function && EvaluateExpressionVisitor.willBecomeConstant(criteria.getRightExpression())) {
            criteria = simplifyWithInverse(criteria);
        }
        
        if (isNull(criteria.getLeftExpression()) || isNull(criteria.getRightExpression())) {
            return UNKNOWN_CRITERIA;
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
    private static Criteria rewriteCriteria(SubqueryCompareCriteria criteria, Command procCommand, CommandContext context, QueryMetadataInterface metadata) throws QueryValidatorException {

        Expression leftExpr = rewriteExpression(criteria.getLeftExpression(), procCommand, context, metadata);
        
        if (isNull(leftExpr)) {
            return UNKNOWN_CRITERIA;
        }
        
        criteria.setLeftExpression(leftExpr);

        if (criteria.getPredicateQuantifier() == SubqueryCompareCriteria.ANY){
            criteria.setPredicateQuantifier(SubqueryCompareCriteria.SOME);
        }
        
        rewriteSubqueryContainer(criteria, procCommand, context, metadata, true);

        return criteria;
    }
    
    private static CompareCriteria simplifyWithInverse(CompareCriteria criteria) throws QueryValidatorException {
        Expression leftExpr = criteria.getLeftExpression();
        
        Function leftFunction = (Function) leftExpr;
        if(isSimpleMathematicalFunction(leftFunction)) {
            return simplifyMathematicalCriteria(criteria);
        }   
        if(criteria.getOperator() == CompareCriteria.EQ || criteria.getOperator() == CompareCriteria.NE) {
            if(criteria.getRightExpression() instanceof Constant && (FunctionLibrary.isConvert(leftFunction))) { 

                return simplifyConvertFunction(criteria); 
                
            } 
            return simplifyParseFormatFunction(criteria);
        }
        return criteria;
    }
    
    private static boolean isSimpleMathematicalFunction(Function function) {
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
    private static Integer INTEGER_ZERO = new Integer(0);
    private static Double DOUBLE_ZERO = new Double(0);
    private static Float FLOAT_ZERO = new Float(0);
    private static Long LONG_ZERO = new Long(0);
    private static BigInteger BIG_INTEGER_ZERO = new BigInteger("0"); //$NON-NLS-1$
    private static BigDecimal BIG_DECIMAL_ZERO = new BigDecimal("0"); //$NON-NLS-1$
    private static Short SHORT_ZERO = new Short((short)0);
    private static Byte BYTE_ZERO = new Byte((byte)0);

    /**
     * @param criteria
     * @return CompareCriteria
     */
    private static CompareCriteria simplifyMathematicalCriteria(CompareCriteria criteria)
    throws QueryValidatorException {

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
        FunctionLibrary funcLib = FunctionLibraryManager.getFunctionLibrary();
        FunctionDescriptor descriptor = funcLib.findFunction(oppFunc, new Class[] { rightExpr.getType(), const1.getType() });
        if (descriptor == null){
            //See defect 9380 - this can be caused by const2 being a null Constant, for example (? + 1) < null
            return criteria;
        }

        
        if (rightExpr instanceof Constant) {
            Constant const2 = (Constant)rightExpr;
            try {
                Object result = funcLib.invokeFunction(
                    descriptor, new Object[] { const2.getValue(), const1.getValue() } );
                combinedConst = new Constant(result, descriptor.getReturnType());
            } catch(InvalidFunctionException e) {
            	throw new QueryValidatorException(e, ErrorMessageKeys.REWRITER_0003, QueryExecPlugin.Util.getString(ErrorMessageKeys.REWRITER_0003, e.getMessage()));
        	} catch(FunctionExecutionException e) {
            	throw new QueryValidatorException(e, ErrorMessageKeys.REWRITER_0003, QueryExecPlugin.Util.getString(ErrorMessageKeys.REWRITER_0003, e.getMessage()));
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

        if (expr instanceof Function) {
            return simplifyWithInverse(criteria);
        }
        
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
    private static CompareCriteria simplifyConvertFunction(CompareCriteria crit) throws QueryValidatorException {
        Function leftFunction = (Function) crit.getLeftExpression();
        Constant rightConstant = (Constant) crit.getRightExpression();
        Expression leftExpr = leftFunction.getArgs()[0];
        
        String leftExprTypeName = DataTypeManager.getDataTypeName(leftExpr.getType());
        
        if (leftExpr.getType() == DataTypeManager.DefaultDataClasses.NULL) {
            return crit;
        }
        
        Constant result = null;
        try {
            result = ResolverUtil.convertConstant(DataTypeManager.getDataTypeName(rightConstant.getType()), leftExprTypeName, rightConstant);
        } catch(QueryResolverException e) {
            
        }
        
        if (result == null) {
        	if (crit.getOperator() == CompareCriteria.EQ) {
                return FALSE_CRITERIA;
            }
            return TRUE_CRITERIA;
        }
        
        crit.setRightExpression(result);
        crit.setLeftExpression(leftExpr);

        if (leftExpr instanceof Function) {
            return simplifyWithInverse(crit);
        }
        
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
    private static Criteria simplifyConvertFunction(SetCriteria crit, Command procCommand, CommandContext context, QueryMetadataInterface metadata) throws QueryValidatorException {
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
            
            Constant result = null;
            try {
                result = ResolverUtil.convertConstant(DataTypeManager.getDataTypeName(rightConstant.getType()), leftExprTypeName, rightConstant);
            } catch(QueryResolverException e) {
                
            }
            
            if (result != null) {
            	newValues.add(result);
            } else {
            	removedSome = true;
            	i.remove();
            }
        }
        
        if (!convertedAll) {
        	if (!removedSome) {
        		return crit; //just return as is
        	}
        	return rewriteCriteria(crit, procCommand, context, metadata);
        }
        crit.setExpression(leftExpr);
        crit.setValues(newValues);
        return rewriteCriteria(crit, procCommand, context, metadata);
    }
        
    private static CompareCriteria simplifyParseFormatFunction(CompareCriteria crit) throws QueryValidatorException {
        Function leftFunction = (Function) crit.getLeftExpression();
        String funcName = leftFunction.getName().toLowerCase();
        String inverseFunction = null;
        if(funcName.startsWith("parse")) { //$NON-NLS-1$
            String type = funcName.substring(5);
            inverseFunction = "format" + type; //$NON-NLS-1$
        } else if(funcName.startsWith("format")) { //$NON-NLS-1$
            String type = funcName.substring(6);
            inverseFunction = "parse" + type; //$NON-NLS-1$
        } else {
            return crit;
        }
        Expression rightExpr = crit.getRightExpression();
        Expression leftExpr = leftFunction.getArgs()[0];
        Expression formatExpr = leftFunction.getArgs()[1];
        if(!(formatExpr instanceof Constant)) {
            return crit;
        }
        FunctionLibrary funcLib = FunctionLibraryManager.getFunctionLibrary();
        FunctionDescriptor descriptor = funcLib.findFunction(inverseFunction, new Class[] { rightExpr.getType(), formatExpr.getType() });
        if(descriptor == null){
            return crit;
        }
        String format = (String)((Constant)formatExpr).getValue();
        try {
            /*
             * The following is a hack at fixing defect 23792.  Our strategy of always inverting parse and format functions was wrong in 
             * many cases, so this code will catch many circumstances that were missed before.
             */
            Expression dateExpression = null;
            FunctionDescriptor forwardFunction = null;
            FunctionDescriptor reverseFunction = null;
            boolean parseFunction = false;
            boolean checkExpression = true;
            
            if (java.util.Date.class.isAssignableFrom(leftExpr.getType())) {
                dateExpression = leftExpr;
                forwardFunction = leftFunction.getFunctionDescriptor();
                reverseFunction = descriptor;
            } else if (java.util.Date.class.isAssignableFrom(rightExpr.getType())) {
                dateExpression = rightExpr;
                forwardFunction = descriptor;
                reverseFunction = leftFunction.getFunctionDescriptor();
                parseFunction = true;
            }
            
            if (dateExpression != null) {
                Object example = EXAMPLE_TIMESTAMP;
                if (DataTypeManager.DefaultDataClasses.DATE.equals(dateExpression.getType())) {
                    example = EXAMPLE_DATE;
                } else if (DataTypeManager.DefaultDataClasses.TIME.equals(dateExpression.getType())) {
                    example = EXAMPLE_TIME;
                }
                if (parseFunction) {
                    if (rightExpr instanceof Constant) {
                        example = ((Constant)rightExpr).getValue(); 
                    } else {
                        checkExpression = false;
                    }
                }
                if (checkExpression) {
                    Object result = funcLib.invokeFunction(forwardFunction, new Object[] {example, format});
                    result = funcLib.invokeFunction(reverseFunction, new Object[] { result, format } );
                    if (!example.equals(result)) {
                        if (parseFunction) {
                            if (crit.getOperator() == CompareCriteria.EQ) {
                                return FALSE_CRITERIA;
                            }
                            return TRUE_CRITERIA;
                        }
                        //the format is loosing information, so it must not be invertable
                        return crit;
                    }
                }
            }
        } catch(InvalidFunctionException e) {
            String errorMsg = QueryExecPlugin.Util.getString("QueryRewriter.criteriaError", crit); //$NON-NLS-1$
            throw new QueryValidatorException(e, errorMsg);
        } catch(FunctionExecutionException e) {
            String errorMsg = QueryExecPlugin.Util.getString("QueryRewriter.criteriaError", crit); //$NON-NLS-1$
            throw new QueryValidatorException(e, errorMsg);
        }
        try {   
            if (rightExpr instanceof Constant) {
                Object result = funcLib.invokeFunction(
                                                       descriptor, new Object[] { ((Constant)rightExpr).getValue(), format } );
                crit.setRightExpression(new Constant(result, descriptor.getReturnType()));
                crit.setLeftExpression(leftExpr);
            } else {
                Function conversion = new Function(descriptor.getName(), new Expression[] { rightExpr, formatExpr });
                conversion.setType(leftExpr.getType());
                conversion.setFunctionDescriptor(descriptor);
                
                crit.setRightExpression(conversion);
                crit.setLeftExpression(leftExpr);
            }
        } catch(InvalidFunctionException e) {
            String errorMsg = QueryExecPlugin.Util.getString("QueryRewriter.criteriaError", crit); //$NON-NLS-1$
            throw new QueryValidatorException(e, errorMsg);
        } catch(FunctionExecutionException e) {
            //Not all numeric formats are invertable, so just return the criteria as it may still be valid
            return crit;
        }
        if (leftExpr instanceof Function) {
            return simplifyWithInverse(crit);
        }
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
    private static Criteria simplifyTimestampMerge2(CompareCriteria criteria) {
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
        } else if(leftExpr instanceof Constant && rightExpr instanceof Function) {
            tsCreateFunction = (Function) rightExpr;
            timestampConstant = (Constant) leftExpr;
        } else {
            return criteria;
        }

        // Verify data type of constant and that constant has a value
        if(! timestampConstant.getType().equals(DataTypeManager.DefaultDataClasses.TIMESTAMP) || 
           timestampConstant.getValue() == null) {
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

   /*
    * This method also applies the same simplification for Case 1829.  This is conceptually 
    * the same thing but done  using the timestampCreate system function.  
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

   private static Criteria simplifyTimestampMerge(CompareCriteria criteria) {
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
       } else if(leftExpr instanceof Constant && rightExpr instanceof Function) {
           concatFunction = (Function) rightExpr;
           timestampConstant = (Constant) leftExpr;
       } else {
           return criteria;
       }

       // Verify data type of string constant and that constant has a value
       if(! timestampConstant.getType().equals(DataTypeManager.DefaultDataClasses.STRING) || 
          timestampConstant.getValue() == null) {
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
       if(dateFormat == null || timeFormat == null) {
           return criteria;
       }
       String timestampValue = (String) timestampConstant.getValue();
       if(timestampValue.length() != dateFormat.length() + timeFormat.length()) {
           return criteria;
       }
       
       // Passed all the checks, so build the optimized version
       try {
           SimpleDateFormat timestampFormatter = new SimpleDateFormat(dateFormat + timeFormat);
           java.util.Date parsedTimestamp = timestampFormatter.parse(timestampValue);

           Constant dateConstant = new Constant(TimestampWithTimezone.createDate(parsedTimestamp));
           CompareCriteria dateCompare = new CompareCriteria(formatDateFunction.getArgs()[0], CompareCriteria.EQ, dateConstant);

           Constant timeConstant = new Constant(TimestampWithTimezone.createTime(parsedTimestamp));
           CompareCriteria timeCompare = new CompareCriteria(formatTimeFunction.getArgs()[0], CompareCriteria.EQ, timeConstant);
           
           CompoundCriteria compCrit = new CompoundCriteria(CompoundCriteria.AND, dateCompare, timeCompare);
           return compCrit;
           
       } catch(ParseException e) {
           return criteria;        
       }
    }
    
    private static Criteria rewriteCriteria(MatchCriteria criteria, Command procCommand, CommandContext context, QueryMetadataInterface metadata) throws QueryValidatorException {
		criteria.setLeftExpression( rewriteExpression(criteria.getLeftExpression(), procCommand, context, metadata));
		criteria.setRightExpression( rewriteExpression(criteria.getRightExpression(), procCommand, context, metadata));
        
        if (isNull(criteria.getLeftExpression()) || isNull(criteria.getRightExpression())) {
            return UNKNOWN_CRITERIA;
        }

        Expression rightExpr = criteria.getRightExpression();
        if(rightExpr instanceof Constant) {
            Constant constant = (Constant) rightExpr;
            if(constant.getType().equals(DataTypeManager.DefaultDataClasses.STRING)) {
                String value = (String) constant.getValue();

                char escape = criteria.getEscapeChar();

                // Check whether escape char is unnecessary and remove it
                if(escape != MatchCriteria.NULL_ESCAPE_CHAR) {                            
                    if(value.indexOf(escape) < 0) {
                        criteria.setEscapeChar(MatchCriteria.NULL_ESCAPE_CHAR);
                    }
                }

                // if the value of this string constant is '%', then we know the crit will 
                // always be true                    
                if ( value.equals( String.valueOf(MatchCriteria.WILDCARD_CHAR)) ) { 
                    return criteria.isNegated()?FALSE_CRITERIA:TRUE_CRITERIA;                        
                }  
                
                // if both left and right expressions are strings, and the LIKE match characters ('*', '_') are not present 
                //  in the right expression, rewrite the criteria as EQUALs rather than LIKE
                if(DataTypeManager.DefaultDataClasses.STRING.equals(criteria.getLeftExpression().getType()) && value.indexOf(escape) < 0 && value.indexOf(MatchCriteria.MATCH_CHAR) < 0 && value.indexOf(MatchCriteria.WILDCARD_CHAR) < 0) {
                    return rewriteCriteria(new CompareCriteria(criteria.getLeftExpression(), criteria.isNegated()?CompareCriteria.NE:CompareCriteria.EQ, criteria.getRightExpression()), procCommand, context, metadata);
                }
            }
        }

		return criteria;
	}
    
    private static Criteria rewriteCriteria(AbstractSetCriteria criteria, Command procCommand, CommandContext context, QueryMetadataInterface metadata) throws QueryValidatorException {
        criteria.setExpression(rewriteExpression(criteria.getExpression(), procCommand, context, metadata));
        
        if (isNull(criteria.getExpression())) {
            return UNKNOWN_CRITERIA;
        }

        return criteria;
    }


	private static Criteria rewriteCriteria(SetCriteria criteria, Command procCommand, CommandContext context, QueryMetadataInterface metadata) throws QueryValidatorException {
		criteria.setExpression(rewriteExpression(criteria.getExpression(), procCommand, context, metadata));
        
        if (isNull(criteria.getExpression())) {
            return UNKNOWN_CRITERIA;
        }

		Collection vals = criteria.getValues();

        LinkedHashSet newVals = new LinkedHashSet(vals.size());
        Iterator valIter = vals.iterator();
        while(valIter.hasNext()) {
            Expression value = rewriteExpression( (Expression) valIter.next(), procCommand, context, metadata);
            if (isNull(value)) {
                continue;
            }
            newVals.add(value);
        }
        
        criteria.setValues(newVals);
        
        if (newVals.size() == 1) {
            Expression value = (Expression)newVals.iterator().next();
            return rewriteCriteria(new CompareCriteria(criteria.getExpression(), criteria.isNegated()?CompareCriteria.NE:CompareCriteria.EQ, value), procCommand, context, metadata);
        } else if (newVals.size() == 0) {
            return FALSE_CRITERIA;
        }
        
        if(criteria.getExpression() instanceof Function ) {
            
            Function leftFunction = (Function)criteria.getExpression();
            if(FunctionLibrary.isConvert(leftFunction)) {
                return simplifyConvertFunction(criteria, procCommand, context, metadata);        
            }
        }

		return criteria;
	}

	private static Criteria rewriteCriteria(IsNullCriteria criteria, Command procCommand, CommandContext context, QueryMetadataInterface metadata) throws QueryValidatorException {
		criteria.setExpression(rewriteExpression(criteria.getExpression(), procCommand, context, metadata));
		return criteria;
	}

    public static Expression rewriteExpression(Expression expression, Command procCommand, CommandContext context, QueryMetadataInterface metadata) throws QueryValidatorException {
		if(expression instanceof Function) {
			return rewriteFunction((Function) expression, procCommand, context, metadata);
		} else if (expression instanceof CaseExpression) {
            return rewriteCaseExpression((CaseExpression)expression, procCommand, context, metadata);
        } else if (expression instanceof SearchedCaseExpression) {
            return rewriteCaseExpression((SearchedCaseExpression)expression, procCommand, context, metadata);
        } else if (expression instanceof ScalarSubquery) {
            rewriteSubqueryContainer((ScalarSubquery)expression, procCommand, context, metadata, true);
            return expression;
        } else if (expression instanceof ExpressionSymbol && !(expression instanceof AggregateSymbol)) {
            return rewriteExpression(((ExpressionSymbol)expression).getExpression(), procCommand, context, metadata);
        } else if(expression instanceof AggregateSymbol){
        	return rewriteExpression((AggregateSymbol)expression);
        }
        return expression;
	}

    private static Expression rewriteExpression(AggregateSymbol expression) {
    	if (!expression.getAggregateFunction().equals(ReservedWords.COUNT)
				&& !expression.getAggregateFunction().equals(ReservedWords.SUM)
				&& EvaluateExpressionVisitor.willBecomeConstant(expression.getExpression())) {
			try {
				return new ExpressionSymbol(expression.getName(), ResolverUtil
						.convertExpression(expression.getExpression(),DataTypeManager.getDataTypeName(expression.getType())));
			} catch (QueryResolverException e) {
				//should not happen, so throw as a runtime
				throw new MetaMatrixRuntimeException(e);
			}
		}
		return expression;
	}

	private static Expression rewriteFunction(Function function, Command procCommand, CommandContext context, QueryMetadataInterface metadata) throws QueryValidatorException {
		//rewrite alias functions
		String actualName =ALIASED_FUNCTIONS.get(function.getName().toLowerCase());
		if (actualName != null) {
			function.setName(actualName);
		}
		
		//space(x) => repeat(' ', x)
		if (function.getName().equalsIgnoreCase(FunctionLibrary.SPACE)) {
			//change the function into timestampadd
			Function result = new Function(SourceSystemFunctions.REPEAT,
					new Expression[] {new Constant(" "), function.getArg(0)}); //$NON-NLS-1$
			//resolve the function
			FunctionDescriptor descriptor = 
	        	FunctionLibraryManager.getFunctionLibrary().findFunction(SourceSystemFunctions.REPEAT, new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.INTEGER});
			result.setFunctionDescriptor(descriptor);
			result.setType(DataTypeManager.DefaultDataClasses.STRING);
			return rewriteFunction(result, procCommand, context, metadata);
		}
		
		//from_unixtime(a) => timestampadd(SQL_TSI_SECOND, a, new Timestamp(0)) 
		if (function.getName().equalsIgnoreCase(FunctionLibrary.FROM_UNIXTIME)) {
			//change the function into timestampadd
			Function result = new Function(FunctionLibrary.TIMESTAMPADD,
					new Expression[] {new Constant(ReservedWords.SQL_TSI_SECOND), function.getArg(0), new Constant(new Timestamp(0)) });
			//resolve the function
			FunctionDescriptor descriptor = 
	        	FunctionLibraryManager.getFunctionLibrary().findFunction(FunctionLibrary.TIMESTAMPADD, new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.INTEGER, DataTypeManager.DefaultDataClasses.TIMESTAMP });
			result.setFunctionDescriptor(descriptor);
			result.setType(DataTypeManager.DefaultDataClasses.TIMESTAMP);
			return rewriteFunction(result, procCommand, context, metadata);
		}
		
		//rewrite nullif => case when (a = b) then null else a
		if (function.getName().equalsIgnoreCase(FunctionLibrary.NULLIF)) {
			List when = Arrays.asList(new Criteria[] {new CompareCriteria(function.getArg(0), CompareCriteria.EQ, function.getArg(1))});
			Constant nullConstant = new Constant(null, function.getType());
			List then = Arrays.asList(new Expression[] {nullConstant});
			SearchedCaseExpression caseExpr = new SearchedCaseExpression(when, then);
			caseExpr.setElseExpression(function.getArg(0));
			caseExpr.setType(function.getType());
			return rewriteExpression(caseExpr, procCommand, context, metadata);
		}
		
		if (function.getName().equalsIgnoreCase(FunctionLibrary.COALESCE)) {
			Expression[] args = function.getArgs();
			if (args.length == 2) {
				Function result = new Function(SourceSystemFunctions.IFNULL,
						new Expression[] {function.getArg(0), function.getArg(1) });
				//resolve the function
				FunctionDescriptor descriptor = 
		        	FunctionLibraryManager.getFunctionLibrary().findFunction(SourceSystemFunctions.IFNULL, new Class[] { function.getType(), function.getType()  });
				result.setFunctionDescriptor(descriptor);
				result.setType(function.getType());
				return rewriteFunction(result, procCommand, context, metadata);
			}
		}
		
		//rewrite concat2 - CONCAT2(a, b) ==> CASE WHEN (a is NULL AND b is NULL) THEN NULL ELSE CONCAT( NVL(a, ''), NVL(b, '') )
		if (function.getName().equalsIgnoreCase(FunctionLibrary.CONCAT2)) {
			Expression[] args = function.getArgs();
			Function[] newArgs = new Function[args.length];

			for(int i=0; i<args.length; i++) {
				newArgs[i] = new Function(SourceSystemFunctions.IFNULL, new Expression[] {args[i], new Constant("")}); //$NON-NLS-1$
				newArgs[i].setType(args[i].getType());
				Assertion.assertTrue(args[i].getType() == DataTypeManager.DefaultDataClasses.STRING);
		        FunctionDescriptor descriptor = 
		        	FunctionLibraryManager.getFunctionLibrary().findFunction(SourceSystemFunctions.IFNULL, new Class[] { args[i].getType(), DataTypeManager.DefaultDataClasses.STRING });
		        newArgs[i].setFunctionDescriptor(descriptor);
			}
			
			Function concat = new Function(SourceSystemFunctions.CONCAT, newArgs);
			concat.setType(DataTypeManager.DefaultDataClasses.STRING);
			FunctionDescriptor descriptor = 
	        	FunctionLibraryManager.getFunctionLibrary().findFunction(SourceSystemFunctions.CONCAT, new Class[] { DataTypeManager.DefaultDataClasses.STRING, DataTypeManager.DefaultDataClasses.STRING });
			concat.setFunctionDescriptor(descriptor);
			
			List when = Arrays.asList(new Criteria[] {new CompoundCriteria(CompoundCriteria.AND, new IsNullCriteria(args[0]), new IsNullCriteria(args[1]))});
			Constant nullConstant = new Constant(null, DataTypeManager.DefaultDataClasses.STRING);
			List then = Arrays.asList(new Expression[] {nullConstant});
			SearchedCaseExpression caseExpr = new SearchedCaseExpression(when, then);
			caseExpr.setElseExpression(concat);
			caseExpr.setType(DataTypeManager.DefaultDataClasses.STRING);
			return rewriteExpression(caseExpr, procCommand, context, metadata);
		}
		
		Expression[] args = function.getArgs();
		Expression[] newArgs = new Expression[args.length];
		        
        // Rewrite args
		for(int i=0; i<args.length; i++) {
			newArgs[i] = rewriteExpression(args[i], procCommand, context, metadata);
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
        
        if(EvaluateExpressionVisitor.isFullyEvaluatable(function, true)) {
            try {
                Object result = new Evaluator(Collections.emptyMap(), null, context).evaluate(function, Collections.emptyList());
				Constant constant = new Constant(result, function.getType());
				return constant;
			} catch(ExpressionEvaluationException e) {
                String funcName = function.getName();
                if(FunctionLibrary.isConvert(function)) {
                    Expression expr = newArgs[0];
                    String sourceType = DataTypeManager.getDataTypeName(newArgs[0].getType());
                    String targetType = (String) ((Constant) newArgs[1]).getValue();
                    throw new QueryValidatorException(e, ErrorMessageKeys.REWRITER_0004, 
                        QueryExecPlugin.Util.getString(ErrorMessageKeys.REWRITER_0004, new Object[] {funcName, expr, sourceType, targetType}));
                        
                }
                throw new QueryValidatorException(e, e.getMessage());
			} catch(MetaMatrixComponentException e) {
                throw new QueryValidatorException(e, ErrorMessageKeys.REWRITER_0005, QueryExecPlugin.Util.getString(ErrorMessageKeys.REWRITER_0005, function));
			}
		} 
        return function;
	}

	private static Expression convertDecodeFunction(Function function){
    	Expression exprs[] = function.getArgs();
    	String decodeString = (String)((Constant)exprs[1]).getValue();
    	String decodeDelimiter = ","; //$NON-NLS-1$
    	if(exprs.length == 3){
    		decodeDelimiter = (String)((Constant)exprs[2]).getValue();
    	}
    	List newWhens = new ArrayList();
    	List newThens = new ArrayList();
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
	
    public static String convertString(String string) {
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
	
    private static Expression rewriteCaseExpression(CaseExpression expr, Command procCommand, CommandContext context, QueryMetadataInterface metadata)
        throws QueryValidatorException {
    	List<CompareCriteria> whens = new ArrayList<CompareCriteria>(expr.getWhenCount());
    	for (Expression expression: (List<Expression>)expr.getWhen()) {
    		whens.add(new CompareCriteria((Expression)expr.getExpression().clone(), CompareCriteria.EQ, expression));
    	}
    	SearchedCaseExpression sce = new SearchedCaseExpression(whens, expr.getThen());
    	sce.setElseExpression(expr.getElseExpression());
    	sce.setType(expr.getType());
    	return rewriteCaseExpression(sce, procCommand, context, metadata);
    }

    private static Expression rewriteCaseExpression(SearchedCaseExpression expr, Command procCommand, CommandContext context, QueryMetadataInterface metadata)
        throws QueryValidatorException {
        int whenCount = expr.getWhenCount();
        ArrayList whens = new ArrayList(whenCount);
        ArrayList thens = new ArrayList(whenCount);

        for (int i = 0; i < whenCount; i++) {
            
            // Check the when to see if this CASE can be rewritten due to an always true/false when
            Criteria rewrittenWhen = rewriteCriteria(expr.getWhenCriteria(i), procCommand, context, metadata);
            if(EvaluateExpressionVisitor.isFullyEvaluatable(rewrittenWhen, true)) {
                try {
                	boolean eval = Evaluator.evaluate(rewrittenWhen);
                    if(eval) {
                        // WHEN is always true, so just return the THEN
                        return rewriteExpression(expr.getThenExpression(i), procCommand, context, metadata);
                    } 

                    // WHEN is never true, so just skip this WHEN/THEN pair in the lists
                    continue;
                } catch(Exception e) {
                    // ignore and don't simplify - shouldn't happen
                }
            } 
            
            whens.add(rewrittenWhen);
            thens.add(rewriteExpression(expr.getThenExpression(i), procCommand, context, metadata));
        }

        expr.setElseExpression(rewriteExpression(expr.getElseExpression(), procCommand, context, metadata));
        
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
        
    private static Command rewriteExec(StoredProcedure storedProcedure, Command procCommand, QueryMetadataInterface metadata, CommandContext context) throws QueryValidatorException {
        //After this method, no longer need to display named parameters
        storedProcedure.setDisplayNamedParameters(false);
        
        for (Iterator i = storedProcedure.getInputParameters().iterator(); i.hasNext();) {
            SPParameter param = (SPParameter)i.next();
            param.setExpression(rewriteExpression(param.getExpression(), procCommand, context, metadata));
        }
        
        return storedProcedure;
    }

	private static Command rewriteInsert(Insert insert, Command procCommand, CommandContext context, QueryMetadataInterface metadata) throws QueryValidatorException {
        
        if ( insert.getQueryExpression() != null ) {
            Query query = null;
            QueryCommand nested = insert.getQueryExpression();
            QueryRewriter.rewrite(nested, procCommand, metadata, context);
            if(nested instanceof SetQuery) {
                try {
                    query = createInlineViewQuery(insert.getGroup(), nested, metadata);
                } catch (QueryMetadataException err) {
                    throw new QueryValidatorException(err, err.getMessage());
                } catch (QueryResolverException err) {
                    throw new QueryValidatorException(err, err.getMessage());
                } catch (MetaMatrixComponentException err) {
                    throw new QueryValidatorException(err, err.getMessage());
                }
            } else {
                query = (Query)nested;  
                query.setOption(insert.getOption());
            }
            query.setInto( new Into( insert.getGroup() ) );  
            return query;
        }

        // Evaluate any function / constant trees in the insert values
        List expressions = insert.getValues();
        List evalExpressions = new ArrayList(expressions.size());
        Iterator expIter = expressions.iterator();
        while(expIter.hasNext()) {
            Expression exp = (Expression) expIter.next();
            evalExpressions.add( rewriteExpression( exp, procCommand, context, metadata ));
        }

        insert.setValues(evalExpressions);        

		return insert;
	}

	/**
	 * Creates an inline view around the target query.
	 */
    private static Query createInlineViewQuery(GroupSymbol group,
                                          QueryCommand nested,
                                          QueryMetadataInterface metadata) throws QueryMetadataException, QueryResolverException, MetaMatrixComponentException {
        List actualSymbols = ResolverUtil.resolveElementsInGroup(group, metadata);
        return createInlineViewQuery(group, nested, metadata, actualSymbols);
    }

    public static Query createInlineViewQuery(GroupSymbol group,
                                               QueryCommand nested,
                                               QueryMetadataInterface metadata,
                                               List actualSymbols) throws QueryMetadataException,
                                                                  QueryResolverException,
                                                                  MetaMatrixComponentException {
        Query query = new Query();
        Select select = new Select();
        query.setSelect(select);
        From from = new From();
        GroupSymbol inlineGroup = new GroupSymbol(group.getName().replace('.', '_') + "_1"); //$NON-NLS-1$
        from.addClause(new UnaryFromClause(inlineGroup)); 
        TempMetadataStore store = new TempMetadataStore();
        TempMetadataAdapter tma = new TempMetadataAdapter(metadata, store);
        Query firstProject = nested.getProjectedQuery(); 
        makeSelectUnique(firstProject.getSelect(), false);
        
        store.addTempGroup(inlineGroup.getName(), nested.getProjectedSymbols());
        inlineGroup.setMetadataID(store.getTempGroupID(inlineGroup.getName()));
        
        List actualTypes = new ArrayList(nested.getProjectedSymbols().size());
        
        for (Iterator i = actualSymbols.iterator(); i.hasNext();) {
            SingleElementSymbol ses = (SingleElementSymbol)i.next();
            actualTypes.add(ses.getType());
        }
        List selectSymbols = SetQuery.getTypedProjectedSymbols(ResolverUtil.resolveElementsInGroup(inlineGroup, tma), actualTypes);
        select.addSymbols(deepClone(selectSymbols, SingleElementSymbol.class));
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
    
    public static <S extends Expression, T extends S> List<S> deepClone(List<T> collection, Class<S> clazz) {
    	ArrayList<S> result = new ArrayList<S>(collection.size());
    	for (Expression expression : collection) {
			result.add((S)expression.clone());
		}
    	return result;
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

	private static Update rewriteUpdate(Update update, Command procCommand, CommandContext context, QueryMetadataInterface metadata) throws QueryValidatorException {
        // Evaluate any function on the right side of set clauses
        for (SetClause entry : update.getChangeList().getClauses()) {
        	entry.setValue(rewriteExpression(entry.getValue(), procCommand, context, metadata));
        }

		// Rewrite criteria
		Criteria crit = update.getCriteria();
		if(crit != null) {
			update.setCriteria(rewriteCriteria(crit, procCommand, context, metadata));
		}

		return update;
	}

	private static Delete rewriteDelete(Delete delete, Command procCommand, CommandContext context, QueryMetadataInterface metadata) throws QueryValidatorException {
		// Rewrite criteria
		Criteria crit = delete.getCriteria();
		if(crit != null) {
			delete.setCriteria(rewriteCriteria(crit, procCommand, context, metadata));
		}

		return delete;
	}
    
    private static Limit rewriteLimitClause(Limit limit) {
        try {
            if (limit.getOffset() != null && EvaluateExpressionVisitor.isFullyEvaluatable(limit.getOffset(), true)) {
                limit.setOffset(new Constant(Evaluator.evaluate(limit.getOffset())));
            }
            if (limit.getRowLimit() != null && EvaluateExpressionVisitor.isFullyEvaluatable(limit.getRowLimit(), true)) {
                limit.setRowLimit(new Constant(Evaluator.evaluate(limit.getRowLimit())));
            }
        } catch (ExpressionEvaluationException e) {
            throw new MetaMatrixRuntimeException(e);
        } catch (MetaMatrixComponentException e) {
            throw new MetaMatrixRuntimeException(e);
        }
        return limit;
    }
}