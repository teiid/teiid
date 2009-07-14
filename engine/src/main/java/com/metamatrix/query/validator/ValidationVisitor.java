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

package com.metamatrix.query.validator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.function.FunctionLibrary;
import com.metamatrix.query.metadata.SupportConstants;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.ProcedureReservedWords;
import com.metamatrix.query.sql.lang.BatchedUpdateCommand;
import com.metamatrix.query.sql.lang.BetweenCriteria;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.CompoundCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.Delete;
import com.metamatrix.query.sql.lang.DependentSetCriteria;
import com.metamatrix.query.sql.lang.Drop;
import com.metamatrix.query.sql.lang.DynamicCommand;
import com.metamatrix.query.sql.lang.GroupBy;
import com.metamatrix.query.sql.lang.Insert;
import com.metamatrix.query.sql.lang.Into;
import com.metamatrix.query.sql.lang.IsNullCriteria;
import com.metamatrix.query.sql.lang.MatchCriteria;
import com.metamatrix.query.sql.lang.NotCriteria;
import com.metamatrix.query.sql.lang.Option;
import com.metamatrix.query.sql.lang.OrderBy;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.QueryCommand;
import com.metamatrix.query.sql.lang.Select;
import com.metamatrix.query.sql.lang.SetClause;
import com.metamatrix.query.sql.lang.SetClauseList;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.lang.SubqueryCompareCriteria;
import com.metamatrix.query.sql.lang.SubquerySetCriteria;
import com.metamatrix.query.sql.lang.Update;
import com.metamatrix.query.sql.lang.SetQuery.Operation;
import com.metamatrix.query.sql.navigator.PreOrderNavigator;
import com.metamatrix.query.sql.proc.AssignmentStatement;
import com.metamatrix.query.sql.proc.CreateUpdateProcedureCommand;
import com.metamatrix.query.sql.proc.CriteriaSelector;
import com.metamatrix.query.sql.proc.DeclareStatement;
import com.metamatrix.query.sql.proc.HasCriteria;
import com.metamatrix.query.sql.proc.IfStatement;
import com.metamatrix.query.sql.proc.TranslateCriteria;
import com.metamatrix.query.sql.proc.WhileStatement;
import com.metamatrix.query.sql.symbol.AbstractCaseExpression;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.ExpressionSymbol;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.util.SymbolMap;
import com.metamatrix.query.sql.visitor.AggregateSymbolCollectorVisitor;
import com.metamatrix.query.sql.visitor.CommandCollectorVisitor;
import com.metamatrix.query.sql.visitor.ElementCollectorVisitor;
import com.metamatrix.query.sql.visitor.EvaluateExpressionVisitor;
import com.metamatrix.query.sql.visitor.FunctionCollectorVisitor;
import com.metamatrix.query.sql.visitor.GroupCollectorVisitor;
import com.metamatrix.query.sql.visitor.PredicateCollectorVisitor;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;
import com.metamatrix.query.util.ErrorMessageKeys;

public class ValidationVisitor extends AbstractValidationVisitor {

    // State during validation
    private boolean isXML = false;	// only used for Query commands
    
    // update procedure being validated
    private CreateUpdateProcedureCommand updateProc;
    
    public void reset() {
        super.reset();
        this.isXML = false;
        this.updateProc = null;
    }

    // ############### Visitor methods for language objects ##################
    
    public void visit(BatchedUpdateCommand obj) {
        List commands = obj.getUpdateCommands();
        Command command = null;
        int type = 0;
        for (int i = 0; i < commands.size(); i++) {
            command = (Command)commands.get(i);
            type = command.getType();
            if (type != Command.TYPE_INSERT &&
                type != Command.TYPE_UPDATE &&
                type != Command.TYPE_DELETE &&
                type != Command.TYPE_QUERY) { // SELECT INTO command
                handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.invalid_batch_command"),command); //$NON-NLS-1$
            } else if (type == Command.TYPE_QUERY) {
                Into into = ((Query)command).getInto();
                if (into == null) {
                    handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.invalid_batch_command"),command); //$NON-NLS-1$
                }
            }
        }
    }

	public void visit(Delete obj) {
    	validateNoXMLUpdates(obj);
        validateHasProjectedSymbols(obj);
        validateGroupUpdatable(obj);
        if (obj.getGroup().isTempTable()) {
            handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.update_temp", obj.getGroup()), obj.getGroup()); //$NON-NLS-1$
        }
    }

    public void visit(GroupBy obj) {
    	// Get list of all group by IDs
        List groupBySymbols = obj.getSymbols();
        validateSortable(groupBySymbols);
		Iterator symbolIter = groupBySymbols.iterator();
		while(symbolIter.hasNext()) {
            SingleElementSymbol symbol = (SingleElementSymbol)symbolIter.next();
            if(symbol instanceof ExpressionSymbol) {
                ExpressionSymbol exprSymbol = (ExpressionSymbol) symbol;
                Expression expr = exprSymbol.getExpression();
                if(! (expr instanceof Function || expr instanceof AbstractCaseExpression)) {
                    handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.Expr_in_GROUP_BY_must_be_elem_func_case", expr), expr); //$NON-NLS-1$                        
                }
            }                
		}
    }
    
    @Override
    public void visit(GroupSymbol obj) {
    	try {
			if (this.getMetadata().isScalarGroup(obj.getMetadataID())) {
			    handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.invalid_scalar_group_reference", obj),obj); //$NON-NLS-1$    		
			}
		} catch (QueryMetadataException e) {
			handleException(e);
		} catch (MetaMatrixComponentException e) {
			handleException(e);
		}
    }

    public void visit(Insert obj) {
        validateNoXMLUpdates(obj);
        validateHasProjectedSymbols(obj);
        validateGroupUpdatable(obj);
        validateInsert(obj);
    }

    public void visit(OrderBy obj) {
        validateSortable(obj.getVariables());
    }
    
    public void visit(Query obj) {
        validateHasProjectedSymbols(obj);
        if(isXMLCommand(obj)) {
            //no temp table (Select Into) allowed
            if(obj.getInto() != null){
                handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0069),obj);
            }

        	this.isXML = true;
	        validateXMLQuery(obj);
        } else {
            validateAggregates(obj);

            //if it is select with no from, should not have ScalarSubQuery
            if(obj.getSelect() != null && obj.getFrom() == null){
                if(!CommandCollectorVisitor.getCommands(obj).isEmpty()){
                    handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0067),obj);
                }
            }
            
            if (obj.getInto() != null) {
                validateSelectInto(obj);
            }                        
        }
    }
	
	public void visit(Select obj) {
        validateSelectElements(obj);
        if(obj.isDistinct()) {
            validateSortable(obj.getProjectedSymbols());
        }
    }

	public void visit(SubquerySetCriteria obj) {
		if (isNonComparable(obj.getExpression())) {
			handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0027, obj),obj);
    	}
        this.validateRowLimitFunctionNotInInvalidCriteria(obj);
        
		Collection projSymbols = obj.getCommand().getProjectedSymbols();

		//Subcommand should have one projected symbol (query with one expression
		//in SELECT or stored procedure execution that returns a single value).
		if(projSymbols.size() != 1) {
			handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0011),obj);
		}
	}

    public void visit(DependentSetCriteria obj) {
        this.validateRowLimitFunctionNotInInvalidCriteria(obj);
    }

    public void visit(SetQuery obj) {
        validateHasProjectedSymbols(obj);
        validateSetQuery(obj);
    }
    
    public void visit(Update obj) {
        validateNoXMLUpdates(obj);
        validateHasProjectedSymbols(obj);
        validateGroupUpdatable(obj);
        validateUpdate(obj);
    }

    public void visit(Into obj) {
        GroupSymbol target = obj.getGroup();
        validateInsertGroup(obj, target);
    }

    private void validateInsertGroup(LanguageObject obj,
                                     GroupSymbol target) {
        try {
            if(!target.isTempGroupSymbol() && getMetadata().isVirtualGroup(target.getMetadataID())) {
                handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0070),obj);
            } else {
                validateGroupSupportsUpdate(target);
            }
        } catch (MetaMatrixComponentException e) {
            handleException(e, obj);
        }
    }
    
    public void visit(Function obj) {
    	if(FunctionLibrary.LOOKUP.equalsIgnoreCase(obj.getName())) {
    		try {
				ResolverUtil.ResolvedLookup resolvedLookup = ResolverUtil.resolveLookup(obj, getMetadata());
				if(ValidationVisitor.isNonComparable(resolvedLookup.getKeyElement())) {
		            handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.invalid_lookup_key", resolvedLookup.getKeyElement()), resolvedLookup.getKeyElement()); //$NON-NLS-1$            
		        }
			} catch (MetaMatrixComponentException e) {
				handleException(e, obj);
			} catch (MetaMatrixProcessingException e) {
				handleException(e, obj);
			}
        } else if (obj.getFunctionDescriptor().getName().equalsIgnoreCase(FunctionLibrary.CONTEXT)) {
            if(!isXML) {
                // can't use this pseudo-function in non-XML queries
                handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.The_context_function_cannot_be_used_in_a_non-XML_command"), obj); //$NON-NLS-1$
            } else {
                if (!(obj.getArg(0) instanceof ElementSymbol)){
                    handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.OPTIMIZER_0036), obj); 
                }
                
                for (Iterator functions = FunctionCollectorVisitor.getFunctions(obj.getArg(1), false).iterator(); functions.hasNext();) {
                    Function function = (Function)functions.next();
                    
                    if (function.getFunctionDescriptor().getName().equalsIgnoreCase(FunctionLibrary.CONTEXT)) {
                        handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.Context_function_nested"), obj); //$NON-NLS-1$
                    }
                }
            }
    	} else if (obj.getFunctionDescriptor().getName().equalsIgnoreCase(FunctionLibrary.ROWLIMIT) ||
                   obj.getFunctionDescriptor().getName().equalsIgnoreCase(FunctionLibrary.ROWLIMITEXCEPTION)) {
            if(isXML) {
                if (!(obj.getArg(0) instanceof ElementSymbol)) {
                    // Arg must be an element symbol
                    handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.2"), obj); //$NON-NLS-1$
                }
            } else {
                // can't use this pseudo-function in non-XML queries
                handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.The_rowlimit_function_cannot_be_used_in_a_non-XML_command"), obj); //$NON-NLS-1$
            }
        }
    }

    // ############### Visitor methods for stored procedure lang objects ##################

    public void visit(AssignmentStatement obj) {
    	
    	ElementSymbol variable = obj.getVariable();

    	String groupName = variable.getGroupSymbol().getCanonicalName();

    	if(groupName.equals(ProcedureReservedWords.INPUT) ||
			groupName.equals(ProcedureReservedWords.CHANGING)) {
			handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0012, new Object[] {ProcedureReservedWords.INPUT, ProcedureReservedWords.CHANGING}), obj);
		}

		if(obj.hasCommand()) {
	        Collection projSymbols = obj.getCommand().getProjectedSymbols();

			//The command execution should result is a value that is assigned to the variable
			// there cannot be more than one column in its results
			if(projSymbols.size() != 1) {
				handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0013), obj);
			} else {
				SingleElementSymbol value = (SingleElementSymbol) projSymbols.iterator().next();
                Class valueType = value.getType();
				Class varType = variable.getType();
				if(!varType.equals(valueType)) {
					handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0014), obj);
				}
			}
		} 
    }

    public void visit(CreateUpdateProcedureCommand obj) {
        if(!obj.isUpdateProcedure()){
        	//Every virtual procedure should have at least one query.
        	if(CommandCollectorVisitor.getCommands(obj).isEmpty()){
        		handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.Procedure_should_have_query"), obj); //$NON-NLS-1$
        	}
        	
            //check that the procedure does not contain references to itself
            if (GroupCollectorVisitor.getGroups(obj,true).contains(obj.getVirtualGroup())) {
            	handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.Procedure_has_group_self_reference"),obj); //$NON-NLS-1$
            }
            
            return;
        }

		// set the state to validate this procedure
        this.updateProc = obj;
        validateContainsRowsUpdatedVariable(obj);
    }

    public void visit(DeclareStatement obj) {
    	ElementSymbol variable = obj.getVariable();
    	String elementname = variable.getShortName().toUpperCase();

		// varaible cnnot be one of the special variables
    	if(elementname.equals(ProcedureReservedWords.INPUT) || elementname.equals(ProcedureReservedWords.CHANGING)
				|| elementname.equals(ProcedureReservedWords.ROWS_UPDATED)) {
			handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0017, new Object[] {ProcedureReservedWords.INPUT, ProcedureReservedWords.CHANGING, ProcedureReservedWords.ROWS_UPDATED}), obj);
		}
        
        visit((AssignmentStatement)obj);
    }

    public void visit(IfStatement obj) {
    	Criteria criteria = obj.getCondition();

    	validatorCriteriaStatement(obj, criteria);
    }
    
    public void visit(WhileStatement obj) {
        Criteria criteria = obj.getCondition();

        validatorCriteriaStatement(obj, criteria);
    }

    /** 
     * @param obj
     * @param criteria
     */
    private void validatorCriteriaStatement(LanguageObject obj,
                                            Criteria criteria) {
        Iterator criteriaIter = PredicateCollectorVisitor.getPredicates(criteria).iterator();
    	while(criteriaIter.hasNext()) {
    		Criteria predicateCriteria = (Criteria) criteriaIter.next();
    		Iterator elmntIter = ElementCollectorVisitor.getElements(predicateCriteria, true).iterator();
    		if(predicateCriteria instanceof TranslateCriteria) {
				handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0019), obj);
    		} else if(!(predicateCriteria instanceof HasCriteria)) {
    			while(elmntIter.hasNext()) {
    				ElementSymbol element = (ElementSymbol) elmntIter.next();
    				if(!element.isExternalReference()) {
						handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0020), obj);
    				}
    			}
    		}
    	}
    }

    public void visit(TranslateCriteria obj) {

		if(obj.hasTranslations()) {
			Collection selectElmnts = null;
			if(obj.getSelector().hasElements()) {
				selectElmnts = obj.getSelector().getElements();
			}
			Iterator critIter = obj.getTranslations().iterator();
			while(critIter.hasNext()) {
				CompareCriteria transCrit = (CompareCriteria) critIter.next();
				Collection leftElmnts = ElementCollectorVisitor.getElements(transCrit.getLeftExpression(), true);
				// there is always only one element
				ElementSymbol leftExpr = (ElementSymbol)leftElmnts.iterator().next();

				if(selectElmnts != null && !selectElmnts.contains(leftExpr)) {
					handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0021), leftExpr);
				}
			}
		}

		// additional validation checks
		validateTranslateCriteria(obj);
    }

    public void visit(CompoundCriteria obj) {
        // Validate use of 'rowlimit' or 'rowlimitexception' pseudo-function - each occurrence must be in a single
        // CompareCriteria which is entirely it's own conjunct (not OR'ed with anything else)
        if (isXML) {
            // Collect all occurrances of rowlimit and rowlimitexception functions
            List rowLimitFunctions = new ArrayList();
            FunctionCollectorVisitor visitor = new FunctionCollectorVisitor(rowLimitFunctions, FunctionLibrary.ROWLIMIT);
            PreOrderNavigator.doVisit(obj, visitor); 
            visitor = new FunctionCollectorVisitor(rowLimitFunctions, FunctionLibrary.ROWLIMITEXCEPTION);
            PreOrderNavigator.doVisit(obj, visitor);
            final int functionCount = rowLimitFunctions.size();
            if (functionCount > 0) {
                
                // Verify each use of rowlimit function is in a compare criteria that is 
                // entirely it's own conjunct
                Iterator conjunctIter = Criteria.separateCriteriaByAnd(obj).iterator();            
                
                int i = 0;
                while (conjunctIter.hasNext() && i<functionCount ) {
                    Object conjunct = conjunctIter.next();
                    if (conjunct instanceof CompareCriteria) {
                        CompareCriteria crit = (CompareCriteria)conjunct;
                        if ((rowLimitFunctions.contains(crit.getLeftExpression()) && !rowLimitFunctions.contains(crit.getRightExpression())) || 
                            (rowLimitFunctions.contains(crit.getRightExpression()) && !rowLimitFunctions.contains(crit.getLeftExpression()))) {
                        	i++;
                        }
                    }
                }
                if (i<functionCount) {
                    handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.3"), obj); //$NON-NLS-1$
                }
            }
        }
        
    }

    // ######################### Validation methods #########################

	/**
	 * A valid translated expression is not an <code>AggregateSymbol</code> and
	 * does not include elements not present on the groups of the command using
	 * the translated criteria.
	 */
    protected void validateTranslateCriteria(TranslateCriteria obj) {
    	if(this.currentCommand == null) {
    		return;
    	}
    	Map symbolMap = this.updateProc.getSymbolMap();
    	// modeler validation
    	if(symbolMap == null) {
    		return;
    	}
		Command userCommand = this.updateProc.getUserCommand();
		Criteria userCrit = null;
		int userCmdType = userCommand.getType();
		switch(userCmdType) {
			case Command.TYPE_DELETE:
				userCrit = ((Delete)userCommand).getCriteria();
				break;
			case Command.TYPE_UPDATE:
				userCrit = ((Update)userCommand).getCriteria();
				break;
			default:
				break;
		}
		// nothing to validate if there is no user criteria
		if(userCrit == null) {
			return;
		}

    	Collection transleElmnts = ElementCollectorVisitor.getElements(obj, true);
    	Collection groups = GroupCollectorVisitor.getGroups(this.currentCommand, true);
		int selectType = obj.getSelector().getSelectorType();

		Collection predicates = PredicateCollectorVisitor.getPredicates(userCrit);
		if (predicates.size() != Criteria.separateCriteriaByAnd(userCrit).size()) {
			handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.translated_or"), userCrit); //$NON-NLS-1$
		}
		Iterator critIter = predicates.iterator();
		while(critIter.hasNext()) {
			Criteria predCrit = (Criteria) critIter.next();
			if(selectType != CriteriaSelector.NO_TYPE) {
				if(predCrit instanceof CompareCriteria) {
					CompareCriteria ccCrit = (CompareCriteria) predCrit;
					if(selectType != ccCrit.getOperator()) {
						continue;
					}
				} else if(predCrit instanceof MatchCriteria) {
					if(selectType != CriteriaSelector.LIKE) {
						continue;
					}
				} else if(predCrit instanceof IsNullCriteria) {
					if(selectType != CriteriaSelector.IS_NULL) {
						continue;
					}
                } else if(predCrit instanceof SetCriteria) {
                    if(selectType != CriteriaSelector.IN) {
                    	continue;
                    }
                } else if(predCrit instanceof BetweenCriteria) {
                    if(selectType != CriteriaSelector.BETWEEN) {
                    	continue;
                    }
				}
			}
	    	Iterator critEmlntIter = ElementCollectorVisitor.getElements(predCrit, true).iterator();
	    	// collect all elements elements on the criteria map to
	    	while(critEmlntIter.hasNext()) {
	    		ElementSymbol criteriaElement = (ElementSymbol) critEmlntIter.next();
	    		if(transleElmnts.contains(criteriaElement)) {
		    		Expression mappedExpr = (Expression) symbolMap.get(criteriaElement);
		    		if(mappedExpr instanceof AggregateSymbol) {
						handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0022, criteriaElement), criteriaElement);
		    		}

		    		Iterator mapElmntIter = ElementCollectorVisitor.getElements(mappedExpr, true).iterator();
			    	boolean groupMatch = false;
			    	while(mapElmntIter.hasNext()) {
				    	ElementSymbol mapElement = (ElementSymbol) mapElmntIter.next();
				    	GroupSymbol mapGrp = mapElement.getGroupSymbol();
				    	if(groups.contains(mapGrp)) {
				    		groupMatch = true;
				    	}
			    	}
			    	if(!groupMatch) {
						handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0023, criteriaElement), criteriaElement);
			    	}
				}
	    	}
		}
    }

    protected void validateSelectElements(Select obj) {
    	if(isXML) {
    		return;
    	}

        Collection elements = ElementCollectorVisitor.getElements(obj, true);
        
        Collection cantSelect = validateElementsSupport(
            elements,
            SupportConstants.Element.SELECT );

		if(cantSelect != null) {
            handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0024, cantSelect), cantSelect);
		}
    }

    protected void validateHasProjectedSymbols(Command obj) {
        if(obj.getProjectedSymbols().size() == 0) {
            handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0025), obj);
        }
    }

    /**
     * Validate that no elements of type OBJECT are in a SELECT DISTINCT or
     * and ORDER BY.
     * @param symbols List of SingleElementSymbol
     */
    protected void validateSortable(List symbols) {
        Iterator iter = symbols.iterator();
        while(iter.hasNext()) {
            SingleElementSymbol symbol = (SingleElementSymbol) iter.next();
            if (isNonComparable(symbol)) {
                handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0026, symbol), symbol);
            }
        }
    }

    public static boolean isNonComparable(Expression symbol) {
        return DataTypeManager.isNonComparable(DataTypeManager.getDataTypeName(symbol.getType()));
    }

	/**
	 * This method can be used to validate Update commands cannot be
	 * executed against XML documents.
	 */
    protected void validateNoXMLUpdates(Command obj) {
     	if(isXMLCommand(obj)) {
            handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0029), obj);
     	}
    }

	/**
	 * This method can be used to validate commands used in the stored
	 * procedure languge cannot be executed against XML documents.
	 */
    protected void validateNoXMLProcedures(Command obj) {
     	if(isXMLCommand(obj)) {
            handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0030), obj);
     	}
    }

    private void validateXMLQuery(Query obj) {
        if(obj.getGroupBy() != null) {
            handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0031), obj);
        }
        if(obj.getHaving() != null) {
            handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0032), obj);
        }
        if(obj.getLimit() != null) {
            handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.limit_not_valid_for_xml"), obj);
        }
    }
    
    protected void validateGroupSupportsUpdate(GroupSymbol groupSymbol) throws QueryMetadataException, MetaMatrixComponentException {
        if(! getMetadata().groupSupports(groupSymbol.getMetadataID(), SupportConstants.Group.UPDATE)) {
            handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0033, SQLStringVisitor.getSQLString(groupSymbol)), groupSymbol);
        }
    }
    
    protected void validateGroupUpdatable(Command obj) {
        try {
            Collection groups = GroupCollectorVisitor.getGroups(obj, true);
            validateGroupSupportsUpdate((GroupSymbol) groups.iterator().next());
        } catch(MetaMatrixComponentException e) {
            handleException(e, obj);
        }
    }

    protected void validateSetQuery(SetQuery query) {
        // Walk through sub queries - validate each one separately and
        // also check the columns of each for comparability
        for (QueryCommand subQuery : query.getQueryCommands()) {
            if(isXMLCommand(subQuery)) {
                handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0034), query);
            }
        }
        
        if (!query.isAll() || query.getOperation() == Operation.EXCEPT || query.getOperation() == Operation.INTERSECT) {
            validateSortable(query.getProjectedSymbols());
        }
        
        if (query.isAll() && (query.getOperation() == Operation.EXCEPT || query.getOperation() == Operation.INTERSECT)) {
            handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.excpet_intersect_all"), query); //$NON-NLS-1$
        }
    }

    private void validateAggregates(Query query) {
        Select select = query.getSelect();
        GroupBy groupBy = query.getGroupBy();
        Criteria having = query.getHaving();
        if(groupBy != null || having != null || !AggregateSymbolCollectorVisitor.getAggregates(select, false).isEmpty()) {
            Set<Expression> groupSymbols = null;
            if(groupBy != null) {
                groupSymbols = new HashSet<Expression>();
                for (final Iterator iterator = groupBy.getSymbols().iterator(); iterator.hasNext();) {
                    final SingleElementSymbol element = (SingleElementSymbol)iterator.next();
                    groupSymbols.add(SymbolMap.getExpression(element));
                }
            }
            
            // Validate HAVING, if it exists
            AggregateValidationVisitor visitor = new AggregateValidationVisitor(groupSymbols);
            if(having != null) {
                AggregateValidationVisitor.validate(having, visitor);
            }
            
            // Validate SELECT
            List projectedSymbols = select.getProjectedSymbols();
            Iterator symbolIter = projectedSymbols.iterator();
            while(symbolIter.hasNext()) {
                SingleElementSymbol symbol = (SingleElementSymbol) symbolIter.next();
                AggregateValidationVisitor.validate(symbol, visitor);                                            
            }
            
            // Move items to this report
            ValidatorReport report = visitor.getReport();
            Collection items = report.getItems();
            super.getReport().addItems(items);        
        }
    }
    
    protected void validateInsert(Insert obj) {
        Collection vars = obj.getVariables();
        Iterator varIter = vars.iterator();
        Collection values = obj.getValues();
        Iterator valIter = values.iterator();
        GroupSymbol insertGroup = obj.getGroup();


        try {
            // Validate that all elements in variable list are updatable
            Iterator elementIter = vars.iterator();
            while(elementIter.hasNext()) {
                ElementSymbol insertElem = (ElementSymbol) elementIter.next();
                if(! getMetadata().elementSupports(insertElem.getMetadataID(), SupportConstants.Element.UPDATE)) {
                    handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0052, insertElem), insertElem);
                }
            }

            // Get elements in the group.
    		Collection insertElmnts = new LinkedList(ResolverUtil.resolveElementsInGroup(insertGroup, getMetadata()));

    		// remove all elements specified in insert to get the ignored elements
    		insertElmnts.removeAll(vars);

			Iterator ignoreIter = insertElmnts.iterator();
			while(ignoreIter.hasNext()) {
	            ElementSymbol nextElmnt = (ElementSymbol) ignoreIter.next();
				if(!getMetadata().elementSupports(nextElmnt.getMetadataID(), SupportConstants.Element.DEFAULT_VALUE) &&
					!getMetadata().elementSupports(nextElmnt.getMetadataID(), SupportConstants.Element.NULL) &&
                    !getMetadata().elementSupports(nextElmnt.getMetadataID(), SupportConstants.Element.AUTO_INCREMENT)) {
		                handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0053, new Object[] {insertGroup, nextElmnt}), nextElmnt);
				}
			}

            if ( obj.getQueryExpression() != null) {
                validateInsertGroup(obj, obj.getGroup());
                return;
            }
            
            //check to see if the elements support nulls in metadata,
            // if any of the value present in the insert are null
            while(valIter.hasNext() && varIter.hasNext()) {
                Expression nextValue = (Expression) valIter.next();
                ElementSymbol nextVar = (ElementSymbol) varIter.next();

                if (EvaluateExpressionVisitor.isFullyEvaluatable(nextValue, true)) {
                    try {
                        // If nextValue is an expression, evaluate it before checking for null
                        Object evaluatedValue = Evaluator.evaluate(nextValue);
                        if(evaluatedValue == null && ! getMetadata().elementSupports(nextVar.getMetadataID(), SupportConstants.Element.NULL)) {
                            handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0055, SQLStringVisitor.getSQLString(nextVar)), nextVar);
                        }
                    } catch(ExpressionEvaluationException e) {
                        //ignore for now, we don't have the context which could be the problem
                    }
                }
            }// end of while
        } catch(MetaMatrixComponentException e) {
            handleException(e, obj);
        } 
    }
    
    protected void validateSetClauseList(SetClauseList list) {
    	Set<ElementSymbol> dups = new HashSet<ElementSymbol>();
	    HashSet changeVars = new HashSet();
	    for (SetClause clause : list.getClauses()) {
	    	ElementSymbol elementID = clause.getSymbol();
	        if (!changeVars.add(elementID)) {
	        	dups.add(elementID);
	        }
		}
	    if(!dups.isEmpty()) {
            handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0062, dups), dups);
	    }
    }
    
    protected void validateUpdate(Update update) {
        try {
            // list of elements that are being updated
		    for (SetClause entry : update.getChangeList().getClauses()) {
        	    ElementSymbol elementID = entry.getSymbol();
                if(elementID.isExternalReference()) {
                    handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0058, SQLStringVisitor.getSQLString(elementID)), elementID);
                }

                // Check that left side element is updatable
                if(! getMetadata().elementSupports(elementID.getMetadataID(), SupportConstants.Element.UPDATE)) {
                    handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0059, elementID), elementID);
                }

			    // Check that right expression is a constant and is non-null
                Expression value = entry.getValue();
                
                if (EvaluateExpressionVisitor.isFullyEvaluatable(value, true)) {
                    try {
                        value = new Constant(Evaluator.evaluate(value));
                    } catch (ExpressionEvaluationException err) {
                    }
                }
                
                if(value instanceof Constant) {
    			    // If value is null, check that element supports this as a nullable column
                    if(((Constant)value).isNull() && ! getMetadata().elementSupports(elementID.getMetadataID(), SupportConstants.Element.NULL)) {
                        handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0060, SQLStringVisitor.getSQLString(elementID)), elementID);
                    }// end of if
                } else if (!EvaluateExpressionVisitor.willBecomeConstant(value)) {
                    // If this is an update on a virtual group, verify that no elements are in the right side
                    GroupSymbol group = update.getGroup();
                    if(getMetadata().isVirtualGroup(group.getMetadataID())) {
                        Collection elements = ElementCollectorVisitor.getElements(value, false);
                        if(elements.size() > 0) {
                            Iterator iter = elements.iterator();
                            while(iter.hasNext()) {
                                ElementSymbol element = (ElementSymbol) iter.next();
                                if(! element.isExternalReference()) {
                                    handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0061, SQLStringVisitor.getSQLString(value)), value);
                                }
                            }
                        }
                    }
                } 
		    }// end of while
        } catch(MetaMatrixComponentException e) {
            handleException(e, update);
        }
        
        validateSetClauseList(update.getChangeList());
        
        if (update.getGroup().isTempTable()) {
            handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.update_temp", update.getGroup()), update.getGroup()); //$NON-NLS-1$
        }
    }
    
    /**
     * Validates SELECT INTO queries.
     * @param query
     * @since 4.2
     */
    protected void validateSelectInto(Query query) {
        List symbols = query.getSelect().getProjectedSymbols();
        GroupSymbol intoGroup = query.getInto().getGroup();
        validateInto(query, symbols, intoGroup);
    }

    private void validateInto(LanguageObject query,
                                List symbols,
                                GroupSymbol intoGroup) {
        try {
            List elementIDs = getMetadata().getElementIDsInGroupID(intoGroup.getMetadataID());
            
            // Check if there are too many elements in the SELECT clause
            if (symbols.size() != elementIDs.size()) {
                handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.select_into_wrong_elements", new Object[] {new Integer(elementIDs.size()), new Integer(symbols.size())}), query); //$NON-NLS-1$
                return;
            }
            
            for (int symbolNum = 0; symbolNum < symbols.size(); symbolNum++) {
                SingleElementSymbol symbol = (SingleElementSymbol)symbols.get(symbolNum);
                Object elementID = elementIDs.get(symbolNum);
                // Check if supports updates
                if (!getMetadata().elementSupports(elementID, SupportConstants.Element.UPDATE)) {
                    handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.element_updates_not_allowed", getMetadata().getFullName(elementID)), intoGroup); //$NON-NLS-1$
                }

                Class symbolType = symbol.getType();
                String symbolTypeName = DataTypeManager.getDataTypeName(symbolType);
                String targetTypeName = getMetadata().getElementType(elementID);
                if (symbolTypeName.equals(targetTypeName)) {
                    continue;
                }
                if (!DataTypeManager.isImplicitConversion(symbolTypeName, targetTypeName)) { // If there's no implicit conversion between the two
                    Object[] params = new Object [] {symbolTypeName, targetTypeName, new Integer(symbolNum + 1), query};
                    handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.select_into_no_implicit_conversion", params), query); //$NON-NLS-1$
                    continue;
                }
            }
        } catch (MetaMatrixComponentException e) {
            handleException(e, query);
        } 
    }
    
    /**
     * Validate that the command assigns a value to the ROWS_UPDATED variable 
     * @param obj
     * @since 4.2
     */
    protected void validateContainsRowsUpdatedVariable(CreateUpdateProcedureCommand obj) {
        final Collection assignVars = new ArrayList();
       // Use visitor to find assignment statements
        LanguageVisitor visitor = new LanguageVisitor() {
            public void visit(AssignmentStatement obj) {
                assignVars.add(obj.getVariable());
            }
        };
        PreOrderNavigator.doVisit(obj, visitor);
        boolean foundVar = false;
        for(Iterator varIter = assignVars.iterator(); varIter.hasNext();) {
            ElementSymbol variable = (ElementSymbol) varIter.next();
            if(variable.getShortName().equalsIgnoreCase(ProcedureReservedWords.ROWS_UPDATED)) {
                foundVar = true;
                break;
            }
        }
        if(!foundVar) {
            handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0016, ProcedureReservedWords.ROWS_UPDATED), obj);
        }
    }
    
    private void validateRowLimitFunctionNotInInvalidCriteria(Criteria obj) {
        // Collect all occurrances of rowlimit and rowlimitexception functions
        List rowLimitFunctions = new ArrayList();
        FunctionCollectorVisitor visitor = new FunctionCollectorVisitor(rowLimitFunctions, FunctionLibrary.ROWLIMIT);
        PreOrderNavigator.doVisit(obj, visitor);      
        visitor = new FunctionCollectorVisitor(rowLimitFunctions, FunctionLibrary.ROWLIMITEXCEPTION);
        PreOrderNavigator.doVisit(obj, visitor); 
        if (rowLimitFunctions.size() > 0) {
            handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.3"), obj); //$NON-NLS-1$
        }
    }
    
    /** 
     * @see com.metamatrix.query.sql.LanguageVisitor#visit(com.metamatrix.query.sql.lang.BetweenCriteria)
     * @since 4.3
     */
    public void visit(BetweenCriteria obj) {
    	if (isNonComparable(obj.getExpression())) {
    		handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0027, obj),obj);    		
    	}
        this.validateRowLimitFunctionNotInInvalidCriteria(obj);
    }

    /** 
     * @see com.metamatrix.query.sql.LanguageVisitor#visit(com.metamatrix.query.sql.lang.IsNullCriteria)
     * @since 4.3
     */
    public void visit(IsNullCriteria obj) {
        this.validateRowLimitFunctionNotInInvalidCriteria(obj);
    }

    /** 
     * @see com.metamatrix.query.sql.LanguageVisitor#visit(com.metamatrix.query.sql.lang.MatchCriteria)
     * @since 4.3
     */
    public void visit(MatchCriteria obj) {
        this.validateRowLimitFunctionNotInInvalidCriteria(obj);
    }

    /** 
     * @see com.metamatrix.query.sql.LanguageVisitor#visit(com.metamatrix.query.sql.lang.NotCriteria)
     * @since 4.3
     */
    public void visit(NotCriteria obj) {
        this.validateRowLimitFunctionNotInInvalidCriteria(obj);
    }

    /** 
     * @see com.metamatrix.query.sql.LanguageVisitor#visit(com.metamatrix.query.sql.lang.SetCriteria)
     * @since 4.3
     */
    public void visit(SetCriteria obj) {
    	if (isNonComparable(obj.getExpression())) {
    		handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0027, obj),obj);    		
    	}
        this.validateRowLimitFunctionNotInInvalidCriteria(obj);
    }

    /** 
     * @see com.metamatrix.query.sql.LanguageVisitor#visit(com.metamatrix.query.sql.lang.SubqueryCompareCriteria)
     * @since 4.3
     */
    public void visit(SubqueryCompareCriteria obj) {
    	if (isNonComparable(obj.getLeftExpression())) {
    		handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0027, obj),obj);    		
    	}
        this.validateRowLimitFunctionNotInInvalidCriteria(obj);
    }
    
    public void visit(Option obj) {
        List dep = obj.getDependentGroups();
        List notDep = obj.getNotDependentGroups();
        if (dep != null && !dep.isEmpty()
            && notDep != null && !notDep.isEmpty()) {
            String groupName = null;
            String notDepGroup = null;
            for (Iterator i = dep.iterator(); i.hasNext();) {
                groupName = (String)i.next();
                for (Iterator j = notDep.iterator(); j.hasNext();) {
                    notDepGroup = (String)j.next();
                    if (notDepGroup.equalsIgnoreCase(groupName)) {
                        handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.group_in_both_dep", groupName), obj); //$NON-NLS-1$
                        return;
                    }
                }
            }
        }
    }
    
    /** 
     * @see com.metamatrix.query.sql.LanguageVisitor#visit(com.metamatrix.query.sql.lang.DynamicCommand)
     */
    public void visit(DynamicCommand obj) {
        if (obj.getIntoGroup() != null) {
            validateInto(obj, obj.getAsColumns(), obj.getIntoGroup());
        }
        if (obj.getUsing() != null) {
        	validateSetClauseList(obj.getUsing());
        }
    }
    
    /** 
     * @see com.metamatrix.query.sql.LanguageVisitor#visit(com.metamatrix.query.sql.lang.Drop)
     */
    public void visit(Drop drop) {
        if (!drop.getTable().isTempTable()) {
            handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.drop_of_nontemptable", drop.getTable()), drop); //$NON-NLS-1$
        }
    }
    
    @Override
    public void visit(CompareCriteria obj) {
    	if (isNonComparable(obj.getLeftExpression())) {
    		handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0027, obj),obj);    		
    	}
    }
        
}
