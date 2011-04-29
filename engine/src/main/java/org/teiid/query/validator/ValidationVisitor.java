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

package org.teiid.query.validator;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.saxon.om.Name11Checker;
import net.sf.saxon.om.QNameException;
import net.sf.saxon.trans.XPathException;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.QueryPlugin;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.function.FunctionMethods;
import org.teiid.query.function.source.XMLSystemFunctions;
import org.teiid.query.metadata.StoredProcedureInfo;
import org.teiid.query.metadata.SupportConstants;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.ProcedureReservedWords;
import org.teiid.query.sql.lang.AlterProcedure;
import org.teiid.query.sql.lang.AlterTrigger;
import org.teiid.query.sql.lang.AlterView;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.BetweenCriteria;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Create;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.lang.Drop;
import org.teiid.query.sql.lang.DynamicCommand;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.GroupBy;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Into;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.MatchCriteria;
import org.teiid.query.sql.lang.NotCriteria;
import org.teiid.query.sql.lang.Option;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
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
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.lang.WithQueryCommand;
import org.teiid.query.sql.lang.XMLTable;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.lang.XMLTable.XMLColumn;
import org.teiid.query.sql.navigator.PreOrderNavigator;
import org.teiid.query.sql.proc.AssignmentStatement;
import org.teiid.query.sql.proc.CommandStatement;
import org.teiid.query.sql.proc.CreateUpdateProcedureCommand;
import org.teiid.query.sql.proc.CriteriaSelector;
import org.teiid.query.sql.proc.DeclareStatement;
import org.teiid.query.sql.proc.HasCriteria;
import org.teiid.query.sql.proc.LoopStatement;
import org.teiid.query.sql.proc.TranslateCriteria;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.DerivedColumn;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.QueryString;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.symbol.TextLine;
import org.teiid.query.sql.symbol.XMLAttributes;
import org.teiid.query.sql.symbol.XMLElement;
import org.teiid.query.sql.symbol.XMLForest;
import org.teiid.query.sql.symbol.XMLNamespaces;
import org.teiid.query.sql.symbol.XMLParse;
import org.teiid.query.sql.symbol.XMLQuery;
import org.teiid.query.sql.symbol.AggregateSymbol.Type;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.AggregateSymbolCollectorVisitor;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.sql.visitor.FunctionCollectorVisitor;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.validator.UpdateValidator.UpdateInfo;
import org.teiid.query.validator.UpdateValidator.UpdateType;
import org.teiid.query.xquery.saxon.SaxonXQueryExpression;
import org.teiid.translator.SourceSystemFunctions;

public class ValidationVisitor extends AbstractValidationVisitor {

    private final class PositiveIntegerConstraint implements
			Reference.Constraint {
    	
    	private String msgKey;
    	
    	public PositiveIntegerConstraint(String msgKey) {
    		this.msgKey = msgKey;
		}
    	
		@Override
		public void validate(Object value) throws QueryValidatorException {
			if (((Integer)value).intValue() < 0) {
				throw new QueryValidatorException(QueryPlugin.Util.getString(msgKey)); 
			}
		}
	}

	// State during validation
    private boolean isXML = false;	// only used for Query commands
    
    // update procedure being validated
    private CreateUpdateProcedureCommand updateProc;
    
    public void setUpdateProc(CreateUpdateProcedureCommand updateProc) {
		this.updateProc = updateProc;
	}
    
    public void reset() {
        super.reset();
        this.isXML = false;
    }

    // ############### Visitor methods for language objects ##################
    
    public void visit(BatchedUpdateCommand obj) {
        List<Command> commands = obj.getUpdateCommands();
        Command command = null;
        int type = 0;
        for (int i = 0; i < commands.size(); i++) {
            command = commands.get(i);
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
        GroupSymbol group = obj.getGroup();
        validateGroupSupportsUpdate(group);
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
                if (!ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(expr).isEmpty() || expr instanceof Constant || expr instanceof Reference) {
                	handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.groupby_subquery", expr), expr); //$NON-NLS-1$
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
		} catch (TeiidComponentException e) {
			handleException(e);
		}
    }

    public void visit(Insert obj) {
        validateNoXMLUpdates(obj);
        validateGroupSupportsUpdate(obj.getGroup());
        validateInsert(obj);
        
        if (obj.getQueryExpression() != null) {
        	validateMultisourceInsert(obj.getGroup());
        }
        if (obj.getUpdateInfo() != null && obj.getUpdateInfo().isInherentInsert()) {
        	try {
				if (obj.getUpdateInfo().findInsertUpdateMapping(obj, false) == null) {
					handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.nonUpdatable", obj.getVariables()), obj); //$NON-NLS-1$
				}
			} catch (QueryValidatorException e) {
				handleValidationError(e.getMessage(), obj);
			}
        }
    }

    @Override
    public void visit(OrderByItem obj) {
    	validateSortable(obj.getSymbol());
    }
    
    public void visit(Query obj) {
        validateHasProjectedSymbols(obj);
        if(isXMLCommand(obj)) {
            //no temp table (Select Into) allowed
            if(obj.getInto() != null){
                handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0069"),obj); //$NON-NLS-1$
            }

        	this.isXML = true;
	        validateXMLQuery(obj);
        } else {
            validateAggregates(obj);

            //if it is select with no from, should not have ScalarSubQuery
            if(obj.getSelect() != null && obj.getFrom() == null){
                if(!ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(obj.getSelect()).isEmpty()){
                    handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0067"),obj); //$NON-NLS-1$
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
		validateSubquery(obj);
		if (isNonComparable(obj.getExpression())) {
			handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0027", obj),obj); //$NON-NLS-1$
    	}
        this.validateRowLimitFunctionNotInInvalidCriteria(obj);
        
		Collection<SingleElementSymbol> projSymbols = obj.getCommand().getProjectedSymbols();

		//Subcommand should have one projected symbol (query with one expression
		//in SELECT or stored procedure execution that returns a single value).
		if(projSymbols.size() != 1) {
			handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0011"),obj); //$NON-NLS-1$
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
        validateGroupSupportsUpdate(obj.getGroup());
        validateUpdate(obj);
    }

    public void visit(Into obj) {
        GroupSymbol target = obj.getGroup();
        validateGroupSupportsUpdate(target);
        validateMultisourceInsert(obj.getGroup());
    }

	private void validateMultisourceInsert(GroupSymbol group) {
		try {
			if (getMetadata().isMultiSource(getMetadata().getModelID(group.getMetadataID()))) {
				handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.multisource_insert", group), group); //$NON-NLS-1$
			}
        } catch (QueryMetadataException e) {
			handleException(e);
		} catch (TeiidComponentException e) {
			handleException(e);
		}
	}

    public void visit(Function obj) {
    	if(FunctionLibrary.LOOKUP.equalsIgnoreCase(obj.getName())) {
    		try {
				ResolverUtil.ResolvedLookup resolvedLookup = ResolverUtil.resolveLookup(obj, getMetadata());
				if(ValidationVisitor.isNonComparable(resolvedLookup.getKeyElement())) {
		            handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.invalid_lookup_key", resolvedLookup.getKeyElement()), resolvedLookup.getKeyElement()); //$NON-NLS-1$            
		        }
			} catch (TeiidComponentException e) {
				handleException(e, obj);
			} catch (TeiidProcessingException e) {
				handleException(e, obj);
			}
        } else if (obj.getFunctionDescriptor().getName().equalsIgnoreCase(FunctionLibrary.CONTEXT)) {
            if(!isXML) {
                // can't use this pseudo-function in non-XML queries
                handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.The_context_function_cannot_be_used_in_a_non-XML_command"), obj); //$NON-NLS-1$
            } else {
                if (!(obj.getArg(0) instanceof ElementSymbol)){
                    handleValidationError(QueryPlugin.Util.getString("ERR.015.004.0036"), obj);  //$NON-NLS-1$
                }
                
                for (Iterator<Function> functions = FunctionCollectorVisitor.getFunctions(obj.getArg(1), false).iterator(); functions.hasNext();) {
                    Function function = functions.next();
                    
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
        } else if(obj.getFunctionDescriptor().getName().equalsIgnoreCase(SourceSystemFunctions.XPATHVALUE)) {
	        // Validate the xpath value is valid
	        if(obj.getArgs()[1] instanceof Constant) {
	            Constant xpathConst = (Constant) obj.getArgs()[1];
                try {
                    XMLSystemFunctions.validateXpath((String)xpathConst.getValue());
                } catch(XPathException e) {
                	handleValidationError(QueryPlugin.Util.getString("QueryResolver.invalid_xpath", e.getMessage()), obj); //$NON-NLS-1$
                }
	        }
        } else if(obj.getFunctionDescriptor().getName().equalsIgnoreCase(SourceSystemFunctions.TO_BYTES) || obj.getFunctionDescriptor().getName().equalsIgnoreCase(SourceSystemFunctions.TO_CHARS)) {
        	try {
        		FunctionMethods.getCharset((String)((Constant)obj.getArg(1)).getValue());
        	} catch (IllegalArgumentException e) {
        		handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.invalid_encoding", obj.getArg(1)), obj); //$NON-NLS-1$
        	}
        }
    }

    // ############### Visitor methods for stored procedure lang objects ##################

    public void visit(AssignmentStatement obj) {
    	
    	ElementSymbol variable = obj.getVariable();

    	validateAssignment(obj, variable);
    }
    
    @Override
    public void visit(CommandStatement obj) {
    	if (obj.getCommand() instanceof StoredProcedure) {
    		StoredProcedure proc = (StoredProcedure)obj.getCommand();
    		for (SPParameter param : proc.getParameters()) {
				if ((param.getParameterType() == SPParameter.RETURN_VALUE 
						|| param.getParameterType() == SPParameter.OUT) && param.getExpression() instanceof ElementSymbol) {
					validateAssignment(obj, (ElementSymbol)param.getExpression());
				}
			}
    	}
    }
    
    @Override
    public void visit(StoredProcedure obj) {
		for (SPParameter param : obj.getInputParameters()) {
			try {
				if (!(param.getExpression() instanceof Constant) && getMetadata().isMultiSourceElement(param.getMetadataID())) {
	            	handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.multisource_constant", param.getParameterSymbol()), param.getParameterSymbol()); //$NON-NLS-1$
	            	continue;
	            }
                if (EvaluatableVisitor.isFullyEvaluatable(param.getExpression(), true)) {
	                try {
	                    // If nextValue is an expression, evaluate it before checking for null
	                    Object evaluatedValue = Evaluator.evaluate(param.getExpression());
	                    if(evaluatedValue == null && ! getMetadata().elementSupports(param.getMetadataID(), SupportConstants.Element.NULL)) {
	                        handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0055", param.getParameterSymbol()), param.getParameterSymbol()); //$NON-NLS-1$
	                    }
	                } catch(ExpressionEvaluationException e) {
	                    //ignore for now, we don't have the context which could be the problem
	                }
	            }
            } catch (TeiidComponentException e) {
            	handleException(e);
            }
		}
    }

	private void validateAssignment(LanguageObject obj,
			ElementSymbol variable) {
		String groupName = variable.getGroupSymbol().getCanonicalName();
		//This will actually get detected by the resolver, since we inject an automatic declaration.
    	if(groupName.equals(ProcedureReservedWords.CHANGING) || groupName.equals(ProcedureReservedWords.INPUTS)) {
			handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0012", ProcedureReservedWords.INPUTS, ProcedureReservedWords.CHANGING), obj); //$NON-NLS-1$
		}
	}
    
    @Override
    public void visit(ScalarSubquery obj) {
    	validateSubquery(obj);
        Collection<SingleElementSymbol> projSymbols = obj.getCommand().getProjectedSymbols();

        //Scalar subquery should have one projected symbol (query with one expression
        //in SELECT or stored procedure execution that returns a single value).
        if(projSymbols.size() != 1) {
        	handleValidationError(QueryPlugin.Util.getString("ERR.015.008.0032", obj.getCommand()), obj.getCommand()); //$NON-NLS-1$
        }
    }

    public void visit(CreateUpdateProcedureCommand obj) {
        if(!obj.isUpdateProcedure()){
        	
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
        validateAssignment(obj, obj.getVariable());
    }
    
    @Override
    public void visit(HasCriteria obj) {
    	if (this.updateProc == null || !this.updateProc.isUpdateProcedure()) {
			handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0019"), obj); //$NON-NLS-1$
    	}
    }
    
    public void visit(TranslateCriteria obj) {
    	if (this.updateProc == null || !this.updateProc.isUpdateProcedure()) {
			handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0019"), obj); //$NON-NLS-1$
    	}
		if(obj.hasTranslations()) {
			Collection selectElmnts = null;
			if(obj.getSelector().hasElements()) {
				selectElmnts = obj.getSelector().getElements();
			}
			Iterator critIter = obj.getTranslations().iterator();
			while(critIter.hasNext()) {
				CompareCriteria transCrit = (CompareCriteria) critIter.next();
				Collection<ElementSymbol> leftElmnts = ElementCollectorVisitor.getElements(transCrit.getLeftExpression(), true);
				// there is always only one element
				ElementSymbol leftExpr = leftElmnts.iterator().next();

				if(selectElmnts != null && !selectElmnts.contains(leftExpr)) {
					handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0021"), leftExpr); //$NON-NLS-1$
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
            List<Function> rowLimitFunctions = new ArrayList<Function>();
            FunctionCollectorVisitor visitor = new FunctionCollectorVisitor(rowLimitFunctions, FunctionLibrary.ROWLIMIT);
            PreOrderNavigator.doVisit(obj, visitor); 
            visitor = new FunctionCollectorVisitor(rowLimitFunctions, FunctionLibrary.ROWLIMITEXCEPTION);
            PreOrderNavigator.doVisit(obj, visitor);
            final int functionCount = rowLimitFunctions.size();
            if (functionCount > 0) {
                
                // Verify each use of rowlimit function is in a compare criteria that is 
                // entirely it's own conjunct
                Iterator<Criteria> conjunctIter = Criteria.separateCriteriaByAnd(obj).iterator();            
                
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
		Command userCommand = this.updateProc.getUserCommand();
    	// modeler validation
    	if(userCommand == null) {
    		return;
    	}
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

    	Collection<ElementSymbol> transleElmnts = ElementCollectorVisitor.getElements(obj, true);
    	Collection<GroupSymbol> groups = GroupCollectorVisitor.getGroups(this.currentCommand, true);
		int selectType = obj.getSelector().getSelectorType();

		for (Criteria predCrit : Criteria.separateCriteriaByAnd(userCrit)) {
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
	    	Iterator<ElementSymbol> critEmlntIter = ElementCollectorVisitor.getElements(predCrit, true).iterator();
	    	// collect all elements elements on the criteria map to
	    	while(critEmlntIter.hasNext()) {
	    		ElementSymbol criteriaElement = critEmlntIter.next();
	    		if(transleElmnts.contains(criteriaElement)) {
		    		Expression mappedExpr = (Expression) symbolMap.get(criteriaElement);
		    		if(mappedExpr instanceof AggregateSymbol) {
						handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0022", criteriaElement), criteriaElement); //$NON-NLS-1$
		    		}

		    		if (!groups.containsAll(GroupsUsedByElementsVisitor.getGroups(mappedExpr))) {
						handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0023", criteriaElement), criteriaElement); //$NON-NLS-1$
			    	}
				}
	    	}
		}
    }

    protected void validateSelectElements(Select obj) {
    	if(isXML) {
    		return;
    	}

        Collection<ElementSymbol> elements = ElementCollectorVisitor.getElements(obj, true);
        
        Collection<ElementSymbol> cantSelect = validateElementsSupport(
            elements,
            SupportConstants.Element.SELECT );

		if(cantSelect != null) {
            handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0024", cantSelect), cantSelect); //$NON-NLS-1$
		}
    }

    protected void validateHasProjectedSymbols(Command obj) {
        if(obj.getProjectedSymbols().size() == 0) {
            handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0025"), obj); //$NON-NLS-1$
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
            validateSortable(symbol);
        }
    }

	private void validateSortable(SingleElementSymbol symbol) {
		if (isNonComparable(symbol)) {
		    handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0026", symbol), symbol); //$NON-NLS-1$
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
            handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0029"), obj); //$NON-NLS-1$
     	}
    }

	/**
	 * This method can be used to validate commands used in the stored
	 * procedure languge cannot be executed against XML documents.
	 */
    protected void validateNoXMLProcedures(Command obj) {
     	if(isXMLCommand(obj)) {
            handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0030"), obj); //$NON-NLS-1$
     	}
    }

    private void validateXMLQuery(Query obj) {
        if(obj.getGroupBy() != null) {
            handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0031"), obj); //$NON-NLS-1$
        }
        if(obj.getHaving() != null) {
            handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0032"), obj); //$NON-NLS-1$
        }
        if(obj.getLimit() != null) {
            handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.limit_not_valid_for_xml"), obj); //$NON-NLS-1$
        }
        if (obj.getOrderBy() != null) {
        	OrderBy orderBy = obj.getOrderBy();
        	for (OrderByItem item : orderBy.getOrderByItems()) {
				if (!(item.getSymbol() instanceof ElementSymbol)) {
					handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.orderby_expression_xml"), obj); //$NON-NLS-1$
				}
			}
         }
    }
    
    protected void validateGroupSupportsUpdate(GroupSymbol groupSymbol) {
    	try {
	    	if(! getMetadata().groupSupports(groupSymbol.getMetadataID(), SupportConstants.Group.UPDATE)) {
	            handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0033", SQLStringVisitor.getSQLString(groupSymbol)), groupSymbol); //$NON-NLS-1$
	        }
	    } catch (TeiidComponentException e) {
	        handleException(e, groupSymbol);
	    }
    }
    
    protected void validateSetQuery(SetQuery query) {
        // Walk through sub queries - validate each one separately and
        // also check the columns of each for comparability
        for (QueryCommand subQuery : query.getQueryCommands()) {
            if(isXMLCommand(subQuery)) {
                handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0034"), query); //$NON-NLS-1$
            }
            if (subQuery instanceof Query && ((Query)subQuery).getInto() != null) {
            	handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.union_insert"), query); //$NON-NLS-1$
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
            List<SingleElementSymbol> projectedSymbols = select.getProjectedSymbols();
            for (SingleElementSymbol symbol : projectedSymbols) {
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
                    handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0052", insertElem), insertElem); //$NON-NLS-1$
                }
            }

            // Get elements in the group.
    		Collection<ElementSymbol> insertElmnts = new LinkedList<ElementSymbol>(ResolverUtil.resolveElementsInGroup(insertGroup, getMetadata()));

    		// remove all elements specified in insert to get the ignored elements
    		insertElmnts.removeAll(vars);

    		for (ElementSymbol nextElmnt : insertElmnts) {
				if(!getMetadata().elementSupports(nextElmnt.getMetadataID(), SupportConstants.Element.DEFAULT_VALUE) &&
					!getMetadata().elementSupports(nextElmnt.getMetadataID(), SupportConstants.Element.NULL) &&
                    !getMetadata().elementSupports(nextElmnt.getMetadataID(), SupportConstants.Element.AUTO_INCREMENT) &&
                     !getMetadata().isMultiSourceElement(nextElmnt.getMetadataID())) {
		                handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0053", new Object[] {insertGroup, nextElmnt}), nextElmnt); //$NON-NLS-1$
				}
			}

            //check to see if the elements support nulls in metadata,
            // if any of the value present in the insert are null
            while(valIter.hasNext() && varIter.hasNext()) {
                Expression nextValue = (Expression) valIter.next();
                ElementSymbol nextVar = (ElementSymbol) varIter.next();

                if (!(nextValue instanceof Constant) && getMetadata().isMultiSourceElement(nextVar.getMetadataID())) {
                	handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.multisource_constant", nextVar), nextVar); //$NON-NLS-1$
                	continue;
                }
                
                if (EvaluatableVisitor.isFullyEvaluatable(nextValue, true)) {
                    try {
                        // If nextValue is an expression, evaluate it before checking for null
                        Object evaluatedValue = Evaluator.evaluate(nextValue);
                        if(evaluatedValue == null && ! getMetadata().elementSupports(nextVar.getMetadataID(), SupportConstants.Element.NULL)) {
                            handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0055", nextVar), nextVar); //$NON-NLS-1$
                        }
                    } catch(ExpressionEvaluationException e) {
                        //ignore for now, we don't have the context which could be the problem
                    }
                }
            }// end of while
        } catch(TeiidComponentException e) {
            handleException(e, obj);
        } 
    }
    
    protected void validateSetClauseList(SetClauseList list) {
    	Set<ElementSymbol> dups = new HashSet<ElementSymbol>();
	    HashSet<ElementSymbol> changeVars = new HashSet<ElementSymbol>();
	    for (SetClause clause : list.getClauses()) {
	    	ElementSymbol elementID = clause.getSymbol();
	        if (!changeVars.add(elementID)) {
	        	dups.add(elementID);
	        }
		}
	    if(!dups.isEmpty()) {
            handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0062", dups), dups); //$NON-NLS-1$
	    }
    }
    
    protected void validateUpdate(Update update) {
        try {
            UpdateInfo info = update.getUpdateInfo();

            // list of elements that are being updated
		    for (SetClause entry : update.getChangeList().getClauses()) {
        	    ElementSymbol elementID = entry.getSymbol();

                // Check that left side element is updatable
                if(! getMetadata().elementSupports(elementID.getMetadataID(), SupportConstants.Element.UPDATE)) {
                    handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0059", elementID), elementID); //$NON-NLS-1$
                }
                
                Object metadataID = elementID.getMetadataID();
                if (getMetadata().isMultiSourceElement(metadataID)){
                	handleValidationError(QueryPlugin.Util.getString("multi_source_update_not_allowed", elementID), elementID); //$NON-NLS-1$
                }

			    // Check that right expression is a constant and is non-null
                Expression value = entry.getValue();
                
                if (EvaluatableVisitor.isFullyEvaluatable(value, true)) {
                    try {
                        value = new Constant(Evaluator.evaluate(value));
                    } catch (ExpressionEvaluationException err) {
                    }
                }
                
                if(value instanceof Constant) {
    			    // If value is null, check that element supports this as a nullable column
                    if(((Constant)value).isNull() && ! getMetadata().elementSupports(elementID.getMetadataID(), SupportConstants.Element.NULL)) {
                        handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0060", SQLStringVisitor.getSQLString(elementID)), elementID); //$NON-NLS-1$
                    }// end of if
                } else if (info != null && info.getUpdateType() == UpdateType.UPDATE_PROCEDURE && getMetadata().isVirtualGroup(update.getGroup().getMetadataID()) && !EvaluatableVisitor.willBecomeConstant(value)) {
                    // If this is an update on a virtual group, verify that no elements are in the right side
                    Collection<ElementSymbol> elements = ElementCollectorVisitor.getElements(value, false);
                    for (ElementSymbol element : elements) {
                        if(! element.isExternalReference()) {
                            handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0061", SQLStringVisitor.getSQLString(value)), value); //$NON-NLS-1$
                        }
                    }
                } 
		    }
            if (info != null && info.isInherentUpdate()) {
            	Set<ElementSymbol> updateCols = update.getChangeList().getClauseMap().keySet();
            	if (!info.hasValidUpdateMapping(updateCols)) {
            		handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.nonUpdatable", updateCols), update); //$NON-NLS-1$
            	}
            }
        } catch(TeiidException e) {
            handleException(e, update);
        }
        
        validateSetClauseList(update.getChangeList());
    }
    
    /**
     * Validates SELECT INTO queries.
     * @param query
     * @since 4.2
     */
    protected void validateSelectInto(Query query) {
        List<SingleElementSymbol> symbols = query.getSelect().getProjectedSymbols();
        GroupSymbol intoGroup = query.getInto().getGroup();
        validateInto(query, symbols, intoGroup);
    }

    private void validateInto(LanguageObject query,
                                List<SingleElementSymbol> symbols,
                                GroupSymbol intoGroup) {
        try {
            List elementIDs = getMetadata().getElementIDsInGroupID(intoGroup.getMetadataID());
            
            // Check if there are too many elements in the SELECT clause
            if (symbols.size() != elementIDs.size()) {
                handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.select_into_wrong_elements", new Object[] {new Integer(elementIDs.size()), new Integer(symbols.size())}), query); //$NON-NLS-1$
                return;
            }
            
            for (int symbolNum = 0; symbolNum < symbols.size(); symbolNum++) {
                SingleElementSymbol symbol = symbols.get(symbolNum);
                Object elementID = elementIDs.get(symbolNum);
                // Check if supports updates
                if (!getMetadata().elementSupports(elementID, SupportConstants.Element.UPDATE)) {
                    handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.element_updates_not_allowed", getMetadata().getFullName(elementID)), intoGroup); //$NON-NLS-1$
                }

                Class<?> symbolType = symbol.getType();
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
        } catch (TeiidComponentException e) {
            handleException(e, query);
        } 
    }
    
    /**
     * Validate that the command assigns a value to the ROWS_UPDATED variable 
     * @param obj
     * @since 4.2
     */
    protected void validateContainsRowsUpdatedVariable(CreateUpdateProcedureCommand obj) {
        final Collection<ElementSymbol> assignVars = new ArrayList<ElementSymbol>();
       // Use visitor to find assignment statements
        LanguageVisitor visitor = new LanguageVisitor() {
            public void visit(AssignmentStatement stmt) {
                assignVars.add(stmt.getVariable());
            }
        };
        PreOrderNavigator.doVisit(obj, visitor);
        boolean foundVar = false;
        for (ElementSymbol variable : assignVars) {
            if(variable.getShortName().equalsIgnoreCase(ProcedureReservedWords.ROWS_UPDATED)) {
                foundVar = true;
                break;
            }
        }
        if(!foundVar) {
            handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0016", ProcedureReservedWords.ROWS_UPDATED), obj); //$NON-NLS-1$
        }
    }
    
    private void validateRowLimitFunctionNotInInvalidCriteria(Criteria obj) {
        // Collect all occurrances of rowlimit and rowlimitexception functions
        List<Function> rowLimitFunctions = new ArrayList<Function>();
        FunctionCollectorVisitor visitor = new FunctionCollectorVisitor(rowLimitFunctions, FunctionLibrary.ROWLIMIT);
        PreOrderNavigator.doVisit(obj, visitor);      
        visitor = new FunctionCollectorVisitor(rowLimitFunctions, FunctionLibrary.ROWLIMITEXCEPTION);
        PreOrderNavigator.doVisit(obj, visitor); 
        if (rowLimitFunctions.size() > 0) {
            handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.3"), obj); //$NON-NLS-1$
        }
    }
    
    /** 
     * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.lang.BetweenCriteria)
     * @since 4.3
     */
    public void visit(BetweenCriteria obj) {
    	if (isNonComparable(obj.getExpression())) {
    		handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0027", obj),obj);    		 //$NON-NLS-1$
    	}
        this.validateRowLimitFunctionNotInInvalidCriteria(obj);
    }

    /** 
     * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.lang.IsNullCriteria)
     * @since 4.3
     */
    public void visit(IsNullCriteria obj) {
        this.validateRowLimitFunctionNotInInvalidCriteria(obj);
    }

    /** 
     * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.lang.MatchCriteria)
     * @since 4.3
     */
    public void visit(MatchCriteria obj) {
        this.validateRowLimitFunctionNotInInvalidCriteria(obj);
    }

    /** 
     * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.lang.NotCriteria)
     * @since 4.3
     */
    public void visit(NotCriteria obj) {
        this.validateRowLimitFunctionNotInInvalidCriteria(obj);
    }

    /** 
     * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.lang.SetCriteria)
     * @since 4.3
     */
    public void visit(SetCriteria obj) {
    	if (isNonComparable(obj.getExpression())) {
    		handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0027", obj),obj);    		 //$NON-NLS-1$
    	}
        this.validateRowLimitFunctionNotInInvalidCriteria(obj);
    }

    /** 
     * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.lang.SubqueryCompareCriteria)
     * @since 4.3
     */
    public void visit(SubqueryCompareCriteria obj) {
    	validateSubquery(obj);
    	if (isNonComparable(obj.getLeftExpression())) {
    		handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0027", obj),obj);    		 //$NON-NLS-1$
    	}
        this.validateRowLimitFunctionNotInInvalidCriteria(obj);
    }
    
    public void visit(Option obj) {
        List<String> dep = obj.getDependentGroups();
        List<String> notDep = obj.getNotDependentGroups();
        if (dep != null && !dep.isEmpty()
            && notDep != null && !notDep.isEmpty()) {
            String groupName = null;
            String notDepGroup = null;
            for (Iterator<String> i = dep.iterator(); i.hasNext();) {
                groupName = i.next();
                for (Iterator<String> j = notDep.iterator(); j.hasNext();) {
                    notDepGroup = j.next();
                    if (notDepGroup.equalsIgnoreCase(groupName)) {
                        handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.group_in_both_dep", groupName), obj); //$NON-NLS-1$
                        return;
                    }
                }
            }
        }
    }
    
    /** 
     * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.lang.DynamicCommand)
     */
    public void visit(DynamicCommand obj) {
        if (obj.getIntoGroup() != null) {
            validateInto(obj, obj.getAsColumns(), obj.getIntoGroup());
        }
        if (obj.getUsing() != null) {
        	validateSetClauseList(obj.getUsing());
        }
    }
    
    @Override
    public void visit(Create obj) {
    	if (!obj.getPrimaryKey().isEmpty()) {
    		validateSortable(obj.getPrimaryKey());
    	}
    }
    
    /** 
     * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.lang.Drop)
     */
    public void visit(Drop drop) {
        if (!drop.getTable().isTempTable()) {
            handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.drop_of_nontemptable", drop.getTable()), drop); //$NON-NLS-1$
        }
    }
    
    @Override
    public void visit(CompareCriteria obj) {
    	if (isNonComparable(obj.getLeftExpression())) {
    		handleValidationError(QueryPlugin.Util.getString("ERR.015.012.0027", obj),obj);    		 //$NON-NLS-1$
    	}
    	
        // Validate use of 'rowlimit' and 'rowlimitexception' pseudo-functions - they cannot be nested within another
        // function, and their operands must be a nonnegative integers

        // Collect all occurrences of rowlimit function
        List rowLimitFunctions = new ArrayList();
        FunctionCollectorVisitor visitor = new FunctionCollectorVisitor(rowLimitFunctions, FunctionLibrary.ROWLIMIT);
        PreOrderNavigator.doVisit(obj, visitor);   
        visitor = new FunctionCollectorVisitor(rowLimitFunctions, FunctionLibrary.ROWLIMITEXCEPTION);
        PreOrderNavigator.doVisit(obj, visitor);            
        final int functionCount = rowLimitFunctions.size();
        if (functionCount > 0) {
            Function function = null;
            Expression expr = null;
            if (obj.getLeftExpression() instanceof Function) {
                Function leftExpr = (Function)obj.getLeftExpression();
                if (leftExpr.getFunctionDescriptor().getName().equalsIgnoreCase(FunctionLibrary.ROWLIMIT) ||
                    leftExpr.getFunctionDescriptor().getName().equalsIgnoreCase(FunctionLibrary.ROWLIMITEXCEPTION)) {
                    function = leftExpr;
                    expr = obj.getRightExpression();
                }
            } 
            if (function == null && obj.getRightExpression() instanceof Function) {
                Function rightExpr = (Function)obj.getRightExpression();
                if (rightExpr.getFunctionDescriptor().getName().equalsIgnoreCase(FunctionLibrary.ROWLIMIT) ||
                    rightExpr.getFunctionDescriptor().getName().equalsIgnoreCase(FunctionLibrary.ROWLIMITEXCEPTION)) {
                    function = rightExpr;
                    expr = obj.getLeftExpression();
                }
            }
            if (function == null) {
                // must be nested, which is invalid
                handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.0"), obj); //$NON-NLS-1$
            } else {
                if (expr instanceof Constant) {
                    Constant constant = (Constant)expr;
                    if (constant.getValue() instanceof Integer) {
                        Integer integer = (Integer)constant.getValue();
                        if (integer.intValue() < 0) {
                            handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.1"), obj); //$NON-NLS-1$
                        }
                    } else {
                        handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.1"), obj); //$NON-NLS-1$
                    }
                } else if (expr instanceof Reference) {
                	((Reference)expr).setConstraint(new PositiveIntegerConstraint("ValidationVisitor.1")); //$NON-NLS-1$
                } else {
                    handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.1"), obj); //$NON-NLS-1$
                }
            }                 
        }
    }
    
    public void visit(Limit obj) {
        Expression offsetExpr = obj.getOffset();
        if (offsetExpr instanceof Constant) {
            Integer offset = (Integer)((Constant)offsetExpr).getValue();
            if (offset.intValue() < 0) {
                handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.badoffset2"), obj); //$NON-NLS-1$
            }
        } else if (offsetExpr instanceof Reference) {
        	((Reference)offsetExpr).setConstraint(new PositiveIntegerConstraint("ValidationVisitor.badoffset2")); //$NON-NLS-1$
        }
        Expression limitExpr = obj.getRowLimit();
        if (limitExpr instanceof Constant) {
            Integer limit = (Integer)((Constant)limitExpr).getValue();
            if (limit.intValue() < 0) {
                handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.badlimit2"), obj); //$NON-NLS-1$
            }
        } else if (limitExpr instanceof Reference) {
        	((Reference)limitExpr).setConstraint(new PositiveIntegerConstraint("ValidationVisitor.badlimit2")); //$NON-NLS-1$
        }
    }
    
    @Override
    public void visit(XMLForest obj) {
    	validateDerivedColumnNames(obj, obj.getArgs());
    	for (DerivedColumn dc : obj.getArgs()) {
			if (dc.getAlias() == null) {
				continue;
			}
			validateQName(obj, dc.getAlias());
			validateXMLContentTypes(dc.getExpression(), obj);
		}
    }
    
    @Override
    public void visit(AggregateSymbol obj) {
    	if (obj.getAggregateFunction() != Type.TEXTAGG) {
    		return;
    	}
    	TextLine tl = (TextLine)obj.getExpression();
    	if (tl.isIncludeHeader()) {
    		validateDerivedColumnNames(obj, tl.getExpressions());
    	}
    	for (DerivedColumn dc : tl.getExpressions()) {
			validateXMLContentTypes(dc.getExpression(), obj);
		}
    	validateTextOptions(obj, tl.getDelimiter(), tl.getQuote());
    	if (tl.getEncoding() != null) {
    		try {
    			Charset.forName(tl.getEncoding());
    		} catch (IllegalArgumentException e) {
    			handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.invalid_encoding", tl.getEncoding()), obj); //$NON-NLS-1$
    		}
    	}
    }
    
	private String[] validateQName(LanguageObject obj, String name) {
		try {
			return Name11Checker.getInstance().getQNameParts(name);
		} catch (QNameException e) {
			handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.xml_invalid_qname", name), obj); //$NON-NLS-1$
		}
		return null;
	}

	private void validateDerivedColumnNames(LanguageObject obj, List<DerivedColumn> cols) {
		for (DerivedColumn dc : cols) {
    		if (dc.getAlias() == null && !(dc.getExpression() instanceof ElementSymbol)) {
    			handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.expression_requires_name"), obj); //$NON-NLS-1$
        	} 
		}
	}
    
    @Override
    public void visit(XMLAttributes obj) {
    	validateDerivedColumnNames(obj, obj.getArgs());
    	for (DerivedColumn dc : obj.getArgs()) {
			if (dc.getAlias() == null) {
				continue;
			}
			if ("xmlns".equals(dc.getAlias())) { //$NON-NLS-1$
				handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.xml_attributes_reserved"), obj); //$NON-NLS-1$
			}
			String[] parts = validateQName(obj, dc.getAlias());
			if (parts == null) {
				continue;
			}
			if ("xmlns".equals(parts[0])) { //$NON-NLS-1$
				handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.xml_attributes_reserved", dc.getAlias()), obj); //$NON-NLS-1$
			}
		}
    }
    
    @Override
    public void visit(XMLElement obj) {
    	for (Expression expression : obj.getContent()) {
    		validateXMLContentTypes(expression, obj);
    	}
    	validateQName(obj, obj.getName());
    }
    
    public void validateXMLContentTypes(Expression expression, LanguageObject parent) {
		if (expression.getType() == DataTypeManager.DefaultDataClasses.OBJECT || expression.getType() == DataTypeManager.DefaultDataClasses.BLOB) {
			handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.xml_content_type", expression), parent); //$NON-NLS-1$
		}
    }
    
    @Override
    public void visit(QueryString obj) {
    	validateDerivedColumnNames(obj, obj.getArgs());
    }
    
    @Override
    public void visit(XMLTable obj) {
    	List<DerivedColumn> passing = obj.getPassing();
    	validatePassing(obj, obj.getXQueryExpression(), passing);
    	boolean hasOrdinal = false;
    	for (XMLColumn xc : obj.getColumns()) {
			if (!xc.isOrdinal()) {
				if (xc.getDefaultExpression() != null && !EvaluatableVisitor.isFullyEvaluatable(obj, false)) {
					handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.invalid_default", xc.getDefaultExpression()), obj); //$NON-NLS-1$
				}
				continue;
			}
			if (hasOrdinal) {
				handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.one_ordinal"), obj); //$NON-NLS-1$
				break;
			}
			hasOrdinal = true;
		}
    }
    
    @Override
    public void visit(XMLQuery obj) {
    	validatePassing(obj, obj.getXQueryExpression(), obj.getPassing());
    }

	private void validatePassing(LanguageObject obj, SaxonXQueryExpression xqe, List<DerivedColumn> passing) {
		boolean context = false;
    	boolean hadError = false;
    	HashSet<String> names = new HashSet<String>();
    	for (DerivedColumn dc : passing) {
    		if (dc.getAlias() == null) {
    			Class<?> type = dc.getExpression().getType();
    			if (type != DataTypeManager.DefaultDataClasses.XML) {
    				handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.context_item_type"), obj); //$NON-NLS-1$
    			}
    			if (context && !hadError) {
    				handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.passing_requires_name"), obj); //$NON-NLS-1$
    				hadError = true;
    			}
    			context = true;
        	} else { 
        		validateXMLContentTypes(dc.getExpression(), obj);
        		if (!names.add(dc.getAlias().toUpperCase())) {
        			handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.duplicate_passing", dc.getAlias()), obj); //$NON-NLS-1$
        		}
        	}
		}
    	if (xqe.usesContextItem() && !context) {
			handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.context_required"), obj); //$NON-NLS-1$    		
    	}
	}
    
    @Override
    public void visit(XMLNamespaces obj) {
    	boolean hasDefault = false;
    	for (XMLNamespaces.NamespaceItem item : obj.getNamespaceItems()) {
			if (item.getPrefix() != null) {
				if (item.getPrefix().equals("xml") || item.getPrefix().equals("xmlns")) { //$NON-NLS-1$ //$NON-NLS-2$
					handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.xml_namespaces_reserved"), obj); //$NON-NLS-1$
				}
				if (item.getUri().length() == 0) {
					handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.xml_namespaces_null_uri"), obj); //$NON-NLS-1$
				}
				continue;
			}
			if (hasDefault) {
				handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.xml_namespaces"), obj); //$NON-NLS-1$
				break;
			}
			hasDefault = true;
		}
    }
    
    @Override
    public void visit(TextTable obj) {
    	boolean widthSet = false;
    	Character delimiter = null;
    	Character quote = null;
    	for (TextTable.TextColumn column : obj.getColumns()) {
			if (column.getWidth() != null) {
				widthSet = true;
				if (column.getWidth() < 0) {
					handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.text_table_negative"), obj); //$NON-NLS-1$
				}
			} else if (widthSet) {
    			handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.text_table_invalid_width"), obj); //$NON-NLS-1$
			}
		}
    	if (widthSet) {
    		if (obj.getDelimiter() != null || obj.getHeader() != null || obj.getQuote() != null) {
        		handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.text_table_width"), obj); //$NON-NLS-1$
    		}
    	} else {
        	if (obj.getHeader() != null && obj.getHeader() < 0) {
	    		handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.text_table_negative"), obj); //$NON-NLS-1$
	    	}
    		delimiter = obj.getDelimiter();
    		quote = obj.getQuote();
			validateTextOptions(obj, delimiter, quote);
    	}
    	if (obj.getSkip() != null && obj.getSkip() < 0) {
    		handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.text_table_negative"), obj); //$NON-NLS-1$
    	}
    }

	private void validateTextOptions(LanguageObject obj, Character delimiter,
			Character quote) {
		if (quote == null) {
			quote = '"';
		} 
		if (delimiter == null) {
			delimiter = ',';
		}
		if (EquivalenceUtil.areEqual(quote, delimiter)) {
			handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.text_table_delimiter"), obj); //$NON-NLS-1$
		}
		if (EquivalenceUtil.areEqual(quote, '\n') 
				|| EquivalenceUtil.areEqual(delimiter, '\n')) {
			handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.text_table_newline"), obj); //$NON-NLS-1$
		}
	}
    
    @Override
    public void visit(XMLParse obj) {
    	if (obj.getExpression().getType() != DataTypeManager.DefaultDataClasses.STRING && 
    			obj.getExpression().getType() != DataTypeManager.DefaultDataClasses.CLOB &&
    			obj.getExpression().getType() != DataTypeManager.DefaultDataClasses.BLOB) {
    		handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.xmlparse_type"), obj); //$NON-NLS-1$
    	}
    }
    
    @Override
    public void visit(ExistsCriteria obj) {
    	validateSubquery(obj);
    }
    
    @Override
    public void visit(SubqueryFromClause obj) {
    	validateSubquery(obj);
    }
    
    @Override
    public void visit(LoopStatement obj) {
    	validateSubquery(obj);
    }
    
    @Override
    public void visit(WithQueryCommand obj) {
    	validateSubquery(obj);
    }
    
    public void visit(AlterView obj) {
    	try {
			QueryResolver.validateProjectedSymbols(obj.getTarget(), getMetadata(), obj.getDefinition());
		} catch (QueryValidatorException e) {
			handleValidationError(e.getMessage(), obj.getDefinition());
		} catch (TeiidComponentException e) {
			handleException(e);
		}
    }

    @Override
    public void visit(AlterProcedure obj) {
    	GroupSymbol gs = obj.getTarget();
    	try {
	    	if (!gs.isProcedure() || !getMetadata().isVirtualModel(getMetadata().getModelID(gs.getMetadataID()))) {
	    		handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.not_a_procedure", gs), gs); //$NON-NLS-1$
	    		return;
	    	}
	    	StoredProcedureInfo info = getMetadata().getStoredProcedureInfoForProcedure(gs.getName());
	    	for (SPParameter param : info.getParameters()) {
	    		if (param.getParameterType() == SPParameter.RESULT_SET) {
	    	    	QueryResolver.validateProjectedSymbols(gs, param.getResultSetColumns(), obj.getDefinition().getProjectedSymbols());
	    	    	break;
	    		}
	    	}
    	} catch (QueryValidatorException e) {
			handleValidationError(e.getMessage(), obj.getDefinition().getBlock());
    	} catch (TeiidComponentException e) {
			handleException(e);
		}
    }
    
    @Override
    public void visit(AlterTrigger obj) {
    	validateGroupSupportsUpdate(obj.getTarget());
    }

    //TODO: it may be simpler to catch this in the parser
    private void validateSubquery(SubqueryContainer<?> subQuery) {
    	if (subQuery.getCommand() instanceof Query && ((Query)subQuery.getCommand()).getInto() != null) {
        	handleValidationError(QueryPlugin.Util.getString("ValidationVisitor.subquery_insert"), subQuery.getCommand()); //$NON-NLS-1$
        }
    }
    
}
