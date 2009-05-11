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

package com.metamatrix.query.sql.visitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.ReservedWords;
import com.metamatrix.query.sql.lang.BetweenCriteria;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.CompoundCriteria;
import com.metamatrix.query.sql.lang.Create;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.Delete;
import com.metamatrix.query.sql.lang.DependentSetCriteria;
import com.metamatrix.query.sql.lang.Drop;
import com.metamatrix.query.sql.lang.DynamicCommand;
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
import com.metamatrix.query.sql.lang.PredicateCriteria;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.QueryCommand;
import com.metamatrix.query.sql.lang.SPParameter;
import com.metamatrix.query.sql.lang.Select;
import com.metamatrix.query.sql.lang.SetClause;
import com.metamatrix.query.sql.lang.SetClauseList;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.lang.SubqueryCompareCriteria;
import com.metamatrix.query.sql.lang.SubqueryFromClause;
import com.metamatrix.query.sql.lang.SubquerySetCriteria;
import com.metamatrix.query.sql.lang.UnaryFromClause;
import com.metamatrix.query.sql.lang.Update;
import com.metamatrix.query.sql.lang.XQuery;
import com.metamatrix.query.sql.proc.AssignmentStatement;
import com.metamatrix.query.sql.proc.Block;
import com.metamatrix.query.sql.proc.BreakStatement;
import com.metamatrix.query.sql.proc.CommandStatement;
import com.metamatrix.query.sql.proc.ContinueStatement;
import com.metamatrix.query.sql.proc.CreateUpdateProcedureCommand;
import com.metamatrix.query.sql.proc.CriteriaSelector;
import com.metamatrix.query.sql.proc.DeclareStatement;
import com.metamatrix.query.sql.proc.HasCriteria;
import com.metamatrix.query.sql.proc.IfStatement;
import com.metamatrix.query.sql.proc.LoopStatement;
import com.metamatrix.query.sql.proc.RaiseErrorStatement;
import com.metamatrix.query.sql.proc.Statement;
import com.metamatrix.query.sql.proc.TranslateCriteria;
import com.metamatrix.query.sql.proc.WhileStatement;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.AliasSymbol;
import com.metamatrix.query.sql.symbol.AllInGroupSymbol;
import com.metamatrix.query.sql.symbol.AllSymbol;
import com.metamatrix.query.sql.symbol.CaseExpression;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.ExpressionSymbol;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.symbol.ScalarSubquery;
import com.metamatrix.query.sql.symbol.SearchedCaseExpression;
import com.metamatrix.query.sql.symbol.SelectSymbol;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * <p>The SQLStringVisitor will visit a set of language objects and return the
 * corresponding SQL string representation. </p>
 */
public class SQLStringVisitor extends LanguageVisitor {

    public static final String UNDEFINED = "<undefined>"; //$NON-NLS-1$
    private static final String SPACE = " "; //$NON-NLS-1$
    private static final String BEGIN_COMMENT = "/*"; //$NON-NLS-1$
    private static final String END_COMMENT = "*/"; //$NON-NLS-1$
    
    private LinkedList<Object> parts = new LinkedList<Object>();

    /**
     * Helper to quickly get the parser string for an object using the visitor.
     * @param obj Language object
     * @return String SQL String for obj
     */
    public static final String getSQLString(LanguageObject obj) {
        if(obj == null) {
            return UNDEFINED; 
        }
        SQLStringVisitor visitor = new SQLStringVisitor();       
        obj.acceptVisitor(visitor);
    	return visitor.getSQLString();
    }

    /**
     * Retrieve completed string from the visitor.
     * @return Complete SQL string for the visited nodes
     */
    public String getSQLString() {
        StringBuffer output = new StringBuffer();
        getSQLString(this.parts, output);
        return output.toString();
    }
    
    public static void getSQLString(List<Object> parts, StringBuffer output) {
        for (Object object : parts) {
            if (object instanceof List) {
                getSQLString((List<Object>)object, output);
            } else {
                output.append(object);
            }
        } 
    }
    
    public List<Object> registerNode(LanguageObject obj) {
        if (obj == null) {
            return Arrays.asList((Object)UNDEFINED);
        }
        SQLStringVisitor visitor = new SQLStringVisitor();       
        obj.acceptVisitor(visitor);
        return visitor.parts;
    }
    
    public void replaceStringParts(Object[] parts) {
        for (int i = 0; i < parts.length; i++) {
            this.parts.add(parts[i]);
        } 
    }

    // ############ Visitor methods for language objects ####################

    public void visit(BetweenCriteria obj) {
        parts.add(registerNode(obj.getExpression()));
        parts.add(SPACE);
        
        if (obj.isNegated()) {
            parts.add(ReservedWords.NOT);
            parts.add(SPACE);
        }
        parts.add(ReservedWords.BETWEEN);
        parts.add(SPACE);
        parts.add(registerNode(obj.getLowerExpression()));

        parts.add(SPACE);
        parts.add(ReservedWords.AND);
        parts.add(SPACE);
        parts.add(registerNode(obj.getUpperExpression()));
    }

    public void visit(CaseExpression obj) {
        parts.add(ReservedWords.CASE);
        parts.add(SPACE);
        parts.add(registerNode(obj.getExpression()) ); 
        parts.add(SPACE);

        for (int i = 0; i < obj.getWhenCount(); i++) {
            parts.add(ReservedWords.WHEN);
            parts.add(SPACE);
            parts.add(registerNode(obj.getWhenExpression(i)) );
            parts.add(SPACE);
            parts.add(ReservedWords.THEN);
            parts.add(SPACE);
            parts.add(registerNode(obj.getThenExpression(i)));
            parts.add(SPACE);
        }

        if (obj.getElseExpression() != null) {
            parts.add(ReservedWords.ELSE);
            parts.add(SPACE);
            parts.add(registerNode(obj.getElseExpression()));
            parts.add(SPACE);
        }
        parts.add(ReservedWords.END);
    }

    public void visit(CompareCriteria obj) {
        Expression leftExpression = obj.getLeftExpression();
        Object leftPart = registerNode(leftExpression);

        String operator = obj.getOperatorAsString();

        Expression rightExpression = obj.getRightExpression();
        Object rightPart = registerNode(rightExpression);

        replaceStringParts(new Object[] { leftPart, SPACE, operator, SPACE, rightPart });
    }

    public void visit(CompoundCriteria obj) {
        // Get operator string
        int operator = obj.getOperator();
        String operatorStr = ""; //$NON-NLS-1$
        if(operator == CompoundCriteria.AND) {
            operatorStr = ReservedWords.AND;
        } else if(operator == CompoundCriteria.OR) {
            operatorStr = ReservedWords.OR;
        }

        // Get criteria
        List subCriteria = obj.getCriteria();

        // Build parts
        if(subCriteria.size() == 1) {
            // Special case - should really never happen, but we are tolerant
            Criteria firstChild = (Criteria) subCriteria.get(0);
            replaceStringParts(new Object[] { registerNode(firstChild) });
        } else {
            // Magic formula - suppose you have 2 sub criteria, then the string
            // has parts: (|x|)| |AND| |(|y|)
            // Each sub criteria has 3 parts and each connector has 3 parts
            // Number of connectors = number of sub criteria - 1
            // # parts = 3n + 3c      ; c=n-1
            //         = 3n + 3(n-1)
            //         = 6n - 3
            Object[] parts = new Object[(6*subCriteria.size())-3];

            // Add first criteria
            Iterator iter = subCriteria.iterator();
            Criteria crit = (Criteria) iter.next();
            parts[0] = "("; //$NON-NLS-1$
            parts[1] = registerNode(crit);
            parts[2] = ")"; //$NON-NLS-1$

            // Add rest of the criteria
            for(int i=3; iter.hasNext(); i=i+6) {
                // Add connector
                parts[i] = SPACE;
                parts[i+1] = operatorStr;
                parts[i+2] = SPACE;

                // Add criteria
                crit = (Criteria) iter.next();
                parts[i+3] = "("; //$NON-NLS-1$
                parts[i+4] = registerNode(crit);
                parts[i+5] = ")"; //$NON-NLS-1$
            }

            replaceStringParts(parts);
        }
    }

    public void visit(Delete obj) {
		//add delete clause
		parts.add(ReservedWords.DELETE);
		parts.add(SPACE);
		//add from clause
		parts.add(ReservedWords.FROM);
		parts.add(SPACE);
		parts.add(registerNode(obj.getGroup()));

		//add where clause
		if(obj.getCriteria() != null) {
			parts.add(SPACE);
			parts.add(ReservedWords.WHERE);
			parts.add(SPACE);
			parts.add(registerNode(obj.getCriteria()));
		}

		// Option clause
		if(obj.getOption() != null) {
            parts.add(SPACE);
            parts.add(registerNode(obj.getOption()));
		}
    }
    
    public void visit(DependentSetCriteria obj) {
        parts.add(registerNode(obj.getExpression()));

        // operator and beginning of list
        parts.add(SPACE);
        if (obj.isNegated()) {
            parts.add(ReservedWords.NOT);
            parts.add(SPACE);
        }
        parts.add(ReservedWords.IN);
        parts.add(" (<dependent values>)"); //$NON-NLS-1$
    }

    public void visit(From obj) {
        Object[] parts = null;
        List clauses = obj.getClauses();
        if(clauses.size() == 1) {
            replaceStringParts(new Object[] {
                ReservedWords.FROM, SPACE,
                registerNode( (FromClause) clauses.get(0) ) });
        } else if(clauses.size() > 1) {
            parts = new Object[2 + clauses.size() + (clauses.size()-1)];

            // Add first clause
            parts[0] = ReservedWords.FROM;
            parts[1] = SPACE;
            Iterator clauseIter = clauses.iterator();
            parts[2] = registerNode((FromClause) clauseIter.next());

            // Add rest of the clauses
            for(int i=3; clauseIter.hasNext(); i=i+2) {
                parts[i] = ", "; //$NON-NLS-1$
                parts[i+1] = registerNode((FromClause) clauseIter.next());
            }

            replaceStringParts(parts);
        } else {
            // Shouldn't happen, but being tolerant
            replaceStringParts(new Object[] { ReservedWords.FROM });
        }
    }

    public void visit(GroupBy obj) {
        Object[] parts = null;
        List symbols = obj.getSymbols();
        if(symbols.size() == 1) {
            replaceStringParts(new Object[] {
                ReservedWords.GROUP, SPACE, ReservedWords.BY, SPACE,
                registerNode( (Expression) symbols.get(0) ) });
        } else if(symbols.size() > 1) {
            parts = new Object[4 + symbols.size() + (symbols.size()-1)];

            // Add first clause
            parts[0] = ReservedWords.GROUP;
            parts[1] = SPACE;
            parts[2] = ReservedWords.BY;
            parts[3] = SPACE;
            Iterator symbolIter = symbols.iterator();
            parts[4] = registerNode((Expression) symbolIter.next());

            // Add rest of the clauses
            for(int i=5; symbolIter.hasNext(); i=i+2) {
                parts[i] = ", "; //$NON-NLS-1$
                parts[i+1] = registerNode((Expression) symbolIter.next());
            }

            replaceStringParts(parts);
        } else {
            // Shouldn't happen, but being tolerant
            replaceStringParts(new Object[] { ReservedWords.GROUP, SPACE, ReservedWords.BY });
        }
    }

    public void visit(Insert obj) {
        formatBasicInsert(obj);
        
        if ( obj.getQueryExpression() != null ) {
            parts.add(registerNode(obj.getQueryExpression()));
        } else {
            parts.add(ReservedWords.VALUES);
            parts.add(" ("); //$NON-NLS-1$
            Iterator valueIter = obj.getValues().iterator();
            while(valueIter.hasNext()) {
                Expression valObj = (Expression) valueIter.next();
                parts.add(registerNode(valObj));
                if(valueIter.hasNext()) {
                    parts.add(", "); //$NON-NLS-1$
                }
            }
            parts.add(")"); //$NON-NLS-1$
        }
            
    	// Option clause
		if(obj.getOption() != null) {
            parts.add(SPACE);
            parts.add(registerNode(obj.getOption()));
		}
    }

    public void visit(Create obj) {
        parts.add(ReservedWords.CREATE);
        parts.add(SPACE);
        parts.add(ReservedWords.LOCAL);
        parts.add(SPACE);
        parts.add(ReservedWords.TEMPORARY);
        parts.add(SPACE);
        parts.add(ReservedWords.TABLE);
        parts.add(SPACE);
        parts.add(registerNode(obj.getTable()));
        parts.add(SPACE);

        // Columns clause
        List columns = obj.getColumns();
        parts.add("("); //$NON-NLS-1$
        Iterator iter = columns.iterator();
        while(iter.hasNext()) {
            ElementSymbol element = (ElementSymbol) iter.next();
            element.setDisplayMode(ElementSymbol.DisplayMode.SHORT_OUTPUT_NAME);
            parts.add(registerNode(element));
            parts.add(SPACE);
            parts.add(DataTypeManager.getDataTypeName(element.getType()));
            if(iter.hasNext()) {
                parts.add(", "); //$NON-NLS-1$
            }
        }
        parts.add(")"); //$NON-NLS-1$
    }
    
    public void visit(Drop obj) {
        parts.add(ReservedWords.DROP);
        parts.add(SPACE);
        parts.add(ReservedWords.TABLE);
        parts.add(SPACE);
        parts.add(registerNode(obj.getTable()));
    }
    
    private void formatBasicInsert(Insert obj) {
        parts.add(ReservedWords.INSERT);
        parts.add(SPACE);
        parts.add(ReservedWords.INTO);
        parts.add(SPACE);
        parts.add(registerNode(obj.getGroup()));
        parts.add(SPACE);
        
        if (!obj.getVariables().isEmpty()) {
            
            // Columns clause
            List vars = obj.getVariables();
            if(vars != null) {
                parts.add("("); //$NON-NLS-1$
                Iterator iter = vars.iterator();
                while(iter.hasNext()) {
                    ElementSymbol element = (ElementSymbol) iter.next();
                    parts.add(registerNode(element));
                    if(iter.hasNext()) {
                        parts.add(", "); //$NON-NLS-1$
                    }
                }
                parts.add(") "); //$NON-NLS-1$
            }
        }
    }

    public void visit(IsNullCriteria obj) {
        Expression expr = obj.getExpression();
        Object exprPart = registerNode(expr);
        parts.add(exprPart);
        parts.add(SPACE);
        parts.add(ReservedWords.IS);
        parts.add(SPACE);
        if (obj.isNegated()) {
            parts.add(ReservedWords.NOT);
            parts.add(SPACE);
        }
        parts.add(ReservedWords.NULL);
    }

    public void visit(JoinPredicate obj) {
        if (obj.isOptional()) {
            addOptionComment(obj);
        }
        
        if(obj.hasHint()) {
            parts.add("(");//$NON-NLS-1$
        }

        // left clause
        FromClause leftClause = obj.getLeftClause();
		if(leftClause instanceof JoinPredicate && !((JoinPredicate)leftClause).hasHint()) {
			parts.add("("); //$NON-NLS-1$
			parts.add(registerNode(leftClause));
			parts.add(")"); //$NON-NLS-1$
		} else {
			parts.add(registerNode(leftClause));
		}

        // join type
        parts.add(SPACE);
        parts.add(registerNode(obj.getJoinType()));
        parts.add(SPACE);

        // right clause
        FromClause rightClause = obj.getRightClause();
		if(rightClause instanceof JoinPredicate && !((JoinPredicate)rightClause).hasHint()) {
			parts.add("("); //$NON-NLS-1$
			parts.add(registerNode(rightClause));
			parts.add(")"); //$NON-NLS-1$
		} else {
			parts.add(registerNode(rightClause));
		}

        // join criteria
        List joinCriteria = obj.getJoinCriteria();
		if(joinCriteria != null && joinCriteria.size() > 0) {
            parts.add(SPACE);
			parts.add(ReservedWords.ON);
            parts.add(SPACE);
			Iterator critIter = joinCriteria.iterator();
			while(critIter.hasNext()) {
				Criteria crit = (Criteria) critIter.next();
                if(crit instanceof PredicateCriteria) {
    				parts.add(registerNode(crit));
                } else {
                    parts.add("("); //$NON-NLS-1$
                    parts.add(registerNode(crit));
                    parts.add(")"); //$NON-NLS-1$
                }

				if(critIter.hasNext()) {
					parts.add(SPACE);
					parts.add(ReservedWords.AND);
					parts.add(SPACE);
				}
			}
		}

        if(obj.hasHint()) {
            parts.add(")"); //$NON-NLS-1$
        }
        addFromClasueDepOptions(obj);
    }

    private void addFromClasueDepOptions(FromClause obj) {
        if (obj.isMakeDep()) {
            parts.add(SPACE);
            parts.add(Option.MAKEDEP);
        }
        if (obj.isMakeNotDep()) {
            parts.add(SPACE);
            parts.add(Option.MAKENOTDEP);
        }
    }

    private void addOptionComment(FromClause obj) {
        parts.add(BEGIN_COMMENT);
        parts.add(SPACE);
        if (obj.isOptional()) {
            parts.add(Option.OPTIONAL);
            parts.add(SPACE);
        }
        parts.add(END_COMMENT);
        parts.add(SPACE);
    }

    public void visit(JoinType obj) {
        Object[] parts = null;
        if(obj.equals(JoinType.JOIN_INNER)) {
            parts = new Object[] { ReservedWords.INNER, SPACE, ReservedWords.JOIN };
        } else if(obj.equals(JoinType.JOIN_CROSS)) {
            parts = new Object[] { ReservedWords.CROSS, SPACE, ReservedWords.JOIN };
        } else if(obj.equals(JoinType.JOIN_LEFT_OUTER)) {
            parts = new Object[] { ReservedWords.LEFT, SPACE, ReservedWords.OUTER, SPACE, ReservedWords.JOIN };
        } else if(obj.equals(JoinType.JOIN_RIGHT_OUTER)) {
            parts = new Object[] { ReservedWords.RIGHT, SPACE, ReservedWords.OUTER, SPACE, ReservedWords.JOIN };
        } else if(obj.equals(JoinType.JOIN_FULL_OUTER)) {
            parts = new Object[] { ReservedWords.FULL, SPACE, ReservedWords.OUTER, SPACE, ReservedWords.JOIN };
        } else if(obj.equals(JoinType.JOIN_UNION)) {
            parts = new Object[] { ReservedWords.UNION, SPACE, ReservedWords.JOIN };
        } else if (obj.equals(JoinType.JOIN_SEMI)) {
            parts = new Object[] { "SEMI", SPACE, ReservedWords.JOIN }; //$NON-NLS-1$
        } else if (obj.equals(JoinType.JOIN_ANTI_SEMI)) {
            parts = new Object[] { "ANTI SEMI", SPACE, ReservedWords.JOIN }; //$NON-NLS-1$
        }

        replaceStringParts(parts);
    }

    public void visit(MatchCriteria obj) {
        parts.add(registerNode(obj.getLeftExpression()));

        parts.add(SPACE);
        if (obj.isNegated()) {
            parts.add(ReservedWords.NOT);
            parts.add(SPACE);
        }
        parts.add(ReservedWords.LIKE);
        parts.add(SPACE);

        parts.add(registerNode(obj.getRightExpression()));

        if(obj.getEscapeChar() != MatchCriteria.NULL_ESCAPE_CHAR) {
            parts.add(SPACE);
            parts.add(ReservedWords.ESCAPE);
            parts.add(" '"); //$NON-NLS-1$
            parts.add("" + obj.getEscapeChar()); //$NON-NLS-1$
            parts.add("'"); //$NON-NLS-1$
        }
    }

    public void visit(NotCriteria obj) {
        parts.add(ReservedWords.NOT);
        parts.add(" ("); //$NON-NLS-1$
        parts.add(registerNode(obj.getCriteria()));
        parts.add(")"); //$NON-NLS-1$
    }

    public void visit(Option obj) {
		boolean anOption = false;
        List parts = new ArrayList();

        parts.add(ReservedWords.OPTION);

		if(obj.getShowPlan()) {
			anOption = true;
			parts.add(" "); //$NON-NLS-1$
			parts.add(ReservedWords.SHOWPLAN);
		}

        if(obj.getPlanOnly()) {
            anOption = true;
            parts.add(" "); //$NON-NLS-1$
            parts.add(ReservedWords.PLANONLY);
        }

		if(obj.getDebug()) {
			anOption = true;
			parts.add(" "); //$NON-NLS-1$
			parts.add(ReservedWords.DEBUG);
		}
        
        Collection groups = obj.getDependentGroups();
        if(groups != null && groups.size() > 0) {
            anOption = true;
            parts.add(" "); //$NON-NLS-1$
            parts.add(ReservedWords.MAKEDEP);
            parts.add(" "); //$NON-NLS-1$

            Iterator iter = groups.iterator();
            parts.add(iter.next());

            while(iter.hasNext()) {
                parts.add(", "); //$NON-NLS-1$
                parts.add(iter.next());
            }
        }
        
        groups = obj.getNotDependentGroups();
        if(groups != null && groups.size() > 0) {
            anOption = true;
            parts.add(" "); //$NON-NLS-1$
            parts.add(ReservedWords.MAKENOTDEP);
            parts.add(" "); //$NON-NLS-1$

            Iterator iter = groups.iterator();
            parts.add(iter.next());

            while(iter.hasNext()) {
                parts.add(", "); //$NON-NLS-1$
                parts.add(iter.next());
            }
        }
        
        groups = obj.getNoCacheGroups();
        if(groups != null && groups.size() > 0) {
            anOption = true;
            parts.add(" "); //$NON-NLS-1$
            parts.add(ReservedWords.NOCACHE);
            parts.add(" "); //$NON-NLS-1$

            Iterator iter = groups.iterator();
            parts.add(iter.next());

            while(iter.hasNext()) {
                parts.add(", "); //$NON-NLS-1$
                parts.add(iter.next());
            }
        }else if(obj.isNoCache()){
            anOption = true;
            parts.add(" "); //$NON-NLS-1$
            parts.add(ReservedWords.NOCACHE);
        }

        // Only add this if any option was set to true - otherwise omit
		if(anOption) {
            replaceStringParts(parts.toArray());
		} else {
            replaceStringParts(new Object[0]);
        }
    }

    public void visit(OrderBy obj) {
        parts.add(ReservedWords.ORDER);
        parts.add(SPACE);
        parts.add(ReservedWords.BY);
		parts.add(SPACE);

        List variables = obj.getVariables();
        List types = obj.getTypes();
        Iterator iter = variables.iterator();
        Iterator typeIter = types.iterator();
        while ( iter.hasNext() ) {
		    SingleElementSymbol ses = (SingleElementSymbol)iter.next();
            if (ses instanceof ElementSymbol && ((ElementSymbol)ses).getDisplayMode().equals(ElementSymbol.DisplayMode.SHORT_OUTPUT_NAME)) {
                parts.add(registerNode(ses));
            } else {
                outputDisplayName(ses.getOutputName());
            }
            Boolean type = (Boolean) typeIter.next();
            if( type.booleanValue() == OrderBy.DESC ) {
                parts.add(SPACE);
                parts.add(ReservedWords.DESC);
            } // Don't print default "ASC"

            if (iter.hasNext()) {
                parts.add( ", " ); //$NON-NLS-1$
            }
	    }
    }
    
    public void visit(DynamicCommand obj) {
        parts.add(ReservedWords.EXECUTE);
        parts.add(SPACE);
        parts.add(ReservedWords.STRING);
        parts.add(SPACE);
        parts.add(registerNode(obj.getSql()));

        if(obj.isAsClauseSet()){
            parts.add(SPACE);
            parts.add(ReservedWords.AS);
            parts.add(SPACE);
            for (int i = 0; i < obj.getAsColumns().size(); i++) {
                ElementSymbol symbol = (ElementSymbol)obj.getAsColumns().get(i);
                symbol.setDisplayMode(ElementSymbol.DisplayMode.SHORT_OUTPUT_NAME);
                parts.add(registerNode(symbol));
                parts.add(SPACE);
                parts.add(DataTypeManager.getDataTypeName(symbol.getType()));
                if (i < obj.getAsColumns().size() - 1) {
                    parts.add(", "); //$NON-NLS-1$
                }
            }
        }

        if(obj.getIntoGroup() != null){
            parts.add(SPACE);
            parts.add(ReservedWords.INTO);
            parts.add(SPACE);
            parts.add(registerNode(obj.getIntoGroup()));
        }

        if(obj.getUsing() != null && !obj.getUsing().isEmpty()) {
            parts.add(SPACE);
            parts.add(ReservedWords.USING);
            parts.add(SPACE);
            parts.add(registerNode(obj.getUsing()));
        }

        if (obj.getUpdatingModelCount() > 0) {
            parts.add(SPACE);
            parts.add(ReservedWords.UPDATE);
            parts.add(SPACE);
            if (obj.getUpdatingModelCount() > 1) {
                parts.add("*"); //$NON-NLS-1$
            } else {
                parts.add("1"); //$NON-NLS-1$
            }
        }
    }

    public void visit(SetClauseList obj) {
    	for (Iterator<SetClause> iterator = obj.getClauses().iterator(); iterator.hasNext();) {
			SetClause clause = iterator.next();
			parts.add(registerNode(clause));
            if (iterator.hasNext()) {
                parts.add(", "); //$NON-NLS-1$
            }
		}
    }
    
    public void visit(SetClause obj) {
        ElementSymbol symbol = obj.getSymbol();
        symbol.setDisplayMode(ElementSymbol.DisplayMode.SHORT_OUTPUT_NAME);
        parts.add(registerNode(symbol));
        parts.add(" = "); //$NON-NLS-1$
        parts.add(registerNode(obj.getValue()));
    }

    public void visit(Query obj) {
        parts.add(registerNode(obj.getSelect()));

        if(obj.getInto() != null){
            parts.add(SPACE);
            parts.add(ReservedWords.INTO);
            parts.add(SPACE);
            parts.add(registerNode(obj.getInto()));
        }

        if(obj.getFrom() != null){
            parts.add(SPACE);
            parts.add(registerNode(obj.getFrom()));
        }

        // Where clause
        if(obj.getCriteria() != null) {
            parts.add(SPACE);
            parts.add(ReservedWords.WHERE);
            parts.add(SPACE);
            parts.add(registerNode(obj.getCriteria()));
        }

		// Group by clause
        if(obj.getGroupBy() != null) {
            parts.add(SPACE);
            parts.add(registerNode(obj.getGroupBy()));
        }

		// Having clause
		if(obj.getHaving() != null) {
            parts.add(SPACE);
            parts.add(ReservedWords.HAVING);
            parts.add(SPACE);
            parts.add(registerNode(obj.getHaving()));
		}

		// Order by clause
		if(obj.getOrderBy() != null) {
            parts.add(SPACE);
            parts.add(registerNode(obj.getOrderBy()));
		}
        
        if (obj.getLimit() != null) {
            parts.add(SPACE);
            parts.add(registerNode(obj.getLimit()));
        }

		// Option clause
		if(obj.getOption() != null) {
            parts.add(SPACE);
            parts.add(registerNode(obj.getOption()));
		}
    }

    public void visit(SearchedCaseExpression obj) {
        parts.add(ReservedWords.CASE);
        for (int i = 0; i < obj.getWhenCount(); i++) {
            parts.add(SPACE);
            parts.add(ReservedWords.WHEN);
            parts.add(SPACE);
            parts.add(registerNode(obj.getWhenCriteria(i)));
            parts.add(SPACE);
            parts.add(ReservedWords.THEN);
            parts.add(SPACE);
            parts.add(registerNode(obj.getThenExpression(i)));
        }
        parts.add(SPACE);
        if (obj.getElseExpression() != null) {
            parts.add(ReservedWords.ELSE);
            parts.add(SPACE);
            parts.add(registerNode(obj.getElseExpression()));
            parts.add(SPACE);
        }
        parts.add(ReservedWords.END);
    }

    public void visit(Select obj) {
        parts.add(ReservedWords.SELECT);
        parts.add(SPACE);

		if(obj.isDistinct()) {
			parts.add(ReservedWords.DISTINCT);
			parts.add(SPACE);
		}

	    Iterator iter = obj.getSymbols().iterator();
        while( iter.hasNext() ) {
			SelectSymbol symbol = (SelectSymbol) iter.next();
			parts.add(registerNode(symbol));
			if(iter.hasNext()) {
				parts.add(", "); //$NON-NLS-1$
			}
        }
    }

    public void visit(SetCriteria obj) {
		// variable
		parts.add(registerNode(obj.getExpression()));

		// operator and beginning of list
		parts.add(SPACE);
        if (obj.isNegated()) {
            parts.add(ReservedWords.NOT);
            parts.add(SPACE);
        }
		parts.add(ReservedWords.IN);
		parts.add(" ("); //$NON-NLS-1$

		// value list
		List vals = obj.getValues();
		int size = vals.size();
		if(size == 1) {
			Iterator iter = vals.iterator();
			Expression expr = (Expression) iter.next();
			parts.add( registerNode( expr ) );
		} else if(size > 1) {
			Iterator iter = vals.iterator();
			Expression expr = (Expression) iter.next();
			parts.add( registerNode( expr ) );
			while(iter.hasNext()) {
			    expr = (Expression) iter.next();
				parts.add(", "); //$NON-NLS-1$
				parts.add( registerNode( expr ) );
			}
		}
		parts.add(")"); //$NON-NLS-1$
    }

    public void visit(SetQuery obj) {
        QueryCommand query = obj.getLeftQuery();
        if(query instanceof Query) {
            parts.add(registerNode(query));
        } else {
            parts.add("("); //$NON-NLS-1$
            parts.add(registerNode(query));
            parts.add(")"); //$NON-NLS-1$
        }

        parts.add(SPACE);
        parts.add(obj.getOperation());
        parts.add(SPACE);

        if(obj.isAll()) {
            parts.add(ReservedWords.ALL);
            parts.add(SPACE);
        }

        query = obj.getRightQuery();
        if(query instanceof Query) {
            parts.add(registerNode(query));
        } else {
            parts.add("("); //$NON-NLS-1$
            parts.add(registerNode(query));
            parts.add(")"); //$NON-NLS-1$
        }

        if(obj.getOrderBy() != null) {
            parts.add(SPACE);
            parts.add(registerNode(obj.getOrderBy()));
        }

        if(obj.getLimit() != null) {
            parts.add(SPACE);
            parts.add(registerNode(obj.getLimit()));
        }

        if(obj.getOption() != null) {
            parts.add(SPACE);
            parts.add(registerNode(obj.getOption()));
        }
    }

    public void visit(XQuery obj) {
        // XQuery string
        parts.add(obj.getXQuery());

        // Option clause
        if(obj.getOption() != null) {
            parts.add(SPACE);
            parts.add(registerNode(obj.getOption()));
        }        
    }

    public void visit(StoredProcedure obj) {
        //exec clause
        parts.add(ReservedWords.EXEC);
		parts.add(SPACE);
		parts.add(obj.getProcedureName());
		parts.add("("); //$NON-NLS-1$
		List params = obj.getInputParameters();
        if(params != null) {
            Iterator iter = params.iterator();
            while(iter.hasNext()) {
            	SPParameter param = (SPParameter) iter.next();
                
            	if (obj.displayNamedParameters()) {
            	    parts.add(escapeSinglePart(ElementSymbol.getShortName(param.getParameterSymbol().getOutputName())));
                    parts.add(" = "); //$NON-NLS-1$
                }
                
                if(param.getExpression() == null) {
                    if(param.getName() != null) {
                        parts.add(escapeSinglePart(obj.getParamFullName(param)));
                    } else {
                        parts.add("?"); //$NON-NLS-1$
                    }
                } else {
                    parts.add(registerNode(param.getExpression()));
                }
                if(iter.hasNext()) {
                    parts.add(", "); //$NON-NLS-1$
                }
            }
        }
        parts.add(")"); //$NON-NLS-1$

        // Option clause
		if(obj.getOption() != null) {
            parts.add(SPACE);
            parts.add(registerNode(obj.getOption()));
		}else{
			parts.add(""); //$NON-NLS-1$
		}
    }

    public void visit(SubqueryFromClause obj) {
        if(obj.isOptional()) {
            addOptionComment(obj);
        }
        parts.add("(");//$NON-NLS-1$
        parts.add(registerNode(obj.getCommand()));
        parts.add(")");//$NON-NLS-1$
        parts.add(" AS ");//$NON-NLS-1$
        parts.add(obj.getOutputName());
        addFromClasueDepOptions(obj);
    }

    public void visit(SubquerySetCriteria obj) {
        // variable
        parts.add(registerNode(obj.getExpression()));

        // operator and beginning of list
        parts.add(SPACE);
        if (obj.isNegated()) {
            parts.add(ReservedWords.NOT);
            parts.add(SPACE);
        }
        parts.add(ReservedWords.IN);
        parts.add(" ("); //$NON-NLS-1$
        parts.add(registerNode(obj.getCommand()));
        parts.add(")"); //$NON-NLS-1$
    }


    public void visit(UnaryFromClause obj) {
        if(obj.isOptional()) {
            addOptionComment(obj);
        }
        parts.add(registerNode(obj.getGroup()));
        addFromClasueDepOptions(obj);
    }

    public void visit(Update obj) {
        // Update clause
        parts.add(ReservedWords.UPDATE);
		parts.add(SPACE);
        parts.add(registerNode(obj.getGroup()));
		parts.add(SPACE);

        // Set clause
        parts.add(ReservedWords.SET);
        parts.add(SPACE);

        parts.add(registerNode(obj.getChangeList()));
        
		// Where clause
		if(obj.getCriteria() != null) {
			parts.add(SPACE);
			parts.add(ReservedWords.WHERE);
			parts.add(SPACE);
			parts.add(registerNode(obj.getCriteria()));
		}

		// Option clause
		if(obj.getOption() != null) {
            parts.add(SPACE);
            parts.add(registerNode(obj.getOption()));
		}
    }

    public void visit(Into obj) {
        parts.add(registerNode(obj.getGroup()));
    }

    // ############ Visitor methods for symbol objects ####################

    public void visit(AggregateSymbol obj) {
        parts.add(obj.getAggregateFunction());
		parts.add("("); //$NON-NLS-1$

		if(obj.isDistinct()) {
			parts.add(ReservedWords.DISTINCT);
			parts.add(" "); //$NON-NLS-1$
		}

		if(obj.getExpression() == null) {
			parts.add(ReservedWords.ALL_COLS);
		} else {
			parts.add(registerNode(obj.getExpression()));
		}
		parts.add(")"); //$NON-NLS-1$
    }

    public void visit(AliasSymbol obj) {
        parts.add(registerNode(obj.getSymbol()));
        parts.add(SPACE);
        parts.add(ReservedWords.AS);
        parts.add(SPACE);
        parts.add(escapeSinglePart(obj.getOutputName()));
    }

    public void visit(AllInGroupSymbol obj) {
        parts.add(obj.getName());
    }

    public void visit(AllSymbol obj) {
        parts.add(obj.getName());
    }

    public void visit(Constant obj) {
        Object[] constantParts = null;
        if (obj.isMultiValued()) {
        	constantParts = new Object[] {"?"}; //$NON-NLS-1$
        } else if(obj.isNull()) {
			constantParts = new Object[] {"null"}; //$NON-NLS-1$
		} else {
		    try {
                Class type = obj.getType();
		        if(type.equals(DataTypeManager.DefaultDataClasses.STRING)) {
			        String strValue = (String) obj.getValue();
			        strValue = escapeStringValue(strValue);
				    constantParts = new Object[] { getStringQuoteBegin(), strValue, getStringQuoteEnd() };
		        } else if(Number.class.isAssignableFrom(type)) {
                    constantParts = new Object[] { obj.getValue().toString() };
                } else if(type.equals(DataTypeManager.DefaultDataClasses.BOOLEAN)) {
                    constantParts = new Object[] { obj.getValue().equals(Boolean.TRUE) ? ReservedWords.TRUE : ReservedWords.FALSE}; 
			    } else if(type.equals(DataTypeManager.DefaultDataClasses.TIMESTAMP)) {
                    constantParts = new Object[] { "{ts'", obj.getValue().toString(), "'}" }; //$NON-NLS-1$ //$NON-NLS-2$
                } else if(type.equals(DataTypeManager.DefaultDataClasses.TIME)) {
                    constantParts = new Object[] { "{t'", obj.getValue().toString(), "'}" }; //$NON-NLS-1$ //$NON-NLS-2$
                } else if(type.equals(DataTypeManager.DefaultDataClasses.DATE)) {
                    constantParts = new Object[] { "{d'", obj.getValue().toString(), "'}" }; //$NON-NLS-1$ //$NON-NLS-2$
                } else {
                    constantParts = new Object[] { getStringQuoteBegin(), obj.getValue().toString(), getStringQuoteEnd() };
                }
		    } catch(RuntimeException e) {
                Assertion.failed(QueryPlugin.Util.getString(ErrorMessageKeys.SQL_0026, e.getMessage()));
		    }
        }

        replaceStringParts(constantParts);
    }

	/**
	 * Get the string to quote the beginning of a string with.  By default this is the ' character.
	 * @return String quote begin character
	 */
    protected String getStringQuoteBegin() {
    	return "'"; //$NON-NLS-1$
    }

	/**
	 * Get the string to quote the end of a string with.  By default this is the ' character.
	 * @return String quote end character
	 */
    protected String getStringQuoteEnd() {
    	return "'"; //$NON-NLS-1$
    }

 	/**
 	 * Take a string literal and escape it as necessary.  By default, this converts ' to ''.
 	 * @param str String literal value (unquoted), never null
 	 * @return Escaped string literal value
 	 */
    protected String escapeStringValue(String str) {
        int index = str.indexOf('\'');
        if(index < 0) {
            return str;
        }
        int last = 0;
    	StringBuffer temp = new StringBuffer();
    	while(index >= 0) {
        	temp.append(str.substring(last, index));
    		temp.append("''"); //$NON-NLS-1$
    		last = index+1;
    		index = str.indexOf('\'', last);
    	}

    	if(last <= (str.length()-1)) {
    		temp.append(str.substring(last));
    	}

    	return temp.toString();
    }

    public void visit(ElementSymbol obj) {
        String name = obj.getOutputName();
        if (obj.getDisplayMode().equals(ElementSymbol.DisplayMode.FULLY_QUALIFIED)) {
            name = obj.getName();
        } else if (obj.getDisplayMode().equals(ElementSymbol.DisplayMode.SHORT_OUTPUT_NAME)) {
            String shortName = SingleElementSymbol.getShortName(name);
            //TODO: this is a hack - since we default to not supporting double quoted identifiers, we need to fully qualify reserved
            if (!isReservedWord(shortName)) {
                name = shortName;
            }
        }
        
        outputDisplayName(name);
    }

    private void outputDisplayName(String name) {
        String[] pathParts = name.split("\\."); //$NON-NLS-1$
        for (int i = 0; i < pathParts.length; i++) {
            if (i > 0) {
                parts.add(ElementSymbol.SEPARATOR);
            }
            parts.add(escapeSinglePart(pathParts[i]));
        }
    }

    public void visit(ExpressionSymbol obj) {
        parts.add(registerNode(obj.getExpression()));
    }

    public void visit(Function obj) {
        String name = obj.getName();
        Expression[] args = obj.getArgs();
		if(obj.isImplicit()) {
			// Hide this function, which is implicit
            parts.add(registerNode(args[0]));

		} else if(name.equalsIgnoreCase(ReservedWords.CONVERT) || name.equalsIgnoreCase(ReservedWords.CAST)) {
			parts.add(name);
			parts.add("("); //$NON-NLS-1$

			if(args != null && args.length > 0) {
				parts.add(registerNode(args[0]));

				if(name.equalsIgnoreCase(ReservedWords.CONVERT)) {
					parts.add(", "); //$NON-NLS-1$
				} else {
					parts.add(" "); //$NON-NLS-1$
					parts.add(ReservedWords.AS);
					parts.add(" "); //$NON-NLS-1$
				}

				if(args.length < 2 || args[1] == null || !(args[1] instanceof Constant)) {
				    parts.add(UNDEFINED);
				} else {
					parts.add(((Constant)args[1]).getValue());
				}
			}
			parts.add(")"); //$NON-NLS-1$

		} else if(name.equals("+") || name.equals("-") || name.equals("*") || name.equals("/") || name.equals("||")) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
			parts.add("("); //$NON-NLS-1$

			if(args != null) {
				for(int i=0; i<args.length; i++) {
					parts.add(registerNode(args[i]));
					if(i < (args.length-1)) {
						parts.add(SPACE);
						parts.add(name);
						parts.add(SPACE);
					}
				}
			}
			parts.add(")"); //$NON-NLS-1$

        } else if(name.equalsIgnoreCase(ReservedWords.TIMESTAMPADD) || name.equalsIgnoreCase(ReservedWords.TIMESTAMPDIFF)) {
            parts.add(name);
            parts.add("("); //$NON-NLS-1$

            if(args != null && args.length > 0) {
                parts.add(((Constant)args[0]).getValue());

                for(int i=1; i<args.length; i++) {
                    parts.add(", "); //$NON-NLS-1$
                    parts.add(registerNode(args[i]));
                }
            }
            parts.add(")"); //$NON-NLS-1$

		} else {
			parts.add(name);
			parts.add("("); //$NON-NLS-1$

			if(args.length > 0) {
				for(int i=0; i<args.length; i++) {
					parts.add(registerNode(args[i]));
					if(i < (args.length-1)) {
						parts.add(", "); //$NON-NLS-1$
					}
				}
			}

			parts.add(")"); //$NON-NLS-1$
		}
    }

    public void visit(GroupSymbol obj) {
        String alias = null;
        String fullGroup = obj.getOutputName();
        if(obj.getOutputDefinition() != null) {
            alias = obj.getOutputName();
            fullGroup = obj.getOutputDefinition();
        }
        
        outputDisplayName(fullGroup);

        if(alias != null) {
            parts.add(SPACE);
            parts.add(ReservedWords.AS);
            parts.add(SPACE);
            parts.add(escapeSinglePart(alias));
        }
    }

    public void visit(Reference obj) {
        if (!obj.isPositional() && obj.getExpression() != null) {
            replaceStringParts(new Object[] { obj.getExpression().toString() });
        } else {
            replaceStringParts(new Object[] { "?" }); //$NON-NLS-1$
        }
    }

    // ############ Visitor methods for storedprocedure language objects ####################

    public void visit(Block obj) {
    	List statements = obj.getStatements();
    	if(statements.size() == 1) {
    		replaceStringParts(new Object[] { ReservedWords.BEGIN, "\n", //$NON-NLS-1$
			registerNode((Statement)obj.getStatements().get(0)), "\n", ReservedWords.END}); //$NON-NLS-1$
    	} else if(statements.size() > 1) {
	        List parts = new ArrayList();
            // Add first clause
            parts.add(ReservedWords.BEGIN);
            parts.add("\n"); //$NON-NLS-1$
            Iterator stmtIter = statements.iterator();
            while(stmtIter.hasNext()) {
				// Add each statement
	            parts.add(registerNode((Statement) stmtIter.next()));
                parts.add("\n"); //$NON-NLS-1$
            }
            parts.add(ReservedWords.END);
            replaceStringParts(parts.toArray());
        } else {
            // Shouldn't happen, but being tolerant
            replaceStringParts(new Object[] { ReservedWords.BEGIN, "\n", //$NON-NLS-1$
            							ReservedWords.END });
        }
    }

    public void visit(CommandStatement obj) {
        parts.add(registerNode(obj.getCommand()));
		parts.add(";"); //$NON-NLS-1$
    }

    public void visit(CreateUpdateProcedureCommand obj) {
        parts.add(ReservedWords.CREATE);
        parts.add(SPACE);
        if(!obj.isUpdateProcedure()){
            parts.add(ReservedWords.VIRTUAL);
            parts.add(SPACE);
        }
        parts.add(ReservedWords.PROCEDURE);
        parts.add("\n"); //$NON-NLS-1$
        parts.add(registerNode(obj.getBlock()));
    }

    public void visit(DeclareStatement obj) {
		parts.add(ReservedWords.DECLARE);
		parts.add(SPACE);
        parts.add(obj.getVariableType());
        parts.add(SPACE);
        createAssignment(obj);
    }

    /** 
     * @param obj
     * @param parts
     */
    private void createAssignment(AssignmentStatement obj) {
        parts.add(registerNode(obj.getVariable()));
        if (obj.getValue() != null) {
            parts.add(" = "); //$NON-NLS-1$
            parts.add(registerNode(obj.getValue()));
        }
		parts.add(";"); //$NON-NLS-1$
    }

    public void visit(IfStatement obj) {
        parts.add(ReservedWords.IF);
        parts.add("("); //$NON-NLS-1$
        parts.add(registerNode(obj.getCondition()));
        parts.add(")\n"); //$NON-NLS-1$
        parts.add(registerNode(obj.getIfBlock()));
        if(obj.hasElseBlock()) {
        	parts.add("\n"); //$NON-NLS-1$
	        parts.add(ReservedWords.ELSE);
	        parts.add("\n"); //$NON-NLS-1$
	        parts.add(registerNode(obj.getElseBlock()));
        }
    }

    public void visit(AssignmentStatement obj) {
        createAssignment(obj);
    }

    public void visit(HasCriteria obj) {
        parts.add( ReservedWords.HAS);
        parts.add(SPACE);
        parts.add(registerNode(obj.getSelector()));
    }

    public void visit(TranslateCriteria obj) {
        parts.add(ReservedWords.TRANSLATE);
        parts.add(SPACE);
        parts.add(registerNode(obj.getSelector()));

        if(obj.hasTranslations()) {
	        parts.add(SPACE);
	        parts.add(ReservedWords.WITH);
	        parts.add(SPACE);
        	parts.add("("); //$NON-NLS-1$
	        Iterator critIter = obj.getTranslations().iterator();

	        while(critIter.hasNext()) {
				parts.add(registerNode((Criteria)critIter.next()));
				if(critIter.hasNext()) {
					parts.add(", "); //$NON-NLS-1$
				}
				if(!critIter.hasNext()) {
		        	parts.add(")"); //$NON-NLS-1$
				}
	        }
        }
    }

    public void visit(CriteriaSelector obj) {
        int selectorType = obj.getSelectorType();

        switch(selectorType) {
        	case CriteriaSelector.COMPARE_EQ:
        		parts.add("= "); //$NON-NLS-1$
        		break;
        	case CriteriaSelector.COMPARE_GE:
        		parts.add(">= "); //$NON-NLS-1$
        		break;
        	case CriteriaSelector.COMPARE_GT:
        		parts.add("> "); //$NON-NLS-1$
        		break;
        	case CriteriaSelector.COMPARE_LE:
        		parts.add("<= "); //$NON-NLS-1$
        		break;
        	case CriteriaSelector.COMPARE_LT:
        		parts.add("< "); //$NON-NLS-1$
        		break;
        	case CriteriaSelector.COMPARE_NE:
        		parts.add("<> "); //$NON-NLS-1$
        		break;
        	case CriteriaSelector.IN:
        		parts.add(ReservedWords.IN);
        		parts.add(SPACE);
        		break;
        	case CriteriaSelector.IS_NULL:
        		parts.add(ReservedWords.IS);
        		parts.add(SPACE);
        		parts.add(ReservedWords.NULL);
        		parts.add(SPACE);
        		break;
            case CriteriaSelector.LIKE:
                parts.add(ReservedWords.LIKE);
                parts.add(SPACE);
                break;
            case CriteriaSelector.BETWEEN:
                parts.add(ReservedWords.BETWEEN);
                parts.add(SPACE);
                break;
        }

        parts.add(ReservedWords.CRITERIA);
		if(obj.hasElements()) {
	        parts.add(SPACE);
	        parts.add(ReservedWords.ON);
	        parts.add(SPACE);
	        parts.add("("); //$NON-NLS-1$

	        Iterator elmtIter = obj.getElements().iterator();
	        while(elmtIter.hasNext()) {
				parts.add(registerNode((ElementSymbol)elmtIter.next()));
				if(elmtIter.hasNext()) {
					parts.add(", "); //$NON-NLS-1$
				}
	        }
	        parts.add(")"); //$NON-NLS-1$
		}
    }

    public void visit(RaiseErrorStatement obj) {
        Object parts[] = new Object[4];

        parts[0] = ReservedWords.ERROR;
        parts[1] = SPACE;
        parts[2] = registerNode(obj.getExpression());
        parts[3] = ";"; //$NON-NLS-1$
        replaceStringParts(parts);
    }

    public void visit(BreakStatement obj) {
        parts.add(ReservedWords.BREAK);
        parts.add(";"); //$NON-NLS-1$
    }

    public void visit(ContinueStatement obj) {
        parts.add(ReservedWords.CONTINUE);
        parts.add(";"); //$NON-NLS-1$
    }

    public void visit(LoopStatement obj) {
        parts.add(ReservedWords.LOOP);
        parts.add(" "); //$NON-NLS-1$
        parts.add(ReservedWords.ON);
        parts.add(" ("); //$NON-NLS-1$
        parts.add(registerNode(obj.getCommand()));
        parts.add(") "); //$NON-NLS-1$
        parts.add(ReservedWords.AS);
        parts.add(" "); //$NON-NLS-1$
        parts.add(obj.getCursorName());
        parts.add("\n"); //$NON-NLS-1$
        parts.add(registerNode(obj.getBlock()));
    }

    public void visit(WhileStatement obj) {
        parts.add(ReservedWords.WHILE);
        parts.add("("); //$NON-NLS-1$
        parts.add(registerNode(obj.getCondition()));
        parts.add(")\n"); //$NON-NLS-1$
        parts.add(registerNode(obj.getBlock()));
    }

    public void visit(ExistsCriteria obj) {
        // operator and beginning of list
        parts.add(ReservedWords.EXISTS);
        parts.add(" ("); //$NON-NLS-1$
        parts.add(registerNode(obj.getCommand()));
        parts.add(")"); //$NON-NLS-1$
    }

    public void visit(SubqueryCompareCriteria obj){
        Expression leftExpression = obj.getLeftExpression();
        parts.add(registerNode(leftExpression));

        String operator = obj.getOperatorAsString();
        String quantifier = obj.getPredicateQuantifierAsString();

        // operator and beginning of list
        parts.add(SPACE);
        parts.add(operator);
        parts.add(SPACE);
        parts.add(quantifier);
        parts.add("("); //$NON-NLS-1$
        parts.add(registerNode(obj.getCommand()));
        parts.add(")"); //$NON-NLS-1$
    }

    public void visit(ScalarSubquery obj) {
        // operator and beginning of list
        parts.add("("); //$NON-NLS-1$
        parts.add(registerNode(obj.getCommand()));
        parts.add(")"); //$NON-NLS-1$
    }

    
    public void visit(Limit obj) {
        parts.add(ReservedWords.LIMIT);
        if (obj.getOffset() != null) {
            parts.add(SPACE);
            parts.add(registerNode(obj.getOffset()));
            parts.add(","); //$NON-NLS-1$
        }
        parts.add(SPACE);
        parts.add(registerNode(obj.getRowLimit()));
    }

    private static final char ID_ESCAPE_CHAR = '\"';

    private String escapeSinglePart(String part) {
    	if(isReservedWord(part)) {
    	    return ID_ESCAPE_CHAR + part + ID_ESCAPE_CHAR;
    	}
  	 	return part;
    }

    /**
     * Check whether a string is considered a reserved word or not.  Subclasses
     * may override to change definition of reserved word.
     * @param string String to check
     * @return True if reserved word
     */
    protected boolean isReservedWord(String string) {
    	if(string == null) {
    	    return false;
    	}
   		return ReservedWords.isReservedWord(string);
    }

}
