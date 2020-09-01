/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.sql.visitor;

import static org.teiid.language.SQLConstants.Reserved.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.StringUtil;
import org.teiid.language.SQLConstants;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.AlterProcedure;
import org.teiid.query.sql.lang.AlterTrigger;
import org.teiid.query.sql.lang.AlterView;
import org.teiid.query.sql.lang.ArrayTable;
import org.teiid.query.sql.lang.BetweenCriteria;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Create;
import org.teiid.query.sql.lang.Create.CommitAction;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.lang.Drop;
import org.teiid.query.sql.lang.DynamicCommand;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.ExistsCriteria.SubqueryHint;
import org.teiid.query.sql.lang.ExplainCommand;
import org.teiid.query.sql.lang.ExpressionCriteria;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.GroupBy;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Into;
import org.teiid.query.sql.lang.IsDistinctCriteria;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.JoinPredicate;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.JsonTable;
import org.teiid.query.sql.lang.JsonTable.JsonColumn;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.MatchCriteria;
import org.teiid.query.sql.lang.NotCriteria;
import org.teiid.query.sql.lang.ObjectTable;
import org.teiid.query.sql.lang.ObjectTable.ObjectColumn;
import org.teiid.query.sql.lang.Option;
import org.teiid.query.sql.lang.Option.MakeDep;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.PredicateCriteria;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SetClause;
import org.teiid.query.sql.lang.SetClauseList;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.lang.SourceHint;
import org.teiid.query.sql.lang.SourceHint.SpecificHint;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.lang.SubqueryFromClause;
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.lang.TableFunctionReference;
import org.teiid.query.sql.lang.TableFunctionReference.ProjectedColumn;
import org.teiid.query.sql.lang.TextTable;
import org.teiid.query.sql.lang.TextTable.TextColumn;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.lang.WithQueryCommand;
import org.teiid.query.sql.lang.XMLTable;
import org.teiid.query.sql.lang.XMLTable.XMLColumn;
import org.teiid.query.sql.proc.AssignmentStatement;
import org.teiid.query.sql.proc.Block;
import org.teiid.query.sql.proc.BranchingStatement;
import org.teiid.query.sql.proc.CommandStatement;
import org.teiid.query.sql.proc.CreateProcedureCommand;
import org.teiid.query.sql.proc.DeclareStatement;
import org.teiid.query.sql.proc.ExceptionExpression;
import org.teiid.query.sql.proc.IfStatement;
import org.teiid.query.sql.proc.LoopStatement;
import org.teiid.query.sql.proc.RaiseStatement;
import org.teiid.query.sql.proc.ReturnStatement;
import org.teiid.query.sql.proc.Statement;
import org.teiid.query.sql.proc.Statement.Labeled;
import org.teiid.query.sql.proc.TriggerAction;
import org.teiid.query.sql.proc.WhileStatement;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.AggregateSymbol.Type;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.Array;
import org.teiid.query.sql.symbol.CaseExpression;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.DerivedColumn;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.JSONObject;
import org.teiid.query.sql.symbol.MultipleElementSymbol;
import org.teiid.query.sql.symbol.QueryString;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.symbol.SearchedCaseExpression;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.symbol.TextLine;
import org.teiid.query.sql.symbol.WindowFrame;
import org.teiid.query.sql.symbol.WindowFrame.FrameBound;
import org.teiid.query.sql.symbol.WindowFunction;
import org.teiid.query.sql.symbol.WindowSpecification;
import org.teiid.query.sql.symbol.XMLAttributes;
import org.teiid.query.sql.symbol.XMLCast;
import org.teiid.query.sql.symbol.XMLElement;
import org.teiid.query.sql.symbol.XMLExists;
import org.teiid.query.sql.symbol.XMLForest;
import org.teiid.query.sql.symbol.XMLNamespaces;
import org.teiid.query.sql.symbol.XMLNamespaces.NamespaceItem;
import org.teiid.query.sql.symbol.XMLParse;
import org.teiid.query.sql.symbol.XMLQuery;
import org.teiid.query.sql.symbol.XMLSerialize;
import org.teiid.translator.SourceSystemFunctions;

/**
 * <p>
 * The SQLStringVisitor will visit a set of language objects and return the corresponding SQL string representation.
 *
 */
public class SQLStringVisitor extends LanguageVisitor {

    private class DDLVisitor extends DDLStringVisitor {

        private DDLVisitor() {
            super(null, null);
        }

        @Override
        protected DDLStringVisitor append(Object o) {
            SQLStringVisitor.this.append(o);
            return this;
        }

    }

    public static final String UNDEFINED = "<undefined>"; //$NON-NLS-1$
    private static final String SPACE = " "; //$NON-NLS-1$
    private static final String BEGIN_HINT = "/*+"; //$NON-NLS-1$
    private static final String END_HINT = "*/"; //$NON-NLS-1$
    private static final char ID_ESCAPE_CHAR = '\"';
    private static final Set<String> INFIX_FUNCTIONS = new HashSet<String>(Arrays.asList(SQLConstants.Tokens.PLUS,
            SQLConstants.Tokens.MINUS, SQLConstants.Tokens.ALL_COLS, SQLConstants.Tokens.SLASH,
            SQLConstants.Tokens.CONCAT, SQLConstants.Tokens.DOUBLE_AMP));
    protected StringBuilder parts = new StringBuilder();
    private boolean shortNameOnly = false;

    /**
     * Helper to quickly get the parser string for an object using the visitor.
     *
     * @param obj Language object
     * @return String SQL String for obj
     */
    public static final String getSQLString( LanguageObject obj ) {
        if (obj == null) {
            return UNDEFINED;
        }
        SQLStringVisitor visitor = new SQLStringVisitor();
        obj.acceptVisitor(visitor);
        return visitor.getSQLString();
    }

    /**
     * Retrieve completed string from the visitor.
     *
     * @return Complete SQL string for the visited nodes
     */
    public String getSQLString() {
        return this.parts.toString();
    }

    protected void visitNode( LanguageObject obj ) {
        if (obj == null) {
            append(UNDEFINED);
            return;
        }
        obj.acceptVisitor(this);
    }

    protected SQLStringVisitor append( Object value ) {
        this.parts.append(value);
        return this;
    }

    protected void beginClause( @SuppressWarnings("unused") int level ) {
        append(SPACE);
    }

    // ############ Visitor methods for language objects ####################

    @Override
    public void visit( BetweenCriteria obj ) {
        visitNode(obj.getExpression());
        append(SPACE);

        if (obj.isNegated()) {
            append(NOT);
            append(SPACE);
        }
        append(BETWEEN);
        append(SPACE);
        visitNode(obj.getLowerExpression());

        append(SPACE);
        append(AND);
        append(SPACE);
        visitNode(obj.getUpperExpression());
    }

    @Override
    public void visit( CaseExpression obj ) {
        append(CASE);
        append(SPACE);
        visitNode(obj.getExpression());
        append(SPACE);

        for (int i = 0; i < obj.getWhenCount(); i++) {
            append(WHEN);
            append(SPACE);
            visitNode(obj.getWhenExpression(i));
            append(SPACE);
            append(THEN);
            append(SPACE);
            visitNode(obj.getThenExpression(i));
            append(SPACE);
        }

        if (obj.getElseExpression() != null) {
            append(ELSE);
            append(SPACE);
            visitNode(obj.getElseExpression());
            append(SPACE);
        }
        append(END);
    }

    @Override
    public void visit( CompareCriteria obj ) {
        Expression leftExpression = obj.getLeftExpression();
        if (leftExpression instanceof Criteria) {
            append(Tokens.LPAREN);
            visitNode(leftExpression);
            append(Tokens.RPAREN);
        } else {
            visitNode(leftExpression);
        }
        append(SPACE);
        append(obj.getOperatorAsString());
        append(SPACE);
        Expression rightExpression = obj.getRightExpression();
        if (rightExpression instanceof Criteria) {
            append(Tokens.LPAREN);
            visitNode(rightExpression);
            append(Tokens.RPAREN);
        } else {
            visitNode(rightExpression);
        }
    }

    @Override
    public void visit( CompoundCriteria obj ) {
        // Get operator string
        int operator = obj.getOperator();
        String operatorStr = ""; //$NON-NLS-1$
        if (operator == CompoundCriteria.AND) {
            operatorStr = AND;
        } else if (operator == CompoundCriteria.OR) {
            operatorStr = OR;
        }

        // Get criteria
        List<Criteria> subCriteria = obj.getCriteria();

        // Build parts
        if (subCriteria.size() == 1) {
            // Special case - should really never happen, but we are tolerant
            Criteria firstChild = subCriteria.get(0);
            visitNode(firstChild);
        } else {
            // Add first criteria
            Iterator<Criteria> iter = subCriteria.iterator();

            while (iter.hasNext()) {
                // Add criteria
                Criteria crit = iter.next();
                append(Tokens.LPAREN);
                visitNode(crit);
                append(Tokens.RPAREN);

                if (iter.hasNext()) {
                    // Add connector
                    append(SPACE);
                    append(operatorStr);
                    append(SPACE);
                }
            }
        }
    }

    @Override
    public void visit( Delete obj ) {
        // add delete clause
        append(DELETE);
        addSourceHint(obj.getSourceHint());
        append(SPACE);
        // add from clause
        append(FROM);
        append(SPACE);
        visitNode(obj.getGroup());

        // add where clause
        if (obj.getCriteria() != null) {
            beginClause(0);
            visitCriteria(WHERE, obj.getCriteria());
        }

        // Option clause
        if (obj.getOption() != null) {
            beginClause(0);
            visitNode(obj.getOption());
        }
    }

    @Override
    public void visit( DependentSetCriteria obj ) {
        visitNode(obj.getExpression());

        // operator and beginning of list
        append(SPACE);
        if (obj.isNegated()) {
            append(NOT);
            append(SPACE);
        }
        append(IN);
        append(" (<dependent values>)"); //$NON-NLS-1$
    }

    @Override
    public void visit( From obj ) {
        append(FROM);
        beginClause(1);
        registerNodes(obj.getClauses(), 0);
    }

    @Override
    public void visit( GroupBy obj ) {
        append(GROUP);
        append(SPACE);
        append(BY);
        append(SPACE);
        if (obj.isRollup()) {
            append(ROLLUP);
            append(Tokens.LPAREN);
        }
        registerNodes(obj.getSymbols(), 0);
        if (obj.isRollup()) {
            append(Tokens.RPAREN);
        }
    }

    @Override
    public void visit( Insert obj ) {
        if (obj.isUpsert()) {
            append(NonReserved.UPSERT);
        } else {
            append(INSERT);
        }
        addSourceHint(obj.getSourceHint());
        append(SPACE);
        append(INTO);
        append(SPACE);
        visitNode(obj.getGroup());

        if (!obj.getVariables().isEmpty()) {
            beginClause(2);

            // Columns clause
            List<ElementSymbol> vars = obj.getVariables();
            if (vars != null) {
                append("("); //$NON-NLS-1$
                this.shortNameOnly = true;
                registerNodes(vars, 0);
                this.shortNameOnly = false;
                append(")"); //$NON-NLS-1$
            }
        }
        beginClause(1);
        if (obj.getQueryExpression() != null) {
            visitNode(obj.getQueryExpression());
        } else if (obj.getTupleSource() != null) {
            append(VALUES);
            append(" (...)"); //$NON-NLS-1$
        } else if (obj.getValues() != null) {
            append(VALUES);
            beginClause(2);
            append("("); //$NON-NLS-1$
            registerNodes(obj.getValues(), 0);
            append(")"); //$NON-NLS-1$
        }

        // Option clause
        if (obj.getOption() != null) {
            beginClause(1);
            visitNode(obj.getOption());
        }
    }

    @Override
    public void visit( Create obj ) {
        append(CREATE);
        append(SPACE);
        if (obj.getTableMetadata() != null) {
            append(FOREIGN);
            append(SPACE);
            append(TEMPORARY);
            append(SPACE);
            append(TABLE);
            append(SPACE);

            new DDLVisitor().addTableBody(obj.getTableMetadata());

            append(SPACE);
            append(ON);
            append(SPACE);
            outputLiteral(String.class, false, obj.getOn());
            return;
        }
        append(LOCAL);
        append(SPACE);
        append(TEMPORARY);
        append(SPACE);
        append(TABLE);
        append(SPACE);
        visitNode(obj.getTable());
        append(SPACE);

        // Columns clause
        List<Column> columns = obj.getColumns();
        append("("); //$NON-NLS-1$
        Iterator<Column> iter = columns.iterator();
        while (iter.hasNext()) {
            Column element = iter.next();
            outputDisplayName(element.getName());
            append(SPACE);
            if (element.isAutoIncremented()) {
                append(NonReserved.SERIAL);
            } else {
                append(element.getRuntimeType());
                if (element.getNullType() == NullType.No_Nulls) {
                    append(SPACE);
                    append(NOT);
                    append(SPACE);
                    append(NULL);
                }
            }
            if (iter.hasNext()) {
                append(", "); //$NON-NLS-1$
            }
        }
        if (!obj.getPrimaryKey().isEmpty()) {
            append(", "); //$NON-NLS-1$
            append(PRIMARY);
            append(" "); //$NON-NLS-1$
            append(NonReserved.KEY);
            append(Tokens.LPAREN);
            Iterator<ElementSymbol> pkiter = obj.getPrimaryKey().iterator();
            while (pkiter.hasNext()) {
                outputShortName(pkiter.next());
                if (pkiter.hasNext()) {
                    append(", "); //$NON-NLS-1$
                }
            }
            append(Tokens.RPAREN);
        }
        append(Tokens.RPAREN);
        CommitAction commitAction = obj.getCommitAction();
        if (commitAction != null) {
            append(Tokens.SPACE);
            append(Reserved.ON);
            append(Tokens.SPACE);
            append(Reserved.COMMIT);
            append(Tokens.SPACE);
            switch (commitAction) {
            case PRESERVE_ROWS:
                append(NonReserved.PRESERVE);
                append(Tokens.SPACE);
                append(Reserved.ROWS);
                break;
            }
        }
    }

    @Override
    public void visit( Drop obj ) {
        append(DROP);
        append(SPACE);
        append(TABLE);
        append(SPACE);
        visitNode(obj.getTable());
    }

    @Override
    public void visit( IsNullCriteria obj ) {
        Expression expr = obj.getExpression();
        appendNested(expr);
        append(SPACE);
        append(IS);
        append(SPACE);
        if (obj.isNegated()) {
            append(NOT);
            append(SPACE);
        }
        append(NULL);
    }

    @Override
    public void visit( JoinPredicate obj ) {
        addHintComment(obj);

        if (obj.hasHint()) {
            append("(");//$NON-NLS-1$
        }

        // left clause
        FromClause leftClause = obj.getLeftClause();
        if (leftClause instanceof JoinPredicate && !((JoinPredicate)leftClause).hasHint()) {
            append("("); //$NON-NLS-1$
            visitNode(leftClause);
            append(")"); //$NON-NLS-1$
        } else {
            visitNode(leftClause);
        }

        // join type
        append(SPACE);
        visitNode(obj.getJoinType());
        append(SPACE);

        // right clause
        FromClause rightClause = obj.getRightClause();
        if (rightClause instanceof JoinPredicate && !((JoinPredicate)rightClause).hasHint()) {
            append("("); //$NON-NLS-1$
            visitNode(rightClause);
            append(")"); //$NON-NLS-1$
        } else {
            visitNode(rightClause);
        }

        // join criteria
        List joinCriteria = obj.getJoinCriteria();
        if (joinCriteria != null && joinCriteria.size() > 0) {
            append(SPACE);
            append(ON);
            append(SPACE);
            Iterator critIter = joinCriteria.iterator();
            while (critIter.hasNext()) {
                Criteria crit = (Criteria)critIter.next();
                if (crit instanceof PredicateCriteria || crit instanceof NotCriteria) {
                    visitNode(crit);
                } else {
                    append("("); //$NON-NLS-1$
                    visitNode(crit);
                    append(")"); //$NON-NLS-1$
                }

                if (critIter.hasNext()) {
                    append(SPACE);
                    append(AND);
                    append(SPACE);
                }
            }
        }

        if (obj.hasHint()) {
            append(")"); //$NON-NLS-1$
        }
    }

    private void addHintComment( FromClause obj ) {
        if (obj.hasHint()) {
            append(BEGIN_HINT);
            append(SPACE);
            if (obj.isOptional()) {
                append(Option.OPTIONAL);
                append(SPACE);
            }
            if (obj.getMakeDep() != null) {
                append(Option.MAKEDEP);
                appendMakeDepOptions(obj.getMakeDep());
                append(SPACE);
            }
            if (obj.isMakeNotDep()) {
                append(Option.MAKENOTDEP);
                append(SPACE);
            }
            if (obj.getMakeInd() != null) {
                append(MAKEIND);
                appendMakeDepOptions(obj.getMakeInd());
                append(SPACE);
            }
            if (obj.isNoUnnest()) {
                append(SubqueryHint.NOUNNEST);
                append(SPACE);
            }
            if (obj.isPreserve()) {
                append(FromClause.PRESERVE);
                append(SPACE);
            }
            append(END_HINT);
            append(SPACE);
        }
    }

    @Override
    public void visit( JoinType obj ) {
        String[] output = null;
        if (obj.equals(JoinType.JOIN_INNER)) {
            output = new String[] {INNER, SPACE, JOIN};
        } else if (obj.equals(JoinType.JOIN_CROSS)) {
            output = new String[] {CROSS, SPACE, JOIN};
        } else if (obj.equals(JoinType.JOIN_LEFT_OUTER)) {
            output = new String[] {LEFT, SPACE, OUTER, SPACE, JOIN};
        } else if (obj.equals(JoinType.JOIN_RIGHT_OUTER)) {
            output = new String[] {RIGHT, SPACE, OUTER, SPACE, JOIN};
        } else if (obj.equals(JoinType.JOIN_FULL_OUTER)) {
            output = new String[] {FULL, SPACE, OUTER, SPACE, JOIN};
        } else if (obj.equals(JoinType.JOIN_UNION)) {
            output = new String[] {UNION, SPACE, JOIN};
        } else if (obj.equals(JoinType.JOIN_SEMI)) {
            output = new String[] {"SEMI", SPACE, JOIN}; //$NON-NLS-1$
        } else if (obj.equals(JoinType.JOIN_ANTI_SEMI)) {
            output = new String[] {"ANTI SEMI", SPACE, JOIN}; //$NON-NLS-1$
        } else {
            throw new AssertionError();
        }
        for (String part : output) {
            append(part);
        }
    }

    @Override
    public void visit( MatchCriteria obj ) {
        visitNode(obj.getLeftExpression());

        append(SPACE);
        if (obj.isNegated()) {
            append(NOT);
            append(SPACE);
        }
        switch (obj.getMode()) {
        case SIMILAR:
            append(SIMILAR);
            append(SPACE);
            append(TO);
            break;
        case LIKE:
            append(LIKE);
            break;
        case REGEX:
            append(LIKE_REGEX);
            break;
        }
        append(SPACE);

        visitNode(obj.getRightExpression());

        if (obj.getEscapeChar() != MatchCriteria.NULL_ESCAPE_CHAR) {
            append(SPACE);
            append(ESCAPE);
            append(SPACE);
            outputLiteral(String.class, false, obj.getEscapeChar());
        }
    }

    @Override
    public void visit( NotCriteria obj ) {
        append(NOT);
        append(" ("); //$NON-NLS-1$
        visitNode(obj.getCriteria());
        append(")"); //$NON-NLS-1$
    }

    @Override
    public void visit( Option obj ) {
        append(OPTION);

        Collection<String> groups = obj.getDependentGroups();
        if (groups != null && groups.size() > 0) {
            append(" "); //$NON-NLS-1$
            append(MAKEDEP);
            append(" "); //$NON-NLS-1$

            Iterator<String> iter = groups.iterator();
            Iterator<MakeDep> iter1 = obj.getMakeDepOptions().iterator();

            while (iter.hasNext()) {
                outputDisplayName(iter.next());

                appendMakeDepOptions(iter1.next());

                if (iter.hasNext()) {
                    append(", ");//$NON-NLS-1$
                }
            }
        }

        groups = obj.getNotDependentGroups();
        if (groups != null && groups.size() > 0) {
            append(" "); //$NON-NLS-1$
            append(MAKENOTDEP);
            append(" "); //$NON-NLS-1$

            Iterator<String> iter = groups.iterator();

            while (iter.hasNext()) {
                outputDisplayName(iter.next());

                if (iter.hasNext()) {
                    append(", ");//$NON-NLS-1$
                }
            }
        }

        groups = obj.getNoCacheGroups();
        if (groups != null && groups.size() > 0) {
            append(" "); //$NON-NLS-1$
            append(NOCACHE);
            append(" "); //$NON-NLS-1$

            Iterator<String> iter = groups.iterator();

            while (iter.hasNext()) {
                outputDisplayName(iter.next());

                if (iter.hasNext()) {
                    append(", ");//$NON-NLS-1$
                }
            }
        } else if (obj.isNoCache()) {
            append(" "); //$NON-NLS-1$
            append(NOCACHE);
        }

    }

    public SQLStringVisitor appendMakeDepOptions(MakeDep makedep) {
        boolean parens = false;
        if (makedep.getMax() != null || makedep.getJoin() != null) {
            append(Tokens.LPAREN);
            parens = true;
        }
        boolean space = false;
        if (makedep.getMax() != null) {
            if (space) {
                append(SPACE);
            } else {
                space = true;
            }
            append(NonReserved.MAX);
            append(Tokens.COLON);
            append(makedep.getMax());
        }
        if (makedep.getJoin() != null) {
            if (space) {
                append(SPACE);
            } else {
                space = true;
            }
            if (!makedep.getJoin()) {
                append(NO);
                append(SPACE);
            }
            append(JOIN);
        }
        if (parens) {
            append(Tokens.RPAREN);
        }
        return this;
    }

    @Override
    public void visit( OrderBy obj ) {
        append(ORDER);
        append(SPACE);
        append(BY);
        append(SPACE);
        registerNodes(obj.getOrderByItems(), 0);
    }

    @Override
    public void visit( OrderByItem obj ) {
        Expression ses = obj.getSymbol();
        if (ses instanceof AliasSymbol) {
            AliasSymbol as = (AliasSymbol)ses;
            outputDisplayName(as.getOutputName());
        } else {
            visitNode(ses);
        }
        if (!obj.isAscending()) {
            append(SPACE);
            append(DESC);
        } // Don't print default "ASC"
        if (obj.getNullOrdering() != null) {
            append(SPACE);
            append(NonReserved.NULLS);
            append(SPACE);
            append(obj.getNullOrdering().name());
        }
    }

    @Override
    public void visit( DynamicCommand obj ) {
        append(EXECUTE);
        append(SPACE);
        append(IMMEDIATE);
        append(SPACE);
        visitNode(obj.getSql());

        if (obj.isAsClauseSet()) {
            beginClause(1);
            append(AS);
            append(SPACE);
            for (int i = 0; i < obj.getAsColumns().size(); i++) {
                ElementSymbol symbol = (ElementSymbol)obj.getAsColumns().get(i);
                outputShortName(symbol);
                append(SPACE);
                append(DataTypeManager.getDataTypeName(symbol.getType()));
                if (i < obj.getAsColumns().size() - 1) {
                    append(", "); //$NON-NLS-1$
                }
            }
        }

        if (obj.getIntoGroup() != null) {
            beginClause(1);
            append(INTO);
            append(SPACE);
            visitNode(obj.getIntoGroup());
        }

        if (obj.getUsing() != null && !obj.getUsing().isEmpty()) {
            beginClause(1);
            append(USING);
            append(SPACE);
            visitNode(obj.getUsing());
        }

        if (obj.getUpdatingModelCount() > 0) {
            beginClause(1);
            append(UPDATE);
            append(SPACE);
            if (obj.getUpdatingModelCount() > 1) {
                append("*"); //$NON-NLS-1$
            } else {
                append("1"); //$NON-NLS-1$
            }
        }
    }

    @Override
    public void visit( SetClauseList obj ) {
        for (Iterator<SetClause> iterator = obj.getClauses().iterator(); iterator.hasNext();) {
            SetClause clause = iterator.next();
            visitNode(clause);
            if (iterator.hasNext()) {
                append(", "); //$NON-NLS-1$
            }
        }
    }

    @Override
    public void visit( SetClause obj ) {
        ElementSymbol symbol = obj.getSymbol();
        outputShortName(symbol);
        append(" = "); //$NON-NLS-1$
        visitNode(obj.getValue());
    }

    @Override
    public void visit(WithQueryCommand obj) {
        visitNode(obj.getGroupSymbol());
        append(SPACE);
        if (obj.getColumns() != null && !obj.getColumns().isEmpty()) {
            append(Tokens.LPAREN);
            shortNameOnly = true;
            registerNodes(obj.getColumns(), 0);
            shortNameOnly = false;
            append(Tokens.RPAREN);
            append(SPACE);
        }
        append(AS);
        if (obj.isMaterialize()) {
            append(SPACE);
            append(BEGIN_HINT);
            append(SPACE);
            append(WithQueryCommand.MATERIALIZE);
            append(SPACE);
            append(END_HINT);
        } else if (obj.isNoInline()) {
            append(SPACE);
            append(BEGIN_HINT);
            append(SPACE);
            append(WithQueryCommand.NO_INLINE);
            append(SPACE);
            append(END_HINT);
        }
        append(SPACE);
        append(Tokens.LPAREN);
        if (obj.getCommand() == null) {
            append("<dependent values>"); //$NON-NLS-1$
        } else {
            visitNode(obj.getCommand());
        }
        append(Tokens.RPAREN);
    }

    @Override
    public void visit( Query obj ) {
        addCacheHint(obj.getCacheHint());
        addWithClause(obj);
        append(SELECT);

        SourceHint sh = obj.getSourceHint();
        addSourceHint(sh);
        if (obj.getSelect() != null) {
            visitNode(obj.getSelect());
        }

        if (obj.getInto() != null) {
            beginClause(1);
            visitNode(obj.getInto());
        }

        if (obj.getFrom() != null) {
            beginClause(1);
            visitNode(obj.getFrom());
        }

        // Where clause
        if (obj.getCriteria() != null) {
            beginClause(1);
            visitCriteria(WHERE, obj.getCriteria());
        }

        // Group by clause
        if (obj.getGroupBy() != null) {
            beginClause(1);
            visitNode(obj.getGroupBy());
        }

        // Having clause
        if (obj.getHaving() != null) {
            beginClause(1);
            visitCriteria(HAVING, obj.getHaving());
        }

        // Order by clause
        if (obj.getOrderBy() != null) {
            beginClause(1);
            visitNode(obj.getOrderBy());
        }

        if (obj.getLimit() != null) {
            beginClause(1);
            visitNode(obj.getLimit());
        }

        // Option clause
        if (obj.getOption() != null) {
            beginClause(1);
            visitNode(obj.getOption());
        }
    }

    private void addSourceHint(SourceHint sh) {
        if (sh != null) {
            append(SPACE);
            append(BEGIN_HINT);
            append("sh"); //$NON-NLS-1$
            if (sh.isUseAliases()) {
                append(SPACE);
                append("KEEP ALIASES"); //$NON-NLS-1$
            }
            if (sh.getGeneralHint() != null) {
                appendSourceHintValue(sh.getGeneralHint());
            } else {
                append(SPACE);
            }
            if (sh.getSpecificHints() != null) {
                for (Map.Entry<String, SpecificHint> entry : sh.getSpecificHints().entrySet()) {
                    append(entry.getKey());
                    if (entry.getValue().isUseAliases()) {
                        append(SPACE);
                        append("KEEP ALIASES"); //$NON-NLS-1$
                    }
                    appendSourceHintValue(entry.getValue().getHint());
                }
            }
            append(END_HINT);
        }
    }

    private void addWithClause(QueryCommand obj) {
        if (obj.getWith() != null) {
            append(WITH);
            append(SPACE);
            registerNodes(obj.getWith(), 0);
            beginClause(0);
        }
    }

    protected void visitCriteria( String keyWord,
                                  Criteria crit ) {
        append(keyWord);
        append(SPACE);
        visitNode(crit);
    }

    @Override
    public void visit( SearchedCaseExpression obj ) {
        append(CASE);
        for (int i = 0; i < obj.getWhenCount(); i++) {
            append(SPACE);
            append(WHEN);
            append(SPACE);
            visitNode(obj.getWhenCriteria(i));
            append(SPACE);
            append(THEN);
            append(SPACE);
            visitNode(obj.getThenExpression(i));
        }
        append(SPACE);
        if (obj.getElseExpression() != null) {
            append(ELSE);
            append(SPACE);
            visitNode(obj.getElseExpression());
            append(SPACE);
        }
        append(END);
    }

    @Override
    public void visit( Select obj ) {
        if (obj.isDistinct()) {
            append(SPACE);
            append(DISTINCT);
        }
        beginClause(2);

        Iterator<Expression> iter = obj.getSymbols().iterator();
        while (iter.hasNext()) {
            Expression symbol = iter.next();
            visitNode(symbol);
            if (iter.hasNext()) {
                append(", "); //$NON-NLS-1$
            }
        }
    }

    private void appendSourceHintValue(String sh) {
        append(Tokens.COLON);
        append('\'');
        append(escapeStringValue(sh, "'")); //$NON-NLS-1$
        append('\'');
        append(SPACE);
    }

    @Override
    public void visit( SetCriteria obj ) {
        // variable
        appendNested(obj.getExpression());

        // operator and beginning of list
        append(SPACE);
        if (obj.isNegated()) {
            append(NOT);
            append(SPACE);
        }
        append(IN);
        append(" ("); //$NON-NLS-1$

        // value list
        Collection vals = obj.getValues();
        int size = vals.size();
        if (size == 1) {
            Iterator iter = vals.iterator();
            Expression expr = (Expression)iter.next();
            visitNode(expr);
        } else if (size > 1) {
            Iterator iter = vals.iterator();
            Expression expr = (Expression)iter.next();
            visitNode(expr);
            while (iter.hasNext()) {
                expr = (Expression)iter.next();
                append(", "); //$NON-NLS-1$
                visitNode(expr);
            }
        }
        append(")"); //$NON-NLS-1$
    }

    /**
     * Condition operators have lower precedence than LIKE/SIMILAR/IS
     * @param ex
     */
    private void appendNested(Expression ex) {
        boolean useParens = ex instanceof Criteria;
        if (useParens) {
            append(Tokens.LPAREN);
        }
        visitNode(ex);
        if (useParens) {
            append(Tokens.RPAREN);
        }
    }

    @Override
    public void visit( SetQuery obj ) {
        addCacheHint(obj.getCacheHint());
        addWithClause(obj);
        QueryCommand query = obj.getLeftQuery();
        appendSetQuery(obj, query, false);

        beginClause(0);
        append(obj.getOperation());

        if (obj.isAll()) {
            append(SPACE);
            append(ALL);
        }
        beginClause(0);
        query = obj.getRightQuery();
        appendSetQuery(obj, query, true);

        if (obj.getOrderBy() != null) {
            beginClause(0);
            visitNode(obj.getOrderBy());
        }

        if (obj.getLimit() != null) {
            beginClause(0);
            visitNode(obj.getLimit());
        }

        if (obj.getOption() != null) {
            beginClause(0);
            visitNode(obj.getOption());
        }
    }

    protected void appendSetQuery( SetQuery parent,
                                   QueryCommand obj,
                                   boolean right ) {
        if (obj.getLimit() != null || obj.getOrderBy() != null || (obj instanceof SetQuery
            && ((right && parent.isAll() && !((SetQuery)obj).isAll())
                    || ((parent.getOperation() == Operation.INTERSECT || right)
                            && parent.getOperation() != ((SetQuery)obj).getOperation())))) {
            append(Tokens.LPAREN);
            visitNode(obj);
            append(Tokens.RPAREN);
        } else {
            visitNode(obj);
        }
    }

    @Override
    public void visit( StoredProcedure obj ) {
        addCacheHint(obj.getCacheHint());
        if (obj.isCalledWithReturn()) {
            for (SPParameter param : obj.getParameters()) {
                if (param.getParameterType() == SPParameter.RETURN_VALUE) {
                    if (param.getExpression() == null) {
                        append("?"); //$NON-NLS-1$
                    } else {
                        visitNode(param.getExpression());
                    }
                }
            }
            append(SPACE);
            append(Tokens.EQ);
            append(SPACE);
        }
        // exec clause
        append(EXEC);
        append(SPACE);
        append(obj.getProcedureName());
        append("("); //$NON-NLS-1$
        boolean first = true;
        for (SPParameter param : obj.getParameters()) {
            if (param.isUsingDefault() || param.getParameterType() == SPParameter.RETURN_VALUE
                    || param.getParameterType() == SPParameter.RESULT_SET || param.getExpression() == null) {
                continue;
            }
            if (first) {
                first = false;
            } else {
                append(", "); //$NON-NLS-1$
            }
            if (obj.displayNamedParameters()) {
                append(escapeSinglePart(Symbol.getShortName(param.getParameterSymbol().getOutputName())));
                append(" => "); //$NON-NLS-1$
            }

            boolean addParens = !obj.displayNamedParameters() && param.getExpression() instanceof CompareCriteria;
            if (addParens) {
                append(Tokens.LPAREN);
            }
            visitNode(param.getExpression());
            if (addParens) {
                append(Tokens.RPAREN);
            }
        }
        append(")"); //$NON-NLS-1$

        // Option clause
        if (obj.getOption() != null) {
            beginClause(1);
            visitNode(obj.getOption());
        }
    }

    public void addCacheHint( CacheHint obj ) {
        if (obj == null) {
            return;
        }
        append(BEGIN_HINT);
        append(SPACE);
        append(CacheHint.CACHE);
        boolean addParens = false;
        if (obj.isPrefersMemory()) {
            append(Tokens.LPAREN);
            addParens = true;
            append(CacheHint.PREF_MEM);
        }
        if (obj.getTtl() != null) {
            if (!addParens) {
                append(Tokens.LPAREN);
                addParens = true;
            } else {
                append(SPACE);
            }
            append(CacheHint.TTL);
            append(obj.getTtl());
        }
        if (obj.getUpdatable() != null) {
            if (!addParens) {
                append(Tokens.LPAREN);
                addParens = true;
            } else {
                append(SPACE);
            }
            append(CacheHint.UPDATABLE);
        }
        if (obj.getScope() != null) {
            if (!addParens) {
                append(Tokens.LPAREN);
                addParens = true;
            } else {
                append(SPACE);
            }
            append(CacheHint.SCOPE);
            append(obj.getScope());
        }
        if (obj.getMinRows() != null) {
            if (!addParens) {
                append(Tokens.LPAREN);
                addParens = true;
            } else {
                append(SPACE);
            }
            append(CacheHint.MIN);
            append(obj.getMinRows());
        }
        if (addParens) {
            append(Tokens.RPAREN);
        }
        append(SPACE);
        append(END_HINT);
        beginClause(0);
    }

    @Override
    public void visit( SubqueryFromClause obj ) {
        addHintComment(obj);
        if (obj.isLateral()) {
            append(LATERAL);
        }
        append("(");//$NON-NLS-1$
        visitNode(obj.getCommand());
        append(")");//$NON-NLS-1$
        append(" AS ");//$NON-NLS-1$
        append(escapeSinglePart(obj.getOutputName()));
    }

    @Override
    public void visit( SubquerySetCriteria obj ) {
        // variable
        visitNode(obj.getExpression());

        // operator and beginning of list
        append(SPACE);
        if (obj.isNegated()) {
            append(NOT);
            append(SPACE);
        }
        append(IN);
        addSubqueryHint(obj.getSubqueryHint());
        append(" ("); //$NON-NLS-1$
        visitNode(obj.getCommand());
        append(")"); //$NON-NLS-1$
    }

    @Override
    public void visit( UnaryFromClause obj ) {
        addHintComment(obj);
        if (obj.getExpandedCommand() != null) {
            append("(");//$NON-NLS-1$
            visitNode(obj.getExpandedCommand());
            append(")");//$NON-NLS-1$
            append(" AS ");//$NON-NLS-1$
            append(escapeSinglePart(obj.getGroup().getName()));
        } else {
            visitNode(obj.getGroup());
        }
    }

    @Override
    public void visit( Update obj ) {
        // Update clause
        append(UPDATE);
        addSourceHint(obj.getSourceHint());
        append(SPACE);
        visitNode(obj.getGroup());
        beginClause(1);
        // Set clause
        append(SET);
        beginClause(2);
        visitNode(obj.getChangeList());

        // Where clause
        if (obj.getCriteria() != null) {
            beginClause(1);
            visitCriteria(WHERE, obj.getCriteria());
        }

        // Option clause
        if (obj.getOption() != null) {
            beginClause(1);
            visitNode(obj.getOption());
        }
    }

    @Override
    public void visit( Into obj ) {
        append(INTO);
        append(SPACE);
        visitNode(obj.getGroup());
    }

    // ############ Visitor methods for symbol objects ####################

    @Override
    public void visit( AggregateSymbol obj ) {
        append(obj.getName());
        append("("); //$NON-NLS-1$

        if (obj.isDistinct()) {
            append(DISTINCT);
            append(" "); //$NON-NLS-1$
        } else if (obj.getAggregateFunction() == Type.USER_DEFINED) {
            //TODO: left in to help the parser, but can be removed
            append(ALL);
            append(" "); //$NON-NLS-1$
        }

        if (obj.getArgs().length == 0) {
            if (obj.isCount()) {
                append(Tokens.ALL_COLS);
            }
        } else {
            registerNodes(obj.getArgs(), 0);
        }

        if (obj.getOrderBy() != null) {
            append(SPACE);
            visitNode(obj.getOrderBy());
        }
        append(")"); //$NON-NLS-1$

        if (obj.getCondition() != null) {
            append(SPACE);
            append(FILTER);
            append(Tokens.LPAREN);
            append(WHERE);
            append(SPACE);
            append(obj.getCondition());
            append(Tokens.RPAREN);
        }
    }

    @Override
    public void visit( AliasSymbol obj ) {
        visitNode(obj.getSymbol());
        append(SPACE);
        append(AS);
        append(SPACE);
        append(escapeSinglePart(obj.getOutputName()));
    }

    @Override
    public void visit( MultipleElementSymbol obj ) {
        if (obj.getGroup() == null) {
            append(Tokens.ALL_COLS);
        } else {
            if (isSinglePart(obj.getGroup())) {
                append(escapeSinglePart(obj.getGroup().getOutputName()));
            } else {
                visitNode(obj.getGroup());
            }
            append(Tokens.DOT);
            append(Tokens.ALL_COLS);
        }
    }

    @Override
    public void visit( Constant obj ) {
        Class<?> type = obj.getType();
        boolean multiValued = obj.isMultiValued();
        Object value = obj.getValue();
        outputLiteral(type, multiValued, value);
    }

    private void outputLiteral(Class<?> type,
            boolean multiValued, Object value) throws AssertionError {
        String[] constantParts = null;
        if (multiValued) {
            constantParts = new String[] {"?"}; //$NON-NLS-1$
        } else if (value == null) {
            if (type.equals(DataTypeManager.DefaultDataClasses.BOOLEAN)) {
                constantParts = new String[] {UNKNOWN};
            } else {
                constantParts = new String[] {"null"}; //$NON-NLS-1$
            }
        } else {
            if (value.getClass() == ArrayImpl.class) {
                ArrayImpl av = (ArrayImpl)value;
                append(Tokens.LPAREN);
                for (int i = 0; i < av.getValues().length; i++) {
                    if (i > 0) {
                        append(Tokens.COMMA);
                        append(SPACE);
                    }
                    Object value2 = av.getValues()[i];
                    outputLiteral(value2!=null?value2.getClass():av.getValues().getClass().getComponentType(), multiValued, value2);
                }
                if (av.getValues().length == 1) {
                    append(Tokens.COMMA);
                }
                append(Tokens.RPAREN);
                return;
            } else if (type.isArray()) {
                append(Tokens.LPAREN);
                int length = java.lang.reflect.Array.getLength(value);
                for (int i = 0; i < length; i++) {
                    if (i > 0) {
                        append(Tokens.COMMA);
                        append(SPACE);
                    }
                    Object value2 = java.lang.reflect.Array.get(value, i);
                    outputLiteral(type.getComponentType(), multiValued, value2);
                }
                if (length == 1) {
                    append(Tokens.COMMA);
                }
                append(Tokens.RPAREN);
                return;
            }
            if (Number.class.isAssignableFrom(type)) {
                constantParts = new String[] {value.toString()};
            } else if (type.equals(DataTypeManager.DefaultDataClasses.BOOLEAN)) {
                constantParts = new String[] {value.equals(Boolean.TRUE) ? TRUE : FALSE};
            } else if (type.equals(DataTypeManager.DefaultDataClasses.TIMESTAMP)) {
                constantParts = new String[] {"{ts'", value.toString(), "'}"}; //$NON-NLS-1$ //$NON-NLS-2$
            } else if (type.equals(DataTypeManager.DefaultDataClasses.TIME)) {
                constantParts = new String[] {"{t'", value.toString(), "'}"}; //$NON-NLS-1$ //$NON-NLS-2$
            } else if (type.equals(DataTypeManager.DefaultDataClasses.DATE)) {
                constantParts = new String[] {"{d'", value.toString(), "'}"}; //$NON-NLS-1$ //$NON-NLS-2$
            } else if (type.equals(DataTypeManager.DefaultDataClasses.VARBINARY)) {
                constantParts = new String[] {"X'", value.toString(), "'"}; //$NON-NLS-1$ //$NON-NLS-2$
            }
            if (constantParts == null) {
                if (DataTypeManager.isLOB(type)) {
                    constantParts = new String[] {"?"}; //$NON-NLS-1$
                } else {
                    append('\'');
                    String strValue = value.toString();
                    for (int i = 0; i < strValue.length(); i++) {
                        char c = strValue.charAt(i);
                        if (c == '\'') {
                            parts.append('\'');
                        } else if (Character.isISOControl(c)) {
                            parts.append("\\u" + PropertiesUtils.toHex((c >> 12) & 0xF) + PropertiesUtils.toHex((c >>  8) & 0xF) //$NON-NLS-1$
                                    + PropertiesUtils.toHex((c >>  4) & 0xF) + PropertiesUtils.toHex(c & 0xF));
                            continue;
                        }
                        parts.append(c);
                    }
                    parts.append('\'');
                    return;
                }
            }
        }

        for (String string : constantParts) {
            append(string);
        }
    }

    /**
     * Take a string literal and escape it as necessary. By default, this converts ' to ''.
     *
     * @param str String literal value (unquoted), never null
     * @return Escaped string literal value
     */
    static String escapeStringValue( String str,
                                     String tick ) {
        return StringUtil.replaceAll(str, tick, tick + tick);
    }

    @Override
    public void visit( ElementSymbol obj ) {
        if (obj.getDisplayMode().equals(ElementSymbol.DisplayMode.SHORT_OUTPUT_NAME) || shortNameOnly) {
            outputShortName(obj);
            return;
        }
        String name = obj.getOutputName();
        //always use full qualification to avoid stripping quotes when an alias with a . is used
        //we can detect this easily with getDefinition != null, but to avoid overlap with existing
        //logic, such as procedure relational naming, we need to only affect non-scalar temp metadata
        GroupSymbol groupSymbol = obj.getGroupSymbol();
        if (name.contains(ElementSymbol.SEPARATOR)
                && groupSymbol != null
                && isSinglePart(groupSymbol)
                && groupSymbol.getOutputName().contains(ElementSymbol.SEPARATOR)) {
            append(escapeSinglePart(groupSymbol.getOutputName()));
            append(ElementSymbol.SEPARATOR);
            outputShortName(obj);
            return;
        }
        if (obj.getDisplayMode().equals(ElementSymbol.DisplayMode.FULLY_QUALIFIED)) {
            name = obj.getName();
        }
        outputDisplayName(name);
    }

    private boolean isSinglePart(GroupSymbol groupSymbol) {
        return (groupSymbol.getDefinition() != null || (!groupSymbol.isProcedure()
                && groupSymbol.getMetadataID() instanceof TempMetadataID
                && ((TempMetadataID) groupSymbol.getMetadataID())
                        .getMetadataType() != TempMetadataID.Type.SCALAR));
    }

    private void outputShortName( ElementSymbol obj ) {
        outputDisplayName(Symbol.getShortName(obj.getOutputName()));
    }

    private void outputDisplayName( String name ) {
        int start = 0;
        int end = 0;
        while (true) {
            end = name.indexOf(Symbol.SEPARATOR, end);
            if (end == -1) {
                append(escapeSinglePart(name.substring(start, name.length())));
                return;
            }

            while (end < name.length() - 1 && name.charAt(end + 1) == '.') {
                end++;
            }

            if (start != end) {
                if (end == name.length() -1) {
                    end++;
                }
                append(escapeSinglePart(name.substring(start, end)));
                if (end == name.length()) {
                    return;
                }
                end++;
                start = end;
                append(Symbol.SEPARATOR);
            } else {
                end++;
            }
        }
    }

    @Override
    public void visit( ExpressionSymbol obj ) {
        visitNode(obj.getExpression());
    }

    @Override
    public void visit( Function obj ) {
        String name = obj.getName();
        Expression[] args = obj.getArgs();
        if (obj.isImplicit()) {
            // Hide this function, which is implicit
            visitNode(args[0]);

        } else if (name.equalsIgnoreCase(CONVERT) || name.equalsIgnoreCase(CAST) || name.equalsIgnoreCase(XMLCAST)) {
            append(name);
            append("("); //$NON-NLS-1$

            if (args != null && args.length > 0) {
                visitNode(args[0]);

                if (name.equalsIgnoreCase(CONVERT)) {
                    append(", "); //$NON-NLS-1$
                } else {
                    append(" "); //$NON-NLS-1$
                    append(AS);
                    append(" "); //$NON-NLS-1$
                }

                if (args.length < 2 || args[1] == null || !(args[1] instanceof Constant)) {
                    append(UNDEFINED);
                } else {
                    append(((Constant)args[1]).getValue());
                }
            }
            append(")"); //$NON-NLS-1$

        } else if (INFIX_FUNCTIONS.contains(name)) {
            append("("); //$NON-NLS-1$

            if (args != null) {
                for (int i = 0; i < args.length; i++) {
                    visitNode(args[i]);
                    if (i < (args.length - 1)) {
                        append(SPACE);
                        append(name);
                        append(SPACE);
                    }
                }
            }
            append(")"); //$NON-NLS-1$

        } else if (name.equalsIgnoreCase(NonReserved.TIMESTAMPADD) || name.equalsIgnoreCase(NonReserved.TIMESTAMPDIFF)) {
            append(name);
            append("("); //$NON-NLS-1$

            if (args != null && args.length > 0) {
                append(((Constant)args[0]).getValue());
                registerNodes(args, 1);
            }
            append(")"); //$NON-NLS-1$

        } else if (name.equalsIgnoreCase(SourceSystemFunctions.XMLPI)) {
            append(name);
            append("(NAME "); //$NON-NLS-1$
            outputDisplayName((String)((Constant)args[0]).getValue());
            registerNodes(args, 1);
            append(")"); //$NON-NLS-1$
        } else if (name.equalsIgnoreCase(SourceSystemFunctions.TRIM)) {
            append(name);
            append(SQLConstants.Tokens.LPAREN);
            String value = (String)((Constant)args[0]).getValue();
            if (!value.equalsIgnoreCase(BOTH)) {
                append(((Constant)args[0]).getValue());
                append(" "); //$NON-NLS-1$
            }
            append(args[1]);
            append(" "); //$NON-NLS-1$
            append(FROM);
            append(" "); //$NON-NLS-1$
            append(args[2]);
            append(")"); //$NON-NLS-1$
        } else {
            append(name);
            if ((args != null && args.length > 0) || (!name.equalsIgnoreCase(Reserved.CURRENT_TIME) && !name.equalsIgnoreCase(Reserved.CURRENT_TIMESTAMP))) {
                append("("); //$NON-NLS-1$
                registerNodes(args, 0);
                append(")"); //$NON-NLS-1$
            }
        }
    }

    private void registerNodes( LanguageObject[] objects,
                                int begin ) {
        registerNodes(Arrays.asList(objects), begin);
    }

    private void registerNodes( List<? extends LanguageObject> objects,
                                int begin ) {
        for (int i = begin; i < objects.size(); i++) {
            if (i > 0) {
                append(", "); //$NON-NLS-1$
            }
            visitNode(objects.get(i));
        }
    }

    @Override
    public void visit( GroupSymbol obj ) {
        String alias = null;
        String fullGroup = obj.getOutputName();
        if (obj.getOutputDefinition() != null) {
            alias = obj.getOutputName();
            fullGroup = obj.getOutputDefinition();
        }

        outputDisplayName(fullGroup);

        if (alias != null) {
            append(SPACE);
            append(AS);
            append(SPACE);
            append(escapeSinglePart(alias));
        }
    }

    @Override
    public void visit( Reference obj ) {
        if (!obj.isPositional() && obj.getExpression() != null) {
            visitNode(obj.getExpression());
        } else {
            append("?"); //$NON-NLS-1$
        }
    }

    // ############ Visitor methods for storedprocedure language objects ####################

    @Override
    public void visit( Block obj ) {
        addLabel(obj);
        List<Statement> statements = obj.getStatements();
        // Add first clause
        append(BEGIN);
        if (obj.isAtomic()) {
            append(SPACE);
            append(ATOMIC);
        }
        append("\n"); //$NON-NLS-1$
        addStatements(statements);
        if (obj.getExceptionGroup() != null) {
            append(NonReserved.EXCEPTION);
            append(SPACE);
            outputDisplayName(obj.getExceptionGroup());
            append("\n"); //$NON-NLS-1$
            if (obj.getExceptionStatements() != null) {
                addStatements(obj.getExceptionStatements());
            }
        }
        append(END);
    }

    private void addStatements(List<Statement> statements) {
        Iterator<Statement> stmtIter = statements.iterator();
        while (stmtIter.hasNext()) {
            // Add each statement
            addTabs(1);
            visitNode(stmtIter.next());
            append("\n"); //$NON-NLS-1$
        }
        addTabs(0);
    }

    private void addLabel(Labeled obj) {
        if (obj.getLabel() != null) {
            outputDisplayName(obj.getLabel());
            append(SPACE);
            append(Tokens.COLON);
            append(SPACE);
        }
    }

    /**
     * @param level
     */
    protected void addTabs( int level ) {
    }

    @Override
    public void visit( CommandStatement obj ) {
        visitNode(obj.getCommand());
        if (!obj.isReturnable()) {
            append(SPACE);
            append(WITHOUT);
            append(SPACE);
            append(RETURN);
        }
        append(";"); //$NON-NLS-1$
    }

    @Override
    public void visit( CreateProcedureCommand obj ) {
        addCacheHint(obj.getCacheHint());
        visitNode(obj.getBlock());
    }

    @Override
    public void visit( DeclareStatement obj ) {
        append(DECLARE);
        append(SPACE);
        append(obj.getVariableType());
        append(SPACE);
        createAssignment(obj);
    }

    /**
     * @param obj
     */
    private void createAssignment( AssignmentStatement obj ) {
        visitNode(obj.getVariable());
        if (obj.getExpression() != null) {
            append(" = "); //$NON-NLS-1$
            visitNode(obj.getExpression());
        }
        append(";"); //$NON-NLS-1$
    }

    @Override
    public void visit( IfStatement obj ) {
        append(IF);
        append("("); //$NON-NLS-1$
        visitNode(obj.getCondition());
        append(")\n"); //$NON-NLS-1$
        addTabs(0);
        visitNode(obj.getIfBlock());
        if (obj.hasElseBlock()) {
            append("\n"); //$NON-NLS-1$
            addTabs(0);
            append(ELSE);
            append("\n"); //$NON-NLS-1$
            addTabs(0);
            visitNode(obj.getElseBlock());
        }
    }

    @Override
    public void visit( AssignmentStatement obj ) {
        createAssignment(obj);
    }

    @Override
    public void visit( RaiseStatement obj ) {
        append(NonReserved.RAISE);
        append(SPACE);
        if (obj.isWarning()) {
            append(SQLWARNING);
            append(SPACE);
        }
        visitNode(obj.getExpression());
        append(";"); //$NON-NLS-1$
    }

    @Override
    public void visit(ExceptionExpression exceptionExpression) {
        append(SQLEXCEPTION);
        append(SPACE);
        visitNode(exceptionExpression.getMessage());
        if (exceptionExpression.getSqlState() != null) {
            append(SPACE);
            append(SQLSTATE);
            append(SPACE);
            append(exceptionExpression.getSqlState());
            if (exceptionExpression.getErrorCode() != null) {
                append(Tokens.COMMA);
                append(SPACE);
                append(exceptionExpression.getErrorCode());
            }
        }
        if (exceptionExpression.getParent() != null) {
            append(SPACE);
            append(NonReserved.CHAIN);
            append(SPACE);
            append(exceptionExpression.getParent());
        }
    }

    @Override
    public void visit(ReturnStatement obj) {
        append(RETURN);
        if (obj.getExpression() != null) {
            append(SPACE);
            visitNode(obj.getExpression());
        }
        append(Tokens.SEMICOLON);
    }

    @Override
    public void visit( BranchingStatement obj ) {
        switch (obj.getMode()) {
        case CONTINUE:
            append(CONTINUE);
            break;
        case BREAK:
            append(BREAK);
            break;
        case LEAVE:
            append(LEAVE);
            break;
        }
        if (obj.getLabel() != null) {
            append(SPACE);
            outputDisplayName(obj.getLabel());
        }
        append(";"); //$NON-NLS-1$
    }

    @Override
    public void visit( LoopStatement obj ) {
        addLabel(obj);
        append(LOOP);
        append(" "); //$NON-NLS-1$
        append(ON);
        append(" ("); //$NON-NLS-1$
        visitNode(obj.getCommand());
        append(") "); //$NON-NLS-1$
        append(AS);
        append(" "); //$NON-NLS-1$
        outputDisplayName(obj.getCursorName());
        append("\n"); //$NON-NLS-1$
        addTabs(0);
        visitNode(obj.getBlock());
    }

    @Override
    public void visit( WhileStatement obj ) {
        addLabel(obj);
        append(WHILE);
        append("("); //$NON-NLS-1$
        visitNode(obj.getCondition());
        append(")\n"); //$NON-NLS-1$
        addTabs(0);
        visitNode(obj.getBlock());
    }

    @Override
    public void visit( ExistsCriteria obj ) {
        if (obj.isNegated()) {
            append(NOT);
            append(SPACE);
        }
        append(EXISTS);
        addSubqueryHint(obj.getSubqueryHint());
        append(" ("); //$NON-NLS-1$
        visitNode(obj.getCommand());
        append(")"); //$NON-NLS-1$
    }

    public void addSubqueryHint(SubqueryHint hint) {
        if (hint.isNoUnnest()) {
            append(SPACE);
            append(BEGIN_HINT);
            append(SPACE);
            append(SubqueryHint.NOUNNEST);
            append(SPACE);
            append(END_HINT);
        } else if (hint.isDepJoin()) {
            append(SPACE);
            append(BEGIN_HINT);
            append(SPACE);
            append(SubqueryHint.DJ);
            append(SPACE);
            append(END_HINT);
        } else if (hint.isMergeJoin()) {
            append(SPACE);
            append(BEGIN_HINT);
            append(SPACE);
            append(SubqueryHint.MJ);
            append(SPACE);
            append(END_HINT);
        }
    }

    @Override
    public void visit( SubqueryCompareCriteria obj ) {
        Expression leftExpression = obj.getLeftExpression();
        visitNode(leftExpression);

        String operator = obj.getOperatorAsString();
        String quantifier = obj.getPredicateQuantifierAsString();

        // operator and beginning of list
        append(SPACE);
        append(operator);
        append(SPACE);
        append(quantifier);
        addSubqueryHint(obj.getSubqueryHint());
        append(" ("); //$NON-NLS-1$
        if (obj.getCommand() != null) {
            visitNode(obj.getCommand());
        } else {
            visitNode(obj.getArrayExpression());
        }
        append(")"); //$NON-NLS-1$
    }

    @Override
    public void visit( ScalarSubquery obj ) {
        if (obj.getSubqueryHint().isDepJoin() || obj.getSubqueryHint().isMergeJoin() || obj.getSubqueryHint().isNoUnnest()) {
            if (this.parts.length() > 0 && this.parts.charAt(this.parts.length()-1) == ' ') {
                this.parts.setLength(this.parts.length() -1);
            }
            addSubqueryHint(obj.getSubqueryHint());
            append(SPACE);
        }
        append("("); //$NON-NLS-1$
        visitNode(obj.getCommand());
        append(")"); //$NON-NLS-1$
    }

    @Override
    public void visit( XMLAttributes obj ) {
        append(XMLATTRIBUTES);
        append("("); //$NON-NLS-1$
        registerNodes(obj.getArgs(), 0);
        append(")"); //$NON-NLS-1$
    }

    @Override
    public void visit( XMLElement obj ) {
        append(XMLELEMENT);
        append("(NAME "); //$NON-NLS-1$
        outputDisplayName(obj.getName());
        if (obj.getNamespaces() != null) {
            append(", "); //$NON-NLS-1$
            visitNode(obj.getNamespaces());
        }
        if (obj.getAttributes() != null) {
            append(", "); //$NON-NLS-1$
            visitNode(obj.getAttributes());
        }
        if (!obj.getContent().isEmpty()) {
            append(", "); //$NON-NLS-1$
        }
        registerNodes(obj.getContent(), 0);
        append(")"); //$NON-NLS-1$
    }

    @Override
    public void visit( XMLForest obj ) {
        append(XMLFOREST);
        append("("); //$NON-NLS-1$
        if (obj.getNamespaces() != null) {
            visitNode(obj.getNamespaces());
            append(", "); //$NON-NLS-1$
        }
        registerNodes(obj.getArgs(), 0);
        append(")"); //$NON-NLS-1$
    }

    @Override
    public void visit( JSONObject obj ) {
        append(NonReserved.JSONOBJECT);
        append("("); //$NON-NLS-1$
        registerNodes(obj.getArgs(), 0);
        append(")"); //$NON-NLS-1$
    }

    @Override
    public void visit( TextLine obj ) {
        append(FOR);
        append(SPACE);
        registerNodes(obj.getExpressions(), 0);

        if (obj.getDelimiter() != null) {
            append(SPACE);
            append(NonReserved.DELIMITER);
            append(SPACE);
            visitNode(new Constant(obj.getDelimiter()));
        }
        if (obj.getQuote() != null) {
            append(SPACE);
            if (obj.getQuote().charValue() == TextLine.NO_QUOTE_CHAR) {
                append(NO);
                append(SPACE);
                append(NonReserved.QUOTE);
            } else {
                append(NonReserved.QUOTE);
                append(SPACE);
                visitNode(new Constant(obj.getQuote()));
            }
        }
        if (obj.isIncludeHeader()) {
            append(SPACE);
            append(NonReserved.HEADER);
        }
        if (obj.getEncoding() != null) {
            append(SPACE);
            append(NonReserved.ENCODING);
            append(SPACE);
            outputDisplayName(obj.getEncoding());
        }
    }

    @Override
    public void visit( XMLNamespaces obj ) {
        append(XMLNAMESPACES);
        append("("); //$NON-NLS-1$
        for (Iterator<NamespaceItem> items = obj.getNamespaceItems().iterator(); items.hasNext();) {
            NamespaceItem item = items.next();
            if (item.getPrefix() == null) {
                if (item.getUri() == null) {
                    append("NO DEFAULT"); //$NON-NLS-1$
                } else {
                    append("DEFAULT "); //$NON-NLS-1$
                    visitNode(new Constant(item.getUri()));
                }
            } else {
                visitNode(new Constant(item.getUri()));
                append(" AS "); //$NON-NLS-1$
                outputDisplayName(item.getPrefix());
            }
            if (items.hasNext()) {
                append(", "); //$NON-NLS-1$
            }
        }
        append(")"); //$NON-NLS-1$
    }

    @Override
    public void visit( Limit obj ) {
        if (!obj.isStrict()) {
            append(BEGIN_HINT);
            append(SPACE);
            append(Limit.NON_STRICT);
            append(SPACE);
            append(END_HINT);
            append(SPACE);
        }
        if (obj.getRowLimit() == null) {
            append(OFFSET);
            append(SPACE);
            visitNode(obj.getOffset());
            append(SPACE);
            append(ROWS);
            return;
        }
        append(LIMIT);
        if (obj.getOffset() != null) {
            append(SPACE);
            visitNode(obj.getOffset());
            append(","); //$NON-NLS-1$
        }
        append(SPACE);
        visitNode(obj.getRowLimit());
    }

    @Override
    public void visit( TextTable obj ) {
        addHintComment(obj);
        append("TEXTTABLE("); //$NON-NLS-1$
        visitNode(obj.getFile());
        if (obj.getSelector() != null) {
            append(SPACE);
            append(NonReserved.SELECTOR);
            append(SPACE);
            outputLiteral(String.class, false, obj.getSelector());
        }
        append(SPACE);
        append(NonReserved.COLUMNS);
        boolean noTrim = obj.isNoTrim();
        for (Iterator<TextColumn> cols = obj.getColumns().iterator(); cols.hasNext();) {
            TextColumn col = cols.next();
            append(SPACE);
            outputDisplayName(col.getName());
            append(SPACE);
            if (col.isOrdinal()) {
                append(FOR);
                append(SPACE);
                append(NonReserved.ORDINALITY);
            } else {
                if (col.getHeader() != null) {
                    append(NonReserved.HEADER);
                    append(SPACE);
                    outputLiteral(String.class, false, col.getHeader());
                    append(SPACE);
                }
                append(col.getType());
                if (col.getWidth() != null) {
                    append(SPACE);
                    append(NonReserved.WIDTH);
                    append(SPACE);
                    append(col.getWidth());
                }
                if (!noTrim && col.isNoTrim()) {
                    append(SPACE);
                    append(NO);
                    append(SPACE);
                    append(NonReserved.TRIM);
                }
                if (col.getSelector() != null) {
                    append(SPACE);
                    append(NonReserved.SELECTOR);
                    append(SPACE);
                    outputLiteral(String.class, false, col.getSelector());
                    append(SPACE);
                    append(col.getPosition());
                }
            }
            if (cols.hasNext()) {
                append(","); //$NON-NLS-1$
            }
        }
        if (!obj.isUsingRowDelimiter()) {
            append(SPACE);
            append(NO);
            append(SPACE);
            append(ROW);
            append(SPACE);
            append(NonReserved.DELIMITER);
        } else if (obj.getRowDelimiter() != null) {
            append(SPACE);
            append(ROW);
            append(SPACE);
            append(NonReserved.DELIMITER);
            append(SPACE);
            visitNode(new Constant(obj.getRowDelimiter()));
        }
        if (obj.getDelimiter() != null) {
            append(SPACE);
            append(NonReserved.DELIMITER);
            append(SPACE);
            visitNode(new Constant(obj.getDelimiter()));
        }
        if (obj.getQuote() != null) {
            append(SPACE);
            if (obj.isEscape()) {
                append(ESCAPE);
            } else {
                append(NonReserved.QUOTE);
            }
            append(SPACE);
            visitNode(new Constant(obj.getQuote()));
        }
        if (obj.getHeader() != null) {
            append(SPACE);
            append(NonReserved.HEADER);
            if (1 != obj.getHeader()) {
                append(SPACE);
                append(obj.getHeader());
            }
        }
        if (obj.getSkip() != null) {
            append(SPACE);
            append(NonReserved.SKIP);
            append(SPACE);
            append(obj.getSkip());
        }
        if (noTrim) {
            append(SPACE);
            append(NO);
            append(SPACE);
            append(NonReserved.TRIM);
        }
        endTableFunction(obj);
    }

    private void endTableFunction(TableFunctionReference obj) {
        append(")");//$NON-NLS-1$
        append(SPACE);
        append(AS);
        append(SPACE);
        append(escapeSinglePart(obj.getName()));
    }

    @Override
    public void visit( XMLTable obj ) {
        addHintComment(obj);
        append("XMLTABLE("); //$NON-NLS-1$
        if (obj.getNamespaces() != null) {
            visitNode(obj.getNamespaces());
            append(","); //$NON-NLS-1$
            append(SPACE);
        }
        visitNode(new Constant(obj.getXquery()));
        if (!obj.getPassing().isEmpty()) {
            append(SPACE);
            append(NonReserved.PASSING);
            append(SPACE);
            registerNodes(obj.getPassing(), 0);
        }
        if (!obj.getColumns().isEmpty() && !obj.isUsingDefaultColumn()) {
            append(SPACE);
            append(NonReserved.COLUMNS);
            for (Iterator<XMLColumn> cols = obj.getColumns().iterator(); cols.hasNext();) {
                XMLColumn col = cols.next();
                append(SPACE);
                outputDisplayName(col.getName());
                append(SPACE);
                if (col.isOrdinal()) {
                    append(FOR);
                    append(SPACE);
                    append(NonReserved.ORDINALITY);
                } else {
                    append(col.getType());
                    if (col.getDefaultExpression() != null) {
                        append(SPACE);
                        append(DEFAULT);
                        append(SPACE);
                        visitNode(col.getDefaultExpression());
                    }
                    if (col.getPath() != null) {
                        append(SPACE);
                        append(NonReserved.PATH);
                        append(SPACE);
                        visitNode(new Constant(col.getPath()));
                    }
                }
                if (cols.hasNext()) {
                    append(","); //$NON-NLS-1$
                }
            }
        }
        endTableFunction(obj);
    }

    @Override
    public void visit( ObjectTable obj ) {
        addHintComment(obj);
        append("OBJECTTABLE("); //$NON-NLS-1$
        if (obj.getScriptingLanguage() != null) {
            append(LANGUAGE);
            append(SPACE);
            visitNode(new Constant(obj.getScriptingLanguage()));
            append(SPACE);
        }
        visitNode(new Constant(obj.getRowScript()));
        if (!obj.getPassing().isEmpty()) {
            append(SPACE);
            append(NonReserved.PASSING);
            append(SPACE);
            registerNodes(obj.getPassing(), 0);
        }
        append(SPACE);
        append(NonReserved.COLUMNS);
        for (Iterator<ObjectColumn> cols = obj.getColumns().iterator(); cols.hasNext();) {
            ObjectColumn col = cols.next();
            append(SPACE);
            outputDisplayName(col.getName());
            append(SPACE);
            append(col.getType());
            append(SPACE);
            visitNode(new Constant(col.getPath()));
            if (col.getDefaultExpression() != null) {
                append(SPACE);
                append(DEFAULT);
                append(SPACE);
                visitNode(col.getDefaultExpression());
            }
            if (cols.hasNext()) {
                append(","); //$NON-NLS-1$
            }
        }
        endTableFunction(obj);
    }

    @Override
    public void visit( JsonTable obj ) {
        addHintComment(obj);
        append(NonReserved.JSONTABLE).append(Tokens.LPAREN);
        visitNode(obj.getJson());
        append(Tokens.COMMA).append(SPACE);
        visitNode(new Constant(obj.getRowPath()));
        if (obj.getNullLeaf() != null) {
            append(Tokens.COMMA).append(SPACE);
            visitNode(new Constant(obj.getNullLeaf()));
        }
        append(SPACE);
        append(NonReserved.COLUMNS);
        for (Iterator<JsonColumn> cols = obj.getColumns().iterator(); cols.hasNext();) {
            JsonColumn col = cols.next();
            append(SPACE);
            outputDisplayName(col.getName());
            append(SPACE);
            if (col.isOrdinal()) {
                append(FOR);
                append(SPACE);
                append(NonReserved.ORDINALITY);
            } else {
                append(col.getType());
                if (col.getPath() != null) {
                    append(SPACE);
                    append(NonReserved.PATH);
                    append(SPACE);
                    visitNode(new Constant(col.getPath()));
                }
            }
            if (cols.hasNext()) {
                append(","); //$NON-NLS-1$
            }
        }
        endTableFunction(obj);
    }

    @Override
    public void visit( XMLQuery obj ) {
        append("XMLQUERY("); //$NON-NLS-1$
        if (obj.getNamespaces() != null) {
            visitNode(obj.getNamespaces());
            append(","); //$NON-NLS-1$
            append(SPACE);
        }
        visitNode(new Constant(obj.getXquery()));
        if (!obj.getPassing().isEmpty()) {
            append(SPACE);
            append(NonReserved.PASSING);
            append(SPACE);
            registerNodes(obj.getPassing(), 0);
        }
        if (obj.getEmptyOnEmpty() != null) {
            append(SPACE);
            if (obj.getEmptyOnEmpty()) {
                append(NonReserved.EMPTY);
            } else {
                append(NULL);
            }
            append(SPACE);
            append(ON);
            append(SPACE);
            append(NonReserved.EMPTY);
        }
        append(")");//$NON-NLS-1$
    }

    @Override
    public void visit(XMLExists exists) {
        append("XMLEXISTS("); //$NON-NLS-1$
        XMLQuery obj = exists.getXmlQuery();
        if (obj.getNamespaces() != null) {
            visitNode(obj.getNamespaces());
            append(","); //$NON-NLS-1$
            append(SPACE);
        }
        visitNode(new Constant(obj.getXquery()));
        if (!obj.getPassing().isEmpty()) {
            append(SPACE);
            append(NonReserved.PASSING);
            append(SPACE);
            registerNodes(obj.getPassing(), 0);
        }
        append(")");//$NON-NLS-1$
    }

    @Override
    public void visit(XMLCast xmlcast) {
        append("XMLCAST("); //$NON-NLS-1$
        append(xmlcast.getExpression());
        append(Tokens.SPACE);
        append(AS);
        append(Tokens.SPACE);
        append(xmlcast.getTypeName());
        append(")");//$NON-NLS-1$
    }

    @Override
    public void visit( DerivedColumn obj ) {
        visitNode(obj.getExpression());
        if (obj.getAlias() != null) {
            append(SPACE);
            append(AS);
            append(SPACE);
            outputDisplayName(obj.getAlias());
        }
    }

    @Override
    public void visit( XMLSerialize obj ) {
        append(XMLSERIALIZE);
        append(Tokens.LPAREN);
        if (obj.getDocument() != null) {
            if (obj.getDocument()) {
                append(NonReserved.DOCUMENT);
            } else {
                append(NonReserved.CONTENT);
            }
            append(SPACE);
        }
        visitNode(obj.getExpression());
        if (obj.getTypeString() != null) {
            append(SPACE);
            append(AS);
            append(SPACE);
            append(obj.getTypeString());
        }
        if (obj.getEncoding() != null) {
            append(SPACE);
            append(NonReserved.ENCODING);
            append(SPACE);
            append(escapeSinglePart(obj.getEncoding()));
        }
        if (obj.getVersion() != null) {
            append(SPACE);
            append(NonReserved.VERSION);
            append(SPACE);
            append(new Constant(obj.getVersion()));
        }
        if (obj.getDeclaration() != null) {
            append(SPACE);
            if (obj.getDeclaration()) {
                append(NonReserved.INCLUDING);
            } else {
                append(NonReserved.EXCLUDING);
            }
            append(SPACE);
            append(NonReserved.XMLDECLARATION);
        }
        append(Tokens.RPAREN);
    }

    @Override
    public void visit( QueryString obj ) {
        append(NonReserved.QUERYSTRING);
        append("("); //$NON-NLS-1$
        visitNode(obj.getPath());
        if (!obj.getArgs().isEmpty()) {
            append(","); //$NON-NLS-1$
            append(SPACE);
            registerNodes(obj.getArgs(), 0);
        }
        append(")"); //$NON-NLS-1$
    }

    @Override
    public void visit( XMLParse obj ) {
        append(XMLPARSE);
        append(Tokens.LPAREN);
        if (obj.isDocument()) {
            append(NonReserved.DOCUMENT);
        } else {
            append(NonReserved.CONTENT);
        }
        append(SPACE);
        visitNode(obj.getExpression());
        if (obj.isWellFormed()) {
            append(SPACE);
            append(NonReserved.WELLFORMED);
        }
        append(Tokens.RPAREN);
    }

    @Override
    public void visit( ExpressionCriteria obj ) {
        visitNode(obj.getExpression());
    }

    @Override
    public void visit(TriggerAction obj) {
        append(FOR);
        append(SPACE);
        append(EACH);
        append(SPACE);
        append(ROW);
        append("\n"); //$NON-NLS-1$
        addTabs(0);
        visitNode(obj.getBlock());
    }

    @Override
    public void visit(ArrayTable obj) {
        addHintComment(obj);
        append("ARRAYTABLE("); //$NON-NLS-1$
        if (obj.getSingleRow() != null) {
            append(obj.getSingleRow()?ROW:ROWS);
            append(SPACE);
        }
        visitNode(obj.getArrayValue());
        append(SPACE);
        append(NonReserved.COLUMNS);

        for (Iterator<ProjectedColumn> cols = obj.getColumns().iterator(); cols.hasNext();) {
            ProjectedColumn col = cols.next();
            append(SPACE);
            outputDisplayName(col.getName());
            append(SPACE);
            append(col.getType());
            if (cols.hasNext()) {
                append(","); //$NON-NLS-1$
            }
        }
        endTableFunction(obj);
    }

    @Override
    public void visit(AlterProcedure alterProcedure) {
        append(ALTER);
        append(SPACE);
        append(PROCEDURE);
        append(SPACE);
        append(alterProcedure.getTarget());
        beginClause(1);
        append(AS);
        addCacheHint(alterProcedure.getCacheHint());
        append(alterProcedure.getDefinition().getBlock());
    }

    @Override
    public void visit(AlterTrigger alterTrigger) {
        if (alterTrigger.isCreate()) {
            append(CREATE);
        } else {
            append(ALTER);
        }
        append(SPACE);
        append(TRIGGER);
        append(SPACE);
        if (alterTrigger.getName() != null) {
            append(escapeSinglePart(alterTrigger.getName()));
            append(SPACE);
        }
        append(ON);
        append(SPACE);
        append(alterTrigger.getTarget());
        beginClause(0);
        if (alterTrigger.isAfter()) {
            append(NonReserved.AFTER);
            append(SPACE);
        } else {
            append(NonReserved.INSTEAD);
            append(SPACE);
            append(OF);
            append(SPACE);
        }
        append(alterTrigger.getEvent());
        if (alterTrigger.getDefinition() != null) {
            beginClause(0);
            append(AS);
            append("\n"); //$NON-NLS-1$
            addTabs(0);
            append(alterTrigger.getDefinition());
        } else {
            append(SPACE);
            append(alterTrigger.getEnabled()?NonReserved.ENABLED:NonReserved.DISABLED);
        }
    }

    @Override
    public void visit(AlterView alterView) {
        append(ALTER);
        append(SPACE);
        append(NonReserved.VIEW);
        append(SPACE);
        append(alterView.getTarget());
        beginClause(0);
        append(AS);
        append("\n"); //$NON-NLS-1$
        addTabs(0);
        append(alterView.getDefinition());
    }

    @Override
    public void visit(WindowFunction windowFunction) {
        append(windowFunction.getFunction());
        append(SPACE);
        append(OVER);
        append(SPACE);
        append(windowFunction.getWindowSpecification());
    }

    @Override
    public void visit(WindowSpecification windowSpecification) {
        append(Tokens.LPAREN);
        boolean needsSpace = false;
        if (windowSpecification.getPartition() != null) {
            append(PARTITION);
            append(SPACE);
            append(BY);
            append(SPACE);
            registerNodes(windowSpecification.getPartition(), 0);
            needsSpace = true;
        }
        if (windowSpecification.getOrderBy() != null) {
            if (needsSpace) {
                append(SPACE);
            }
            append(windowSpecification.getOrderBy());
            needsSpace = true;
        }
        if (windowSpecification.getWindowFrame() != null) {
            if (needsSpace) {
                append(SPACE);
            }
            append(windowSpecification.getWindowFrame());
        }
        append(Tokens.RPAREN);
    }

    @Override
    public void visit(WindowFrame windowFrame) {
        append(windowFrame.getMode().name());
        append(SPACE);
        if (windowFrame.getEnd() != null) {
            append(Reserved.BETWEEN);
            append(SPACE);
        }
        appendFrameBound(windowFrame.getStart());
        if (windowFrame.getEnd() != null) {
            append(SPACE);
            append(Reserved.AND);
            append(SPACE);
            appendFrameBound(windowFrame.getEnd());
        }
    }

    private void appendFrameBound(FrameBound bound) {
        if (bound.getBoundMode() == org.teiid.language.WindowFrame.BoundMode.CURRENT_ROW) {
            append(NonReserved.CURRENT);
            append(SPACE);
            append(ROW);
        } else {
            if (bound.getBound() != null) {
                append(bound.getBound());
            } else {
                append(NonReserved.UNBOUNDED);
            }
            append(SPACE);
            append(bound.getBoundMode().name());
        }
    }

    @Override
    public void visit(Array array) {
        if (!array.isImplicit()) {
            append(Tokens.LPAREN);
        }
        registerNodes(array.getExpressions(), 0);
        if (!array.isImplicit()) {
            if (array.getExpressions().size() == 1) {
                append(Tokens.COMMA);
            }
            append(Tokens.RPAREN);
        }
    }

    @Override
    public void visit(IsDistinctCriteria isDistinctCriteria) {
        append(isDistinctCriteria.getLeftRowValue());
        append(SPACE);
        append(IS);
        append(SPACE);
        if (isDistinctCriteria.isNegated()) {
            append(NOT);
            append(SPACE);
        }
        append(DISTINCT);
        append(SPACE);
        append(FROM);
        append(SPACE);
        append(isDistinctCriteria.getRightRowValue());
    }

    public static String escapeSinglePart( String part ) {
        if (isReservedWord(part)) {
            return ID_ESCAPE_CHAR + part + ID_ESCAPE_CHAR;
        }
        boolean escape = true;
        char start = part.charAt(0);
        if (start == '#' || start == '@' || StringUtil.isLetter(start)) {
            escape = false;
            for (int i = 1; !escape && i < part.length(); i++) {
                char c = part.charAt(i);
                escape = !StringUtil.isLetterOrDigit(c) && c != '_';
            }
        }
        if (escape) {
            return ID_ESCAPE_CHAR + escapeStringValue(part, "\"") + ID_ESCAPE_CHAR; //$NON-NLS-1$
        }
        return part;
    }

    /**
     * Check whether a string is considered a reserved word or not. Subclasses may override to change definition of reserved word.
     *
     * @param string String to check
     * @return True if reserved word
     */
    static boolean isReservedWord( String string ) {
        if (string == null) {
            return false;
        }
        return SQLConstants.isReservedWord(string);
    }

    @Override
    public void visit(ExplainCommand explainCommand) {
       parts.append(NonReserved.EXPLAIN).append(SPACE);
       if (explainCommand.getAnalyze() != null || explainCommand.getFormat() != null) {
           parts.append(Tokens.LPAREN);
           boolean needsComma = false;
           if (explainCommand.getAnalyze() != null) {
               parts.append(NonReserved.ANALYZE).append(SPACE).append(explainCommand.getAnalyze().toString());
               needsComma = true;
           }
           if (explainCommand.getFormat() != null) {
               if (needsComma) {
                   parts.append(Tokens.COMMA).append(SPACE);
               }
               parts.append(NonReserved.FORMAT).append(SPACE).append(explainCommand.getFormat().name());
               needsComma = true;
           }
           parts.append(Tokens.RPAREN).append(SPACE);
       }
       visitNode(explainCommand.getCommand());
    }

}
