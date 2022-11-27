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

package org.teiid.language.visitor;

import static org.teiid.language.SQLConstants.Reserved.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.connector.DataPlugin;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.StringUtil;
import org.teiid.language.*;
import org.teiid.language.Argument.Direction;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.SetQuery.Operation;
import org.teiid.language.SortSpecification.Ordering;
import org.teiid.language.WindowFrame.BoundMode;
import org.teiid.language.WindowFrame.FrameBound;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Table;


/**
 * Creates a SQL string for a LanguageObject subtree. Instances of this class
 * are not reusable, and are not thread-safe.
 */
public class SQLStringVisitor extends AbstractLanguageVisitor {
    public static final String TEIID_NATIVE_QUERY = AbstractMetadataRecord.RELATIONAL_PREFIX + "native-query"; //$NON-NLS-1$

    private static final Set<String> infixFunctions = new HashSet<String>(Arrays.asList("%", "+", "-", "*", "+", "/", "||", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
            "&", "|", "^", "#", "&&"));   //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$

    private static Pattern pattern = Pattern.compile("\\$+\\d+"); //$NON-NLS-1$

    protected static final String UNDEFINED = "<undefined>"; //$NON-NLS-1$
    protected static final String UNDEFINED_PARAM = "?"; //$NON-NLS-1$

    protected StringBuilder buffer = new StringBuilder();
    private boolean appendedSourceComment;
    protected boolean shortNameOnly = false;

    /**
     * Gets the name of a group or element from the RuntimeMetadata
     * @return the name of that element or group as defined in the source
     */
    protected String getName(AbstractMetadataRecord object) {
        return getRecordName(object);
    }

    /**
     * Get the name in source or the name if
     * the name in source is not set.
     * @return
     */
    public static String getRecordName(AbstractMetadataRecord object) {
        return object.getSourceName();
    }

    /**
     * Appends the string form of the LanguageObject to the current buffer.
     * @param obj the language object instance
     */
    public void append(LanguageObject obj) {
        if (obj == null) {
            buffer.append(UNDEFINED);
        } else {
            visitNode(obj);
        }
    }

    /**
     * Simple utility to append a list of language objects to the current buffer
     * by creating a comma-separated list.
     * @param items a list of LanguageObjects
     */
    protected void append(List<? extends LanguageObject> items) {
        if (items != null && items.size() != 0) {
            append(items.get(0));
            for (int i = 1; i < items.size(); i++) {
                buffer.append(Tokens.COMMA)
                      .append(Tokens.SPACE);
                append(items.get(i));
            }
        }
    }

    /**
     * Simple utility to append an array of language objects to the current buffer
     * by creating a comma-separated list.
     * @param items an array of LanguageObjects
     */
    protected void append(LanguageObject[] items) {
        if (items != null && items.length != 0) {
            append(items[0]);
            for (int i = 1; i < items.length; i++) {
                buffer.append(Tokens.COMMA)
                      .append(Tokens.SPACE);
                append(items[i]);
            }
        }
    }

    /**
     * Creates a SQL-safe string. Simply replaces all occurrences of ' with ''
     * @param str the input string
     * @return a SQL-safe string
     */
    protected String escapeString(String str, String quote) {
        return StringUtil.replaceAll(str, quote, quote + quote);
    }

    public String toString() {
        return buffer.toString();
    }

    public void visit(AggregateFunction obj) {
        buffer.append(obj.getName())
              .append(Tokens.LPAREN);

        if ( obj.isDistinct()) {
            buffer.append(DISTINCT)
                  .append(Tokens.SPACE);
        }

        if (obj.getParameters().isEmpty()
                && (SQLConstants.NonReserved.COUNT.equalsIgnoreCase(obj.getName())
                || SQLConstants.NonReserved.COUNT_BIG.equalsIgnoreCase(obj.getName()))) {
            buffer.append(Tokens.ALL_COLS);
        } else {
            append(obj.getParameters());
        }
        if (obj.getOrderBy() != null) {
            buffer.append(Tokens.SPACE);
            append(obj.getOrderBy());
        }
        buffer.append(Tokens.RPAREN);
        if (obj.getCondition() != null) {
            buffer.append(Tokens.SPACE);
            buffer.append(FILTER);
            buffer.append(Tokens.LPAREN);
            buffer.append(WHERE);
            buffer.append(Tokens.SPACE);
            append(obj.getCondition());
            buffer.append(Tokens.RPAREN);
        }
    }

    public void visit(Comparison obj) {
        if (obj.getLeftExpression() instanceof Condition) {
            buffer.append(Tokens.LPAREN);
            append(obj.getLeftExpression());
            buffer.append(Tokens.RPAREN);
        } else {
            append(obj.getLeftExpression());
        }
        buffer.append(Tokens.SPACE);
        buffer.append(obj.getOperator());
        buffer.append(Tokens.SPACE);
        if (obj.getRightExpression() instanceof Condition) {
            buffer.append(Tokens.LPAREN);
            appendRightComparison(obj);
            buffer.append(Tokens.RPAREN);
        } else {
            appendRightComparison(obj);
        }
    }

    protected void appendRightComparison(Comparison obj) {
        append(obj.getRightExpression());
    }

    public void visit(AndOr obj) {
        String opString = obj.getOperator().toString();

        appendNestedCondition(obj, obj.getLeftCondition());
        buffer.append(Tokens.SPACE)
              .append(opString)
              .append(Tokens.SPACE);
        appendNestedCondition(obj, obj.getRightCondition());
    }

    protected void appendNestedCondition(AndOr parent, Condition condition) {
        if (condition instanceof AndOr) {
            AndOr nested = (AndOr)condition;
            if (nested.getOperator() != parent.getOperator()) {
                buffer.append(Tokens.LPAREN);
                append(condition);
                buffer.append(Tokens.RPAREN);
                return;
            }
        }
        append(condition);
    }

    public void visit(Delete obj) {
        buffer.append(DELETE)
              .append(Tokens.SPACE);
        appendSourceComment(obj);
        buffer.append(FROM)
              .append(Tokens.SPACE);
        append(obj.getTable());
        if (obj.getWhere() != null) {
            buffer.append(Tokens.SPACE)
                  .append(WHERE)
                  .append(Tokens.SPACE);
            append(obj.getWhere());
        }
    }

    private void appendSourceComment(Command obj) {
        if (appendedSourceComment) {
            return;
        }
        appendedSourceComment = true;
        buffer.append(getSourceComment(obj));
    }

    /**
     * Take the specified derived group and element short names and determine a
     * replacement element name to use instead.  Most commonly, this is used to strip
     * the group name if the group is a pseudo-group (DUAL) or the element is a pseudo-group
     * (ROWNUM).  It may also be used to strip special information out of the name in source
     * value in some specialized cases.
     *
     * By default, this method returns null, indicating that the normal group and element
     * name logic should be used (group + "." + element).  Subclasses should override and
     * implement this method if desired.
     *
     * @param group Group name, may be null
     * @param element Element name, never null
     * @return Replacement element name to be used as is (no modification will occur)
     * @since 5.0
     */
    protected String replaceElementName(String group, String element) {
        return null;
    }

    public void visit(ColumnReference obj) {
        buffer.append(getElementName(obj, !shortNameOnly));
    }

    private String getElementName(ColumnReference obj, boolean qualify) {
        String groupName = null;
        NamedTable group = obj.getTable();
        if (group != null && qualify) {
            if(group.getCorrelationName() != null) {
                groupName = group.getCorrelationName();
            } else {
                AbstractMetadataRecord groupID = group.getMetadataObject();
                if(groupID != null) {
                    groupName = getName(groupID);
                } else {
                    groupName = group.getName();
                }
            }
        }

        String elemShortName = null;
        AbstractMetadataRecord elementID = obj.getMetadataObject();
        if(elementID != null) {
            elemShortName = getName(elementID);
        } else {
            elemShortName = obj.getName();
        }

        // Check whether a subclass wants to replace the element name to use in special circumstances
        String replacementElement = replaceElementName(groupName, elemShortName);
        if(replacementElement != null) {
            // If so, use it as is
            return replacementElement;
        }
        StringBuffer elementName = new StringBuffer(elemShortName.length());
        // If not, do normal logic:  [group + "."] + element
        if(groupName != null) {
            elementName.append(groupName);
            elementName.append(Tokens.DOT);
        }
        elementName.append(elemShortName);
        return elementName.toString();
    }

    /**
     * @param elementName
     * @return
     * @since 4.3
     */
    public static String getShortName(String elementName) {
        int lastDot = elementName.lastIndexOf("."); //$NON-NLS-1$
        if(lastDot >= 0) {
            elementName = elementName.substring(lastDot+1);
        }
        return elementName;
    }

    public void visit(Call obj) {
        appendCallStart(obj);

        if(obj.getMetadataObject() != null) {
            buffer.append(getName(obj.getMetadataObject()));
        } else {
            buffer.append(obj.getProcedureName());
        }

        buffer.append(Tokens.LPAREN);
        final List<Argument> params = obj.getArguments();
        if (params != null && params.size() != 0) {
            Argument param = null;
            for (int i = 0; i < params.size(); i++) {
                param = params.get(i);
                if (param.getDirection() == Direction.IN || param.getDirection() == Direction.INOUT) {
                    if (i != 0) {
                        buffer.append(Tokens.COMMA)
                              .append(Tokens.SPACE);
                    }
                    append(param);
                }
            }
        }
        buffer.append(Tokens.RPAREN);
    }

    protected void appendCallStart(Call call) {
        buffer.append(EXEC)
              .append(Tokens.SPACE);
    }

    public void visit(Exists obj) {
        buffer.append(EXISTS)
              .append(Tokens.SPACE)
              .append(Tokens.LPAREN);
        append(obj.getSubquery());
        buffer.append(Tokens.RPAREN);
    }

    protected boolean isInfixFunction(String function) {
        return infixFunctions.contains(function);
    }

    public void visit(Function obj) {

        String name = obj.getName();
        List<Expression> args = obj.getParameters();
        if(name.equalsIgnoreCase(CONVERT) || name.equalsIgnoreCase(CAST)) {

            Object typeValue = ((Literal)args.get(1)).getValue();

            buffer.append(name);
            buffer.append(Tokens.LPAREN);

            append(args.get(0));

            if(name.equalsIgnoreCase(CONVERT)) {
                buffer.append(Tokens.COMMA);
                buffer.append(Tokens.SPACE);
            } else {
                buffer.append(Tokens.SPACE);
                buffer.append(AS);
                buffer.append(Tokens.SPACE);
            }
            buffer.append(typeValue);
            buffer.append(Tokens.RPAREN);
        } else if(isInfixFunction(name)) {
            buffer.append(Tokens.LPAREN);

            if(args != null) {
                for(int i=0; i<args.size(); i++) {
                    append(args.get(i));
                    if(i < (args.size()-1)) {
                        buffer.append(Tokens.SPACE);
                        buffer.append(name);
                        buffer.append(Tokens.SPACE);
                    }
                }
            }
            buffer.append(Tokens.RPAREN);

        } else if(name.equalsIgnoreCase(NonReserved.TIMESTAMPADD) || name.equalsIgnoreCase(NonReserved.TIMESTAMPDIFF)) {
            buffer.append(name);
            buffer.append(Tokens.LPAREN);

            if(args != null && args.size() > 0) {
                buffer.append(((Literal)args.get(0)).getValue());

                for(int i=1; i<args.size(); i++) {
                    buffer.append(Tokens.COMMA);
                    buffer.append(Tokens.SPACE);
                    append(args.get(i));
                }
            }
            buffer.append(Tokens.RPAREN);
        } else if (name.equalsIgnoreCase(NonReserved.TRIM)) {
            buffer.append(name);
            buffer.append(Tokens.LPAREN);
            String value = (String)((Literal)args.get(0)).getValue();
            if (!value.equalsIgnoreCase(BOTH)) {
                buffer.append(value);
                buffer.append(Tokens.SPACE);
            }
            append(args.get(1));
            buffer.append(" "); //$NON-NLS-1$
            buffer.append(FROM);
            buffer.append(" "); //$NON-NLS-1$
            buffer.append(args.get(2));
            buffer.append(")"); //$NON-NLS-1$
        } else {

            buffer.append(obj.getName())
                  .append(Tokens.LPAREN);
            append(obj.getParameters());
            buffer.append(Tokens.RPAREN);
        }
    }

    public void visit(NamedTable obj) {
        appendBaseName(obj);
        if (obj.getCorrelationName() != null) {
            buffer.append(Tokens.SPACE);
            if (useAsInGroupAlias()){
                buffer.append(AS)
                      .append(Tokens.SPACE);
            }
            buffer.append(obj.getCorrelationName());
        }
    }

    protected void appendBaseName(NamedTable obj) {
        Table groupID = obj.getMetadataObject();
        if(groupID != null) {
            buffer.append(getName(groupID));
        } else {
            buffer.append(obj.getName());
        }
    }

    /**
     * Indicates whether group alias should be of the form
     * "...FROM groupA AS X" or "...FROM groupA X".  Certain
     * data sources (such as Oracle) may not support the first
     * form.
     * @return boolean
     */
    protected boolean useAsInGroupAlias(){
        return true;
    }

    public void visit(GroupBy obj) {
        buffer.append(GROUP)
              .append(Tokens.SPACE)
              .append(BY)
              .append(Tokens.SPACE);
        if (obj.isRollup()) {
            buffer.append(ROLLUP);
            buffer.append(Tokens.LPAREN);
        }
        append(obj.getElements());
        if (obj.isRollup()) {
            buffer.append(Tokens.RPAREN);
        }
    }

    public void visit(In obj) {
        appendNested(obj.getLeftExpression());
        if (obj.isNegated()) {
            buffer.append(Tokens.SPACE)
                  .append(NOT);
        }
        buffer.append(Tokens.SPACE)
              .append(IN)
              .append(Tokens.SPACE)
              .append(Tokens.LPAREN);
        append(obj.getRightExpressions());
        buffer.append(Tokens.RPAREN);
    }

    public void visit(DerivedTable obj) {
        if (obj.isLateral()) {
            appendLateralKeyword();
            buffer.append(Tokens.SPACE);
        }
        buffer.append(Tokens.LPAREN);
        append(obj.getQuery());
        buffer.append(Tokens.RPAREN);
        buffer.append(Tokens.SPACE);
        if(useAsInGroupAlias()) {
            buffer.append(AS);
            buffer.append(Tokens.SPACE);
        }
        buffer.append(obj.getCorrelationName());
    }

    protected void appendLateralKeyword() {
        buffer.append(LATERAL);
    }

    public void visit(NamedProcedureCall obj) {
        if (obj.isLateral()) {
            appendLateralKeyword();
            buffer.append(Tokens.SPACE);
        }
        buffer.append(Tokens.LPAREN);
        append(obj.getCall());
        buffer.append(Tokens.RPAREN);
        buffer.append(Tokens.SPACE);
        if(useAsInGroupAlias()) {
            buffer.append(AS);
            buffer.append(Tokens.SPACE);
        }
        buffer.append(obj.getCorrelationName());
    }

    public void visit(Insert obj) {
        if (obj.isUpsert()) {
            buffer.append(getUpsertKeyword()).append(Tokens.SPACE);
        } else {
            buffer.append(getInsertKeyword()).append(Tokens.SPACE);
        }
        appendSourceComment(obj);
        buffer.append(INTO).append(Tokens.SPACE);
        append(obj.getTable());
        buffer.append(Tokens.SPACE).append(Tokens.LPAREN);

        this.shortNameOnly = true;
        append(obj.getColumns());
        this.shortNameOnly = false;

        buffer.append(Tokens.RPAREN);
        buffer.append(Tokens.SPACE);
        append(obj.getValueSource());
    }

    protected String getInsertKeyword() {
        return INSERT;
    }

    protected String getUpsertKeyword() {
        return NonReserved.UPSERT;
    }

    @Override
    public void visit(ExpressionValueSource obj) {
        buffer.append(VALUES).append(Tokens.SPACE).append(Tokens.LPAREN);
        append(obj.getValues());
        buffer.append(Tokens.RPAREN);
    }

    @Override
    public void visit(Parameter obj) {
        buffer.append('?');
    }

    public void visit(IsNull obj) {
        appendNested(obj.getExpression());
        buffer.append(Tokens.SPACE)
              .append(IS)
              .append(Tokens.SPACE);
        if (obj.isNegated()) {
            buffer.append(NOT)
                  .append(Tokens.SPACE);
        }
        buffer.append(NULL);
    }

    /**
     * Condition operators have lower precedence than LIKE/SIMILAR/IS
     * @param ex
     */
    private void appendNested(Expression ex) {
        boolean useParens = ex instanceof Condition;
        if (useParens) {
            buffer.append(Tokens.LPAREN);
        }
        append(ex);
        if (useParens) {
            buffer.append(Tokens.RPAREN);
        }
    }

    public void visit(Join obj) {
        TableReference leftItem = obj.getLeftItem();
        if(useParensForLHSJoins() && leftItem instanceof Join) {
            buffer.append(Tokens.LPAREN);
            append(leftItem);
            buffer.append(Tokens.RPAREN);
        } else {
            append(leftItem);
        }
        buffer.append(Tokens.SPACE);

        switch(obj.getJoinType()) {
            case CROSS_JOIN:
                buffer.append(CROSS);
                break;
            case FULL_OUTER_JOIN:
                buffer.append(FULL)
                      .append(Tokens.SPACE)
                      .append(OUTER);
                break;
            case INNER_JOIN:
                buffer.append(INNER);
                break;
            case LEFT_OUTER_JOIN:
                buffer.append(LEFT)
                      .append(Tokens.SPACE)
                      .append(OUTER);
                break;
            case RIGHT_OUTER_JOIN:
                buffer.append(RIGHT)
                      .append(Tokens.SPACE)
                      .append(OUTER);
                break;
            default: buffer.append(UNDEFINED);
        }
        buffer.append(Tokens.SPACE)
              .append(JOIN)
              .append(Tokens.SPACE);

        TableReference rightItem = obj.getRightItem();
        if(rightItem instanceof Join && (useParensForJoins() || obj.getJoinType() == Join.JoinType.CROSS_JOIN)) {
            buffer.append(Tokens.LPAREN);
            append(rightItem);
            buffer.append(Tokens.RPAREN);
        } else {
            append(rightItem);
        }

        final Condition condition = obj.getCondition();
        if (condition != null) {
            buffer.append(Tokens.SPACE)
                  .append(ON)
                  .append(Tokens.SPACE);
            append(condition);
        }
    }

    /**
     * If a nested left hand join should have parens
     * @return
     */
    protected boolean useParensForLHSJoins() {
        return useParensForJoins();
    }

    public void visit(Like obj) {
        append(obj.getLeftExpression());
        if (obj.isNegated()) {
            buffer.append(Tokens.SPACE)
                  .append(NOT);
        }
        buffer.append(Tokens.SPACE);
        switch (obj.getMode()) {
        case LIKE:
            buffer.append(LIKE);
            break;
        case SIMILAR:
            buffer.append(SIMILAR)
                  .append(Tokens.SPACE)
                  .append(TO);
        case REGEX:
            buffer.append(getLikeRegexString());
        }
        buffer.append(Tokens.SPACE);
        append(obj.getRightExpression());
        if (obj.getEscapeCharacter() != null) {
            buffer.append(Tokens.SPACE)
                  .append(ESCAPE)
                  .append(Tokens.SPACE)
                  .append(Tokens.QUOTE)
                  .append(escapeString(String.valueOf(obj.getEscapeCharacter()), Tokens.QUOTE))
                  .append(Tokens.QUOTE);
        }
    }

    protected String getLikeRegexString() {
        return LIKE_REGEX;
    }

    public void visit(Limit obj) {
        buffer.append(LIMIT)
              .append(Tokens.SPACE);
        if (obj.getRowOffset() > 0) {
            buffer.append(obj.getRowOffset())
                  .append(Tokens.COMMA)
                  .append(Tokens.SPACE);
        }
        buffer.append(obj.getRowLimit());
    }

    public void visit(Literal obj) {
        if (obj.getValue() == null) {
            buffer.append(NULL);
        } else {
            Class<?> type = obj.getType();
            appendLiteral(obj, type);
        }
    }

    protected void appendLiteral(Literal obj, Class<?> type) {
        String val = obj.getValue().toString();
        if(Number.class.isAssignableFrom(type)) {
            buffer.append(val);
        } else if(type.equals(DataTypeManager.DefaultDataClasses.BOOLEAN)) {
            buffer.append(obj.getValue().equals(Boolean.TRUE) ? TRUE : FALSE);
        } else if(type.equals(DataTypeManager.DefaultDataClasses.TIMESTAMP)) {
            buffer.append("{ts '") //$NON-NLS-1$
                  .append(val)
                  .append("'}"); //$NON-NLS-1$
        } else if(type.equals(DataTypeManager.DefaultDataClasses.TIME)) {
            buffer.append("{t '") //$NON-NLS-1$
                  .append(val)
                  .append("'}"); //$NON-NLS-1$
        } else if(type.equals(DataTypeManager.DefaultDataClasses.DATE)) {
            buffer.append("{d '") //$NON-NLS-1$
                  .append(val)
                  .append("'}"); //$NON-NLS-1$
        } else if (type.equals(DataTypeManager.DefaultDataClasses.VARBINARY)) {
            buffer.append("X'") //$NON-NLS-1$
                  .append(val)
                  .append("'"); //$NON-NLS-1$
        } else {
            buffer.append(Tokens.QUOTE)
                  .append(escapeString(val, Tokens.QUOTE))
                  .append(Tokens.QUOTE);
        }
    }

    public void visit(Not obj) {
        buffer.append(NOT)
              .append(Tokens.SPACE)
              .append(Tokens.LPAREN);
        append(obj.getCriteria());
        buffer.append(Tokens.RPAREN);
    }

    public void visit(OrderBy obj) {
        buffer.append(ORDER)
              .append(Tokens.SPACE)
              .append(BY)
              .append(Tokens.SPACE);
        append(obj.getSortSpecifications());
    }

    public void visit(SortSpecification obj) {
        append(obj.getExpression());
        if (obj.getOrdering() == Ordering.DESC) {
            buffer.append(Tokens.SPACE)
                  .append(DESC);
        } // Don't print default "ASC"
        if (obj.getNullOrdering() != null) {
            buffer.append(Tokens.SPACE)
                .append(NonReserved.NULLS)
                .append(Tokens.SPACE)
                .append(obj.getNullOrdering().name());
        }
    }

    public void visit(Argument obj) {
        visitNode(obj.getExpression());
    }

    public void visit(Select obj) {
        if (obj.getWith() != null) {
            append(obj.getWith());
        }
        buffer.append(SELECT).append(Tokens.SPACE);
        appendSourceComment(obj);
        if (obj.isDistinct()) {
            buffer.append(DISTINCT).append(Tokens.SPACE);
        }
        if (useSelectLimit() && obj.getLimit() != null) {
            append(obj.getLimit());
            buffer.append(Tokens.SPACE);
        }
        append(obj.getDerivedColumns());
        if (obj.getFrom() != null && !obj.getFrom().isEmpty()) {
            buffer.append(Tokens.SPACE).append(FROM).append(Tokens.SPACE);
            append(obj.getFrom());
        }
        if (obj.getWhere() != null) {
            buffer.append(Tokens.SPACE)
                  .append(WHERE)
                  .append(Tokens.SPACE);
            append(obj.getWhere());
        }
        if (obj.getGroupBy() != null) {
            buffer.append(Tokens.SPACE);
            append(obj.getGroupBy());
        }
        if (obj.getHaving() != null) {
            buffer.append(Tokens.SPACE)
                  .append(HAVING)
                  .append(Tokens.SPACE);
            append(obj.getHaving());
        }
        if (obj.getOrderBy() != null) {
            buffer.append(Tokens.SPACE);
            append(obj.getOrderBy());
        }
        if (!useSelectLimit() && obj.getLimit() != null) {
            buffer.append(Tokens.SPACE);
            append(obj.getLimit());
        }
    }

    public void visit(SearchedCase obj) {
        buffer.append(CASE);
        for (SearchedWhenClause swc : obj.getCases()) {
            append(swc);
        }
        if (obj.getElseExpression() != null) {
            buffer.append(Tokens.SPACE)
                  .append(ELSE)
                  .append(Tokens.SPACE);
            append(obj.getElseExpression());
        }
        buffer.append(Tokens.SPACE)
              .append(END);
    }

    @Override
    public void visit(SearchedWhenClause obj) {
        buffer.append(Tokens.SPACE).append(WHEN)
                .append(Tokens.SPACE);
        append(obj.getCondition());
        buffer.append(Tokens.SPACE).append(THEN)
                .append(Tokens.SPACE);
        append(obj.getResult());
    }

    protected String getSourceComment(Command command) {
        return ""; //$NON-NLS-1$
    }

    public void visit(ScalarSubquery obj) {
        buffer.append(Tokens.LPAREN);
        append(obj.getSubquery());
        buffer.append(Tokens.RPAREN);
    }

    public void visit(DerivedColumn obj) {
        append(obj.getExpression());
        if (obj.getAlias() != null) {
            buffer.append(Tokens.SPACE)
                  .append(AS)
                  .append(Tokens.SPACE)
                  .append(obj.getAlias());
        }
    }

    public void visit(SubqueryComparison obj) {
        append(obj.getLeftExpression());
        buffer.append(Tokens.SPACE);

        switch(obj.getOperator()) {
            case EQ: buffer.append(Tokens.EQ); break;
            case GE: buffer.append(Tokens.GE); break;
            case GT: buffer.append(Tokens.GT); break;
            case LE: buffer.append(Tokens.LE); break;
            case LT: buffer.append(Tokens.LT); break;
            case NE: buffer.append(Tokens.NE); break;
            default: buffer.append(UNDEFINED);
        }
        buffer.append(Tokens.SPACE);
        appendQuantifier(obj);
        buffer.append(Tokens.SPACE);
        buffer.append(Tokens.LPAREN);
        append(obj.getSubquery());
        buffer.append(Tokens.RPAREN);
    }

    protected void appendQuantifier(SubqueryComparison obj) {
        switch(obj.getQuantifier()) {
            case ALL: buffer.append(ALL); break;
            case SOME: buffer.append(SOME); break;
            default: buffer.append(UNDEFINED);
        }
    }

    public void visit(SubqueryIn obj) {
        append(obj.getLeftExpression());
        if (obj.isNegated()) {
            buffer.append(Tokens.SPACE)
                  .append(NOT);
        }
        buffer.append(Tokens.SPACE)
              .append(IN)
              .append(Tokens.SPACE)
              .append(Tokens.LPAREN);
        append(obj.getSubquery());
        buffer.append(Tokens.RPAREN);
    }

    public void visit(Update obj) {
        buffer.append(UPDATE)
              .append(Tokens.SPACE);
        appendSourceComment(obj);
        append(obj.getTable());
        buffer.append(Tokens.SPACE)
              .append(SET)
              .append(Tokens.SPACE);
        append(obj.getChanges());
        if (obj.getWhere() != null) {
            buffer.append(Tokens.SPACE)
                  .append(WHERE)
                  .append(Tokens.SPACE);
            append(obj.getWhere());
        }
    }

    public void visit(SetClause clause) {
        shortNameOnly = true;
        append(clause.getSymbol());
        shortNameOnly = false;
        buffer.append(Tokens.SPACE).append(Tokens.EQ).append(Tokens.SPACE);
        append(clause.getValue());
    }

    public void visit(SetQuery obj) {
        if (obj.getWith() != null) {
            append(obj.getWith());
        }
        appendSetQuery(obj, obj.getLeftQuery(), false);

        buffer.append(Tokens.SPACE);

        appendSetOperation(obj.getOperation());

        if(obj.isAll()) {
            buffer.append(Tokens.SPACE);
            buffer.append(ALL);
        }
        buffer.append(Tokens.SPACE);

        appendSetQuery(obj, obj.getRightQuery(), true);

        OrderBy orderBy = obj.getOrderBy();
        if(orderBy != null) {
            buffer.append(Tokens.SPACE);
            append(orderBy);
        }

        Limit limit = obj.getLimit();
        if(limit != null) {
            buffer.append(Tokens.SPACE);
            append(limit);
        }
    }

    protected void appendSetOperation(SetQuery.Operation operation) {
        buffer.append(operation);
    }

    protected boolean useParensForSetQueries() {
        return false;
    }

    protected void appendSetQuery(SetQuery parent, QueryExpression obj, boolean right) {
        if(shouldNestSetChild(parent, obj, right)) {
            buffer.append(Tokens.LPAREN);
            append(obj);
            buffer.append(Tokens.RPAREN);
        } else {
            if (!parent.isAll() && obj instanceof SetQuery) {
                ((SetQuery)obj).setAll(false);
            }
            append(obj);
        }
    }

    protected boolean shouldNestSetChild(SetQuery parent, QueryExpression obj,
            boolean right) {
        return (!(obj instanceof SetQuery) && useParensForSetQueries())
                || obj.getLimit() != null || obj.getOrderBy() != null || (obj instanceof SetQuery
                        && ((right && parent.isAll() && !((SetQuery)obj).isAll())
                                || ((parent.getOperation() == Operation.INTERSECT || right) && parent.getOperation() != ((SetQuery)obj).getOperation())));
    }

    @Override
    public void visit(With obj) {
        appendedSourceComment = true;
        appendWithKeyword(obj);
        buffer.append(Tokens.SPACE);
        append(obj.getItems());
        buffer.append(Tokens.SPACE);
        appendedSourceComment = false;
    }

    protected void appendWithKeyword(With obj) {
        buffer.append(WITH);
    }

    @Override
    public void visit(WithItem obj) {
        append(obj.getTable());
        buffer.append(Tokens.SPACE);
        if (obj.getColumns() != null) {
            buffer.append(Tokens.LPAREN);
            shortNameOnly = true;
            append(obj.getColumns());
            shortNameOnly = false;
            buffer.append(Tokens.RPAREN);
            buffer.append(Tokens.SPACE);
        }
        buffer.append(AS);
        buffer.append(Tokens.SPACE);
        buffer.append(Tokens.LPAREN);
        if (obj.getSubquery() == null) {
            buffer.append(UNDEFINED_PARAM);
        } else {
            append(obj.getSubquery());
        }
        buffer.append(Tokens.RPAREN);
    }

    @Override
    public void visit(WindowFunction windowFunction) {
        append(windowFunction.getFunction());
        buffer.append(Tokens.SPACE);
        buffer.append(OVER);
        buffer.append(Tokens.SPACE);
        append(windowFunction.getWindowSpecification());
    }

    @Override
    public void visit(WindowSpecification windowSpecification) {
        buffer.append(Tokens.LPAREN);
        boolean needsSpace = false;
        if (windowSpecification.getPartition() != null) {
            buffer.append(PARTITION);
            buffer.append(Tokens.SPACE);
            buffer.append(BY);
            buffer.append(Tokens.SPACE);
            append(windowSpecification.getPartition());
            needsSpace = true;
        }
        if (windowSpecification.getOrderBy() != null) {
            if (needsSpace) {
                buffer.append(Tokens.SPACE);
            }
            append(windowSpecification.getOrderBy());
            needsSpace = true;
        }
        if (windowSpecification.getWindowFrame() != null) {
            if (needsSpace) {
                buffer.append(Tokens.SPACE);
            }
            append(windowSpecification.getWindowFrame());
        }
        buffer.append(Tokens.RPAREN);
    }

    @Override
    public void visit(WindowFrame windowFrame) {
        buffer.append(windowFrame.getMode().name());
        buffer.append(Tokens.SPACE);
        if (windowFrame.getEnd() != null) {
            buffer.append(Reserved.BETWEEN);
            buffer.append(Tokens.SPACE);
        }
        appendFrameBound(windowFrame.getStart());
        if (windowFrame.getEnd() != null) {
            buffer.append(Tokens.SPACE);
            buffer.append(Reserved.AND);
            buffer.append(Tokens.SPACE);
            appendFrameBound(windowFrame.getEnd());
        }
    }

    private void appendFrameBound(FrameBound bound) {
        if (bound.getBoundMode() == BoundMode.CURRENT_ROW) {
            buffer.append(NonReserved.CURRENT);
            buffer.append(Tokens.SPACE);
            buffer.append(ROW);
        } else {
            if (bound.getBound() != null) {
                buffer.append(bound.getBound());
            } else {
                buffer.append(NonReserved.UNBOUNDED);
            }
            buffer.append(Tokens.SPACE);
            buffer.append(bound.getBoundMode().name());
        }
    }

    @Override
    public void visit(Array array) {
        buffer.append(Tokens.LPAREN);
        append(array.getExpressions());
        if (array.getExpressions().size() == 1) {
            buffer.append(Tokens.COMMA);
        }
        buffer.append(Tokens.RPAREN);
    }

    /**
     * Gets the SQL string representation for a given LanguageObject.
     * @param obj the root of the LanguageObject hierarchy that needs to be
     * converted. This can be any subtree, and does not need to be a top-level
     * command
     * @return the SQL representation of that LanguageObject hierarchy
     */
    public static String getSQLString(LanguageObject obj) {
        SQLStringVisitor visitor = new SQLStringVisitor();
        visitor.append(obj);
        return visitor.toString();
    }

    protected boolean useParensForJoins() {
        return false;
    }

    protected boolean useSelectLimit() {
        return false;
    }

    public interface Substitutor {
        void substitute(Argument arg, StringBuilder builder, int index);
    }

    public static void parseNativeQueryParts(String nativeQuery, List<Argument> list, StringBuilder stringBuilder, Substitutor substitutor) {
        Matcher m = pattern.matcher(nativeQuery);
        for (int i = 0; i < nativeQuery.length();) {
            if (!m.find(i)) {
                stringBuilder.append(nativeQuery.substring(i));
                break;
            }
            if (m.start() != i) {
                stringBuilder.append(nativeQuery.substring(i, m.start()));
            }
            String match = m.group();
            int end = match.lastIndexOf('$');
            if ((end&0x1) == 1) {
                //escaped
                stringBuilder.append(match.substring((end+1)/2));
            } else {
                if (end != 0) {
                    stringBuilder.append(match.substring(0, end/2));
                }
                int index = Integer.parseInt(match.substring(end + 1))-1;
                if (index < 0 || index >= list.size()) {
                    throw new IllegalArgumentException(DataPlugin.Util.getString("SQLConversionVisitor.invalid_parameter", index+1, list.size())); //$NON-NLS-1$
                }
                Argument arg = list.get(index);
                if (arg.getDirection() != Direction.IN) {
                    throw new IllegalArgumentException(DataPlugin.Util.getString("SQLConversionVisitor.not_in_parameter", index+1)); //$NON-NLS-1$
                }
                substitutor.substitute(arg, stringBuilder, index);
            }
            i = m.end();
        }
    }

    @Override
    public void visit(IsDistinct isDistinct) {
        append(isDistinct.getLeftExpression());
        buffer.append(Tokens.SPACE);
        buffer.append(IS);
        buffer.append(Tokens.SPACE);
        if (isDistinct.isNegated()) {
            buffer.append(NOT);
            buffer.append(Tokens.SPACE);
        }
        buffer.append(DISTINCT);
        buffer.append(Tokens.SPACE);
        buffer.append(FROM);
        buffer.append(Tokens.SPACE);
        append(isDistinct.getRightExpression());
    }
}
