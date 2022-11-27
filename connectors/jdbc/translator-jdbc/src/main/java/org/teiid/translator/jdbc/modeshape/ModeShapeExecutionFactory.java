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

package org.teiid.translator.jdbc.modeshape;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.*;

import java.sql.Connection;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.*;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.Join.JoinType;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.JDBCMetadataProcessor;
/**
 * Translator class for accessing the ModeShape JCR repository.
 */
@Translator(name="modeshape", description="A translator for the open source Modeshape JCR Repository")
public class ModeShapeExecutionFactory extends JDBCExecutionFactory {

    private static final String JCR = "JCR"; //$NON-NLS-1$
    private static final String JCR_REFERENCE = "JCR_REFERENCE";//$NON-NLS-1$
    private static final String JCR_CONTAINS = "JCR_CONTAINS";//$NON-NLS-1$
    private static final String JCR_ISSAMENODE = "JCR_ISSAMENODE";//$NON-NLS-1$
    private static final String JCR_ISDESCENDANTNODE = "JCR_ISDESCENDANTNODE";//$NON-NLS-1$
    private static final String JCR_ISCHILDNODE = "JCR_ISCHILDNODE";//$NON-NLS-1$

    public ModeShapeExecutionFactory() {
        setUseBindVariables(false);
    }

    @Override
    public void start() throws TranslatorException {
        super.start();

        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("UPPER")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LCASE,new AliasModifier("LOWER")); //$NON-NLS-1$

        registerFunctionModifier(JCR_ISCHILDNODE, new IdentifierFunctionModifier());
        registerFunctionModifier(JCR_ISDESCENDANTNODE, new IdentifierFunctionModifier());
        registerFunctionModifier(JCR_ISSAMENODE, new IdentifierFunctionModifier());
        registerFunctionModifier(JCR_REFERENCE, new IdentifierFunctionModifier());
        registerFunctionModifier(JCR_CONTAINS, new IdentifierFunctionModifier());

        addPushDownFunction(JCR, JCR_ISCHILDNODE, BOOLEAN, STRING, STRING);
        addPushDownFunction(JCR, JCR_ISDESCENDANTNODE, BOOLEAN, STRING, STRING);
        addPushDownFunction(JCR, JCR_ISSAMENODE, BOOLEAN, STRING, STRING);
        addPushDownFunction(JCR, JCR_CONTAINS, BOOLEAN, STRING, STRING);
        addPushDownFunction(JCR, JCR_REFERENCE, STRING, STRING);

        LogManager.logTrace(LogConstants.CTX_CONNECTOR, "ModeShape Translator Started"); //$NON-NLS-1$
     }

    @Override
    public String translateLiteralDate(Date dateValue) {
        return "CAST('" + formatDateValue(dateValue) + "' AS DATE)"; //$NON-NLS-1$//$NON-NLS-2$
    }

    @Override
    public String translateLiteralTime(Time timeValue) {
        return "CAST('" + formatDateValue(timeValue) + "' AS DATE)"; //$NON-NLS-1$//$NON-NLS-2$
    }

    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        return "CAST('" + formatDateValue(timestampValue) + "' AS DATE)"; //$NON-NLS-1$//$NON-NLS-2$
    }

    @Override
    public String translateLiteralBoolean(Boolean booleanValue) {
        return "CAST('" + booleanValue.toString() + "' AS BOOLEAN)"; //$NON-NLS-1$//$NON-NLS-2$
    }

    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());
        supportedFunctions.add(SourceSystemFunctions.UCASE);
        supportedFunctions.add(SourceSystemFunctions.LCASE);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        return supportedFunctions;
    }

    @Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
        if (obj instanceof Comparison) {
            Comparison compare = (Comparison)obj;
            if (compare.getLeftExpression().getType() == TypeFacility.RUNTIME_TYPES.BOOLEAN
                    && compare.getLeftExpression() instanceof Function
                    && compare.getRightExpression() instanceof Literal) {
                boolean isTrue = Boolean.TRUE.equals(((Literal)compare.getRightExpression()).getValue());
                if ((isTrue && compare.getOperator() == Operator.EQ) || (!isTrue && compare.getOperator() == Operator.NE)) {
                    return Arrays.asList(compare.getLeftExpression());
                }
                if ((!isTrue && compare.getOperator() == Operator.EQ) || (isTrue && compare.getOperator() == Operator.NE)) {
                    return Arrays.asList("NOT ", compare.getLeftExpression()); //$NON-NLS-1$
                }
            }
        } else if (obj instanceof Not) {
            Not not = (Not)obj;
            return Arrays.asList("NOT ", not.getCriteria()); //$NON-NLS-1$
        }
        return super.translate(obj, context);
    }

    @Override
    public boolean useBindVariables() {
        return false;
    }

    @Override
    public boolean supportsAggregatesAvg() {
        return false;
    }

    @Override
    public boolean supportsAggregatesCountStar() {
        return false;
    }

    @Override
    public boolean supportsAggregatesCount() {
        return false;
    }

    @Override
    public boolean supportsAggregatesEnhancedNumeric() {
        return false;
    }

    @Override
    public boolean supportsAggregatesMax() {
        return false;
    }

    @Override
    public boolean supportsAggregatesMin() {
        return false;
    }

    @Override
    public boolean supportsAggregatesSum() {
        return false;
    }

    @Override
    public boolean supportsGroupBy() {
        return false;
    }

    @Override
    public boolean supportsHaving() {
        return false;
    }

    @Override
    public boolean supportsSelectExpression() {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return false;
    }

    @Override
    public boolean supportsExistsCriteria() {
        return false;
    }

    @Override
    public boolean supportsInCriteriaSubquery() {
        return false;
    }

    @Override
    public boolean supportsInlineViews() {
        return false;
    }

    @Override
    public boolean supportsOrderByNullOrdering() {
        return false;
    }

    @Override
    public boolean supportsQuantifiedCompareCriteriaAll() {
        return false;
    }

    @Override
    public boolean supportsQuantifiedCompareCriteriaSome() {
        return false;
    }

    @Override
    public boolean supportsScalarSubqueries() {
        return false;
    }

    @Override
    public boolean supportsSearchedCaseExpressions() {
        return false;
    }

    @Override
    public boolean supportsExcept() {
        return true;
    }

    @Override
    public boolean supportsIntersect() {
        return true;
    }

    @Override
    public boolean supportsSetQueryOrderBy() {
        return false;
    }

    @Override
    @Deprecated
    protected JDBCMetadataProcessor createMetadataProcessor() {
        return (JDBCMetadataProcessor)getMetadataProcessor();
    }

    @Override
    public MetadataProcessor<Connection> getMetadataProcessor() {
        return new ModeShapeJDBCMetdataProcessor();
    }

    /**
     * TEIID-3102 - ModeShape requires the use of JOIN, and not ',' when joining tables.
     * {@inheritDoc}
     *
     * @see org.teiid.translator.ExecutionFactory#useAnsiJoin()
     */
    @Override
    public boolean useAnsiJoin() {
        return true;
    }

    public List<?> translateCommand(Command command, ExecutionContext context) {
        if (!(command instanceof Select)) {
            return null;
        }
        Select select = (Select)command;
        TableReference tableReference = select.getFrom().get(0);
        moveCondition(select, tableReference);
        return null;
    }

    /**
     * only a single join predicate is supported, so move up conditions if possible
     */
    private void moveCondition(Select select, TableReference tableReference) {
        if (!(tableReference instanceof Join)) {
            return;
        }
        Join join = (Join)tableReference;
        if (join.getJoinType() != JoinType.INNER_JOIN) {
            return;
        }
        while (join.getCondition() instanceof AndOr) {
            AndOr andOr = (AndOr) join.getCondition();
            if (andOr.getOperator() == AndOr.Operator.OR) {
                break;
            }
            Condition c = andOr.getLeftCondition();
            select.setWhere(LanguageUtil.combineCriteria(select.getWhere(), c, getLanguageFactory()));
            join.setCondition(andOr.getRightCondition());
        }
        moveCondition(select, join.getLeftItem());
        moveCondition(select, join.getRightItem());
    }
}
