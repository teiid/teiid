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

package org.teiid.translator.yahoo;

import java.util.Arrays;

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
@Translator(name="yahoo", description="A translator for testing to obtain stock quotes from Yahoo web site")
public class YahooExecutionFactory extends ExecutionFactory<Object, Object> {

    public static final int YAHOO_MAX_SET_SIZE = 100;

    public YahooExecutionFactory() {
        setMaxInCriteriaSize(YAHOO_MAX_SET_SIZE);
        setSourceRequiredForMetadata(false);
        setTransactionSupport(TransactionSupport.NONE);
    }

    @Override
    public void start() throws TranslatorException {
    }

    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connectionFactory)
            throws TranslatorException {
        return new YahooExecution((Select)command);
    }

    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

    public boolean supportsInCriteria() {
        return true;
    }

    @Override
    public boolean isSourceRequired() {
        return false;
    }

    @Override
    public void getMetadata(MetadataFactory metadataFactory, Object connection) throws TranslatorException {
        Table t = metadataFactory.addTable("Stock"); //$NON-NLS-1$
        metadataFactory.addColumn("symbol", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        Column c = metadataFactory.addColumn("last", DataTypeManager.DefaultDataTypes.DOUBLE, t); //$NON-NLS-1$
        c.setSearchType(SearchType.Unsearchable);
        c = metadataFactory.addColumn("date", DataTypeManager.DefaultDataTypes.DATE, t); //$NON-NLS-1$
        c.setSearchType(SearchType.Unsearchable);
        c = metadataFactory.addColumn("time", DataTypeManager.DefaultDataTypes.TIME, t); //$NON-NLS-1$
        c.setSearchType(SearchType.Unsearchable);
        c = metadataFactory.addColumn("change", DataTypeManager.DefaultDataTypes.DOUBLE, t); //$NON-NLS-1$
        c.setSearchType(SearchType.Unsearchable);
        c = metadataFactory.addColumn("open", DataTypeManager.DefaultDataTypes.DOUBLE, t); //$NON-NLS-1$
        c.setSearchType(SearchType.Unsearchable);
        c = metadataFactory.addColumn("high", DataTypeManager.DefaultDataTypes.DOUBLE, t); //$NON-NLS-1$
        c.setSearchType(SearchType.Unsearchable);
        c = metadataFactory.addColumn("low", DataTypeManager.DefaultDataTypes.DOUBLE, t); //$NON-NLS-1$
        c.setSearchType(SearchType.Unsearchable);
        c = metadataFactory.addColumn("volume", DataTypeManager.DefaultDataTypes.BIG_INTEGER, t); //$NON-NLS-1$
        c.setSearchType(SearchType.Unsearchable);
        metadataFactory.addAccessPattern("needs_symbol", Arrays.asList("symbol"), t); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Override
    public boolean supportsOnlyLiteralComparison() {
        return true;
    }

}
