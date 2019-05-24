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
package org.teiid.translator.solr;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.teiid.resource.api.ConnectionFactory;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.FunctionModifier;

@Translator(name = "solr", description = "A translator for Solr search platform")
public class SolrExecutionFactory extends ExecutionFactory<ConnectionFactory, SolrConnection> {
    protected Map<String, FunctionModifier> functionModifiers = new TreeMap<String, FunctionModifier>(String.CASE_INSENSITIVE_ORDER);

    public SolrExecutionFactory() {
        super();
        setSourceRequiredForMetadata(true);
        setTransactionSupport(TransactionSupport.NONE);
        registerFunctionModifier("%", new AliasModifier("mod"));//$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier("+", new AliasModifier("sum"));//$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier("-", new AliasModifier("sub"));//$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier("*", new AliasModifier("product"));//$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier("/", new AliasModifier("div"));//$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.POWER, new AliasModifier("pow"));//$NON-NLS-1$
        setMaxInCriteriaSize(1024); //default for solr
    }

    @Override
    public void start() throws TranslatorException {
        super.start();
    }

    @Override
    public MetadataProcessor<SolrConnection> getMetadataProcessor() {
        return new SolrMetadataProcessor();
    }

    public void registerFunctionModifier(String name, FunctionModifier modifier) {
        this.functionModifiers.put(name, modifier);
    }

    public Map<String, FunctionModifier> getFunctionModifiers() {
        return this.functionModifiers;
    }


    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(getDefaultSupportedFunctions());

        supportedFunctions.add(SourceSystemFunctions.MOD);
        supportedFunctions.add(SourceSystemFunctions.POWER);
        supportedFunctions.add(SourceSystemFunctions.ABS);
        supportedFunctions.add(SourceSystemFunctions.LOG);
        supportedFunctions.add(SourceSystemFunctions.SQRT);

        return supportedFunctions;
    }

    public List<String> getDefaultSupportedFunctions(){
        return Arrays.asList(new String[] { "+", "-", "*", "/", "%"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }

    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            SolrConnection connection) throws TranslatorException {
        return new SolrQueryExecution(this, command, executionContext, metadata,connection);
    }

    @Override
    public UpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, SolrConnection connection) throws TranslatorException {
        return new SolrUpdateExecution(this, command, executionContext, metadata, connection);
    }


    public Object convertFromSolrType(final Object value, final Class<?> expectedType) {
        if (value == null) {
            return null;
        }

        if (expectedType.isInstance(value)) {
            return value;
        }
        try {
            if (expectedType.isArray()) {
                ArrayList multiValues = (ArrayList)value;
                Object transformed = Array.newInstance(expectedType.getComponentType(), multiValues.size());
                for (int i = 0; i < multiValues.size(); i++) {
                    Object obj = multiValues.get(i);
                    if (obj == null) {
                        Array.set(transformed, i, obj);
                        continue;
                    }

                    if (expectedType.getComponentType().isInstance(obj)) {
                        Array.set(transformed, i, obj);
                        continue;
                    }

                    if (DataTypeManager.isTransformable(obj.getClass(), expectedType.getComponentType())) {
                        Array.set(transformed, i, DataTypeManager.transformValue(obj, expectedType.getComponentType()));
                    }
                    else {
                        throw new TeiidRuntimeException(SolrPlugin.Event.TEIID20001, SolrPlugin.Util.gs(SolrPlugin.Event.TEIID20001, value, expectedType.getName()));
                    }
                }
                return transformed;
            }

            if (DataTypeManager.isTransformable(value.getClass(), expectedType)) {
                return DataTypeManager.transformValue(value, expectedType);
            }
            throw new TeiidRuntimeException(SolrPlugin.Event.TEIID20001, SolrPlugin.Util.gs(SolrPlugin.Event.TEIID20001, value, expectedType.getName()));
        } catch (TransformationException e) {
            throw new TeiidRuntimeException(e);
        }
    }

    @Override
    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

    @Override
    public boolean supportsInCriteria() {
        return true;
    }

    @Override
    public boolean supportsRowLimit() {
        return true;
    }

    @Override
    public boolean supportsNotCriteria() {
        return true;
    }

    @Override
    public boolean supportsLikeCriteria() {
        return true;
    }

    @Override
    public boolean supportsOrderBy() {
        return true;
    }

    @Override
    public boolean supportsCompareCriteriaOrdered(){
        return true;
    }

    @Override
    public boolean supportsOrCriteria(){
        return true;
    }

    @Override
    public boolean supportsOnlyLiteralComparison(){
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated(){
        return true;
    }

    @Override
    public boolean supportsSelectExpression() {
        return true;
    }

    @Override
    public boolean supportsBulkUpdate() {
        return true;
    }

    @Override
    public boolean supportsAggregatesCountStar() {
        return true;
    }

    @Override
    public boolean returnsSingleUpdateCount() {
        return true;
    }
}
