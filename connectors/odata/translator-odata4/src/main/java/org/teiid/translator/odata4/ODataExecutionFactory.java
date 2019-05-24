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
package org.teiid.translator.odata4;

import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import org.teiid.resource.api.ConnectionFactory;

import org.apache.olingo.client.api.edm.xml.XMLMetadata;
import org.apache.olingo.client.core.serialization.ClientODataDeserializerImpl;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.StringUtil;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.Literal;
import org.teiid.language.QueryExpression;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.*;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.ws.BinaryWSProcedureExecution;
import org.teiid.translator.ws.WSConnection;

/**
 * TODO:
 * Type comparison    isof(T), isof(x, T)    Whether targeted instance can be converted to the specified type.
 * media streams are generally not supported yet. (blobs, clobs)
 */
@Translator(name="odata4", description="A translator for making OData V4 data service calls")
public class ODataExecutionFactory extends ExecutionFactory<ConnectionFactory, WSConnection> {

    public final static TimeZone DEFAULT_TIME_ZONE = TimeZone.getDefault();

    static final String INVOKE_HTTP = "invokeHttp"; //$NON-NLS-1$
    protected Map<String, FunctionModifier> functionModifiers = new TreeMap<String, FunctionModifier>(String.CASE_INSENSITIVE_ORDER);
    private boolean supportsOdataFilter;
    private boolean supportsOdataOrderBy;
    private boolean supportsOdataCount;
    private boolean supportsOdataSkip;
    private boolean supportsOdataTop;
    private boolean supportsUpdates = true;
    private XMLMetadata serviceMatadata;

    public ODataExecutionFactory() {
        setSourceRequiredForMetadata(true);
        setSupportsInnerJoins(true);
        setSupportsOrderBy(true);
        setSupportsOuterJoins(true);
        setSupportsFullOuterJoins(true);
        setSupportedJoinCriteria(SupportedJoinCriteria.KEY);

        setSupportsOdataCount(true);
        setSupportsOdataFilter(true);
        setSupportsOdataOrderBy(true);
        setSupportsOdataSkip(false); // based on document based on cursoring, this will not be correct
        setSupportsOdataTop(false);
        setTransactionSupport(TransactionSupport.NONE);
        registerFunctionModifier(SourceSystemFunctions.CONVERT, new AliasModifier("cast")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOCATE, new FunctionModifier() {

            @Override
            public List<?> translate(Function function) {
                function.setName(SourceSystemFunctions.ADD_OP);

                Expression param1 = function.getParameters().get(0);
                Expression param2 = function.getParameters().get(1);

                Function indexOf = new Function("indexof", Arrays.asList(param2, param1), TypeFacility.RUNTIME_TYPES.INTEGER); //$NON-NLS-1$
                indexOf.setMetadataObject(function.getMetadataObject());
                function.getParameters().set(0, indexOf);
                function.getParameters().set(1, new Literal(1, TypeFacility.RUNTIME_TYPES.INTEGER));
                return null;
            }
        });
        registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new FunctionModifier() {

            @Override
            public List<?> translate(Function function) {
                if (function.getParameters().size() != 3) {
                    return null;
                }
                Expression param2 = function.getParameters().get(1);

                param2 = new Function(SourceSystemFunctions.ADD_OP, Arrays.asList(param2, new Literal(1, TypeFacility.RUNTIME_TYPES.INTEGER)), TypeFacility.RUNTIME_TYPES.INTEGER);
                function.getParameters().set(1, param2);
                return null;
            }
        });
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("tolower")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("toupper")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new AliasModifier("day")); //$NON-NLS-1$

        registerFunctionModifier(SourceSystemFunctions.ST_DISTANCE, new AliasModifier("geo.distance")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.ST_INTERSECTS, new AliasModifier("geo.intersects")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.ST_LENGTH, new AliasModifier("geo.length")); //$NON-NLS-1$

        addPushDownFunction("odata", "startswith", TypeFacility.RUNTIME_NAMES.BOOLEAN, //$NON-NLS-1$ //$NON-NLS-2$
                TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING);
        addPushDownFunction("odata", "contains", TypeFacility.RUNTIME_NAMES.BOOLEAN, //$NON-NLS-1$ //$NON-NLS-2$
                TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING);
    }

    @Override
    public void start() throws TranslatorException {
        super.start();
    }

    @Override
    public void getMetadata(MetadataFactory metadataFactory, WSConnection conn) throws TranslatorException {
        ODataMetadataProcessor metadataProcessor = (ODataMetadataProcessor)getMetadataProcessor();
        PropertiesUtils.setBeanProperties(metadataProcessor, metadataFactory.getModelProperties(), "importer"); //$NON-NLS-1$
        metadataProcessor.setExecutionfactory(this);
        metadataProcessor.process(metadataFactory, conn);
    }

    @Override
    public MetadataProcessor<WSConnection> getMetadataProcessor() {
        return new ODataMetadataProcessor();
    }

    protected XMLMetadata getSchema(WSConnection conn) throws TranslatorException {
        if (this.serviceMatadata == null) {
            try {
                BaseQueryExecution execution = new BaseQueryExecution(this, null, null, conn);
                Map<String, List<String>> headers = new HashMap<String, List<String>>();
                BinaryWSProcedureExecution call = execution.invokeHTTP("GET", "$metadata", null, headers); //$NON-NLS-1$ //$NON-NLS-2$
                if (call.getResponseCode() != HttpStatusCode.OK.getStatusCode()) {
                    throw execution.buildError(call);
                }

                Blob out = (Blob)call.getOutputParameterValues().get(0);
                ClientODataDeserializerImpl deserializer =
                        new ClientODataDeserializerImpl(false, ContentType.APPLICATION_XML);
                this.serviceMatadata = deserializer.toMetadata(out.getBinaryStream());
                return this.serviceMatadata;
            } catch (SQLException e) {
                throw new TranslatorException(e);
            } catch (Exception e) {
                throw new TranslatorException(e);
            }
        }
        return this.serviceMatadata;
    }

    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            WSConnection connection) throws TranslatorException {
        return new ODataQueryExecution(this, command, executionContext, metadata, connection);
    }

    @Override
    public ProcedureExecution createProcedureExecution(Call command,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            WSConnection connection) throws TranslatorException {
        String nativeQuery = command.getMetadataObject().getProperty(
                SQLStringVisitor.TEIID_NATIVE_QUERY, false);
        if (nativeQuery != null) {
            throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17014));
        }
        return new ODataProcedureExecution(command, this, executionContext, metadata, connection);
    }

    @Override
    public UpdateExecution createUpdateExecution(Command command,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            WSConnection connection) throws TranslatorException {
        if (supportsUpdates()) {
            return new ODataUpdateExecution(command, this, executionContext,metadata, connection);
        } else {
            throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17030));
        }
    }

    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(getDefaultSupportedFunctions());

        // String functions
        supportedFunctions.add(SourceSystemFunctions.ENDSWITH);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.LOCATE); //indexof
        supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
        supportedFunctions.add(SourceSystemFunctions.LCASE); //tolower
        supportedFunctions.add(SourceSystemFunctions.UCASE); //toupper
        supportedFunctions.add(SourceSystemFunctions.TRIM);
        supportedFunctions.add(SourceSystemFunctions.CONCAT);
        supportedFunctions.add(SourceSystemFunctions.CONVERT);

        // date functions
        supportedFunctions.add(SourceSystemFunctions.YEAR);
        supportedFunctions.add(SourceSystemFunctions.MONTH);
        supportedFunctions.add(SourceSystemFunctions.NOW);
        supportedFunctions.add(SourceSystemFunctions.HOUR);
        supportedFunctions.add(SourceSystemFunctions.MINUTE);
        supportedFunctions.add(SourceSystemFunctions.SECOND);
        supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH);

        // arithmetic functions
        supportedFunctions.add(SourceSystemFunctions.ROUND);
        supportedFunctions.add(SourceSystemFunctions.FLOOR);
        supportedFunctions.add(SourceSystemFunctions.CEILING);
        supportedFunctions.add(SourceSystemFunctions.MOD);

        // geospatial functions
        supportedFunctions.add(SourceSystemFunctions.ST_DISTANCE);
        supportedFunctions.add(SourceSystemFunctions.ST_INTERSECTS);
        supportedFunctions.add(SourceSystemFunctions.ST_LENGTH);

        return supportedFunctions;
    }

    /**
     * Return a map of function name to FunctionModifier.
     * @return Map of function name to FunctionModifier.
     */
    public Map<String, FunctionModifier> getFunctionModifiers() {
        return this.functionModifiers;
    }

    /**
     * Add the {@link FunctionModifier} to the set of known modifiers.
     * @param name
     * @param modifier
     */
    public void registerFunctionModifier(String name, FunctionModifier modifier) {
        this.functionModifiers.put(name, modifier);
    }


    public List<String> getDefaultSupportedFunctions(){
        return Arrays.asList(new String[] { "+", "-", "*", "/" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @TranslatorProperty(display="Supports $Filter", description="True, $filter is supported", advanced=true)
    public boolean supportsOdataFilter() {
        return supportsOdataFilter;
    }

    public void setSupportsOdataFilter(boolean supports) {
        this.supportsOdataFilter = supports;
    }

    @TranslatorProperty(display="Supports $OrderBy", description="True, $orderby is supported", advanced=true)
    public boolean supportsOdataOrderBy() {
        return supportsOdataOrderBy;
    }

    public void setSupportsOdataOrderBy(boolean supports) {
        this.supportsOdataOrderBy = supports;
    }

    @TranslatorProperty(display="Supports $count", description="True, $count is supported", advanced=true)
    public boolean supportsOdataCount() {
        return supportsOdataCount;
    }

    public void setSupportsOdataCount(boolean supports) {
        this.supportsOdataCount = supports;
    }

    @TranslatorProperty(display="Supports $skip", description="True, $skip is supported", advanced=true)
    public boolean supportsOdataSkip() {
        return supportsOdataSkip;
    }

    public void setSupportsOdataSkip(boolean supports) {
        this.supportsOdataSkip = supports;
    }

    @TranslatorProperty(display="Supports $top", description="True, $top is supported", advanced=true)
    public boolean supportsOdataTop() {
        return supportsOdataTop;
    }

    public void setSupportsOdataTop(boolean supports) {
        this.supportsOdataTop = supports;
    }

    @TranslatorProperty(display="Supports Updates",
            description="True, if(PUT,PATCH,DELETE) operations supported",
            advanced=true)
    public boolean supportsUpdates() {
        return supportsUpdates;
    }

    public void setSupportsUpdates(boolean supports) {
        this.supportsUpdates = supports;
    }

    @Override
    public boolean supportsCompareCriteriaEquals() {
        return this.supportsOdataFilter;
    }

    @Override
    public boolean supportsCompareCriteriaOrdered() {
        return supportsOdataFilter;
    }

    @Override
    public boolean supportsIsNullCriteria() {
        return supportsOdataFilter;
    }

    @Override
    public boolean supportsOrCriteria() {
        return supportsOdataFilter;
    }

    @Override
    public boolean supportsNotCriteria() {
        return supportsOdataFilter;
    }

    @Override
    public boolean supportsInCriteria() {
        return false; // not directly supported, the engine will compensate
    }

    @Override
    public boolean supportsQuantifiedCompareCriteriaSome() {
        return false; // TODO:for ANY
    }

    @Override
    public boolean supportsQuantifiedCompareCriteriaAll() {
        return false; // TODO:FOR ALL
    }

    @Override
    @TranslatorProperty(display="Supports ORDER BY",
        description="True, if this connector supports ORDER BY", advanced=true)
    public boolean supportsOrderBy() {
        return supportsOdataOrderBy;
    }

    @Override
    public boolean supportsOrderByUnrelated() {
        return this.supportsOdataOrderBy;
    }

    @Override
    public boolean supportsAggregatesCount() {
        return supportsOdataCount;
    }

    @Override
    public boolean supportsAggregatesCountStar() {
        return supportsOdataCount;
    }

    @Override
    public boolean supportsRowLimit() {
        return supportsOdataTop;
    }

    @Override
    public boolean supportsRowOffset() {
        return supportsOdataSkip;
    }

    @Override
    public boolean supportsOnlyLiteralComparison() {
        return true;
    }

    @Override
    public boolean useAnsiJoin() {
        return true;
    }

    public boolean supportsArrayType() {
        return true;
    }

    protected String escapeString(String str, String quote) {
        return StringUtil.replaceAll(str, quote, quote + quote);
    }

    @Override
    public boolean supportsGeographyType() {
        return true;
    }
}
