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
package org.teiid.translator.odata;

import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.odata4j.core.ODataConstants;
import org.odata4j.core.ODataConstants.Charsets;
import org.odata4j.core.ODataVersion;
import org.odata4j.core.OEntity;
import org.odata4j.core.OError;
import org.odata4j.core.OProperty;
import org.odata4j.edm.EdmComplexType;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.format.Entry;
import org.odata4j.format.Feed;
import org.odata4j.format.FormatParser;
import org.odata4j.format.FormatParserFactory;
import org.odata4j.format.FormatType;
import org.odata4j.format.Settings;
import org.odata4j.format.xml.AtomFeedFormatParser;
import org.odata4j.stax2.XMLEvent2;
import org.odata4j.stax2.XMLEventReader2;
import org.odata4j.stax2.util.StaxUtil;
import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Call;
import org.teiid.language.Literal;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.ws.BinaryWSProcedureExecution;
import org.teiid.translator.ws.WSConnection;

public class BaseQueryExecution {
    protected WSConnection connection;
    protected ODataExecutionFactory translator;
    protected RuntimeMetadata metadata;
    protected ExecutionContext executionContext;

    public BaseQueryExecution(ODataExecutionFactory translator, ExecutionContext executionContext, RuntimeMetadata metadata, WSConnection connection) {
        this.metadata = metadata;
        this.executionContext = executionContext;
        this.translator = translator;
        this.connection = connection;
    }

    protected Feed parse(Blob blob, ODataVersion version, String entityTable, EdmDataServices edsMetadata) throws TranslatorException {
        try {
            // if parser is written to return raw objects; then we can avoid some un-necessary object creation
            // due to time, I am not pursuing that now.
            FormatParser<Feed> parser = FormatParserFactory.getParser(
                    Feed.class, FormatType.ATOM, new Settings(version, edsMetadata, entityTable, null));
            return parser.parse(new InputStreamReader(blob.getBinaryStream()));
        } catch (SQLException e) {
            throw new TranslatorException(ODataPlugin.Event.TEIID17010, e, e.getMessage());
        }
    }

    protected static ODataVersion getDataServiceVersion(String headerValue) {
        ODataVersion version = ODataConstants.DATA_SERVICE_VERSION;
        if (headerValue != null) {
            String[] str = headerValue.split(";"); //$NON-NLS-1$
            version = ODataVersion.parse(str[0]);
        }
        return version;
    }

    protected ODataEntitiesResponse executeWithReturnEntity(String method, String uri, String payload, String entityTable, EdmDataServices edsMetadata, String eTag, Status... expectedStatus) throws TranslatorException {
        Map<String, List<String>> headers = getDefaultHeaders();
        if (eTag != null) {
            headers.put("If-Match", Arrays.asList(eTag)); //$NON-NLS-1$
        }

        if (payload != null) {
            headers.put("Content-Type", Arrays.asList("application/atom+xml")); //$NON-NLS-1$ //$NON-NLS-2$
        }

        BinaryWSProcedureExecution execution = executeDirect(method, uri, payload, headers);
        for (Status status:expectedStatus) {
            if (status.getStatusCode() == execution.getResponseCode()) {
                if (execution.getResponseCode() != Status.NO_CONTENT.getStatusCode()
                        && execution.getResponseCode() != Status.NOT_FOUND.getStatusCode()) {
                    Blob blob = (Blob)execution.getOutputParameterValues().get(0);
                    ODataVersion version = getODataVersion(execution);
                    Feed feed = parse(blob, version, entityTable, edsMetadata);
                    return new ODataEntitiesResponse(uri, feed, entityTable, edsMetadata);
                }
                // this is success with no-data
                return new ODataEntitiesResponse();
            }
        }
        // throw an error
        return new ODataEntitiesResponse(buildError(execution));
    }

    ODataVersion getODataVersion(BinaryWSProcedureExecution execution) {
        return getDataServiceVersion(getHeader(execution, ODataConstants.Headers.DATA_SERVICE_VERSION));
    }

    String getHeader(BinaryWSProcedureExecution execution, String header) {
        Object value = execution.getResponseHeader(header);
        if (value instanceof List) {
            return (String)((List)value).get(0);
        }
        return (String)value;
    }

    protected ODataEntitiesResponse executeWithComplexReturn(String method, String uri, String payload, String complexTypeName, EdmDataServices edsMetadata, String eTag, Status... expectedStatus) throws TranslatorException {
        Map<String, List<String>> headers = getDefaultHeaders();
        if (eTag != null) {
            headers.put("If-Match", Arrays.asList(eTag)); //$NON-NLS-1$
        }

        if (payload != null) {
            headers.put("Content-Type", Arrays.asList("application/atom+xml")); //$NON-NLS-1$ //$NON-NLS-2$
        }

        BinaryWSProcedureExecution execution = executeDirect(method, uri, payload, headers);
        for (Status status:expectedStatus) {
            if (status.getStatusCode() == execution.getResponseCode()) {
                if (execution.getResponseCode() != Status.NO_CONTENT.getStatusCode()) {
                    Blob blob = (Blob)execution.getOutputParameterValues().get(0);
                    //ODataVersion version = getDataServiceVersion((String)execution.getResponseHeader(ODataConstants.Headers.DATA_SERVICE_VERSION));

                    EdmComplexType complexType = edsMetadata.findEdmComplexType(complexTypeName);
                    if (complexType == null) {
                        throw new RuntimeException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17016, complexType));
                    }
                    try {
                        return parserComplex(StaxUtil.newXMLEventReader(new InputStreamReader(blob.getBinaryStream())), complexType, edsMetadata);
                    } catch (SQLException e) {
                        throw new TranslatorException(ODataPlugin.Event.TEIID17010, e, e.getMessage());
                    }
                }
                // this is success with no-data
                return new ODataEntitiesResponse();
            }
        }
        // throw an error
        return new ODataEntitiesResponse(buildError(execution));
    }

    private ODataEntitiesResponse parserComplex(XMLEventReader2 reader, EdmComplexType complexType, EdmDataServices edsMetadata) {
        XMLEvent2 event = reader.nextEvent();
        while (!event.isStartElement()) {
            event = reader.nextEvent();
        }
        return new ODataEntitiesResponse(AtomFeedFormatParser.parseProperties(reader, event.asStartElement(), edsMetadata, complexType).iterator());
    }

    protected TranslatorException buildError(BinaryWSProcedureExecution execution) {
        // do some error handling
        try {
            Blob blob = (Blob)execution.getOutputParameterValues().get(0);
            //FormatParser<OError> parser = FormatParserFactory.getParser(OError.class, FormatType.ATOM, null);
            FormatParser<OError> parser = new AtomErrorFormatParser();
            OError error = parser.parse(new InputStreamReader(blob.getBinaryStream(), Charset.forName("UTF-8"))); //$NON-NLS-1$
            return new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17013, execution.getResponseCode(), error.getCode(), error.getMessage(), error.getInnerError()));
        }
        catch (Throwable t) {
            return new TranslatorException(t);
        }
    }

    protected BinaryWSProcedureExecution executeDirect(String method, String uri, String payload, Map<String, List<String>> headers) throws TranslatorException {
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_ODATA, MessageLevel.DETAIL)) {
            try {
                LogManager.logDetail(LogConstants.CTX_ODATA, "Source-URL=", URLDecoder.decode(uri, "UTF-8")); //$NON-NLS-1$ //$NON-NLS-2$
            } catch (UnsupportedEncodingException e) {
            }
        }

        List<Argument> parameters = new ArrayList<Argument>();
        parameters.add(new Argument(Direction.IN, new Literal(method, TypeFacility.RUNTIME_TYPES.STRING), null));
        parameters.add(new Argument(Direction.IN, new Literal(payload, TypeFacility.RUNTIME_TYPES.STRING), null));
        parameters.add(new Argument(Direction.IN, new Literal(uri, TypeFacility.RUNTIME_TYPES.STRING), null));
        parameters.add(new Argument(Direction.IN, new Literal(true, TypeFacility.RUNTIME_TYPES.BOOLEAN), null));
        //the engine currently always associates out params at resolve time even if the values are not directly read by the call
        parameters.add(new Argument(Direction.OUT, TypeFacility.RUNTIME_TYPES.STRING, null));

        Call call = this.translator.getLanguageFactory().createCall(ODataExecutionFactory.INVOKE_HTTP, parameters, null);

        BinaryWSProcedureExecution execution = new BinaryWSProcedureExecution(call, this.metadata, this.executionContext, null, this.connection);
        execution.setUseResponseContext(true);
        execution.setCustomHeaders(headers);
        execution.execute();
        return execution;
    }

    protected Map<String, List<String>> getDefaultHeaders() {
        Map<String, List<String>> headers = new HashMap<String, List<String>>();
        headers.put("Accept", Arrays.asList(MediaType.APPLICATION_ATOM_XML, MediaType.APPLICATION_XML)); //$NON-NLS-1$
        headers.put("Content-Type", Arrays.asList(MediaType.APPLICATION_XML)); //$NON-NLS-1$
        return headers;
    }

    class ODataEntitiesResponse {
        private Feed feed;
        private String uri;
        private Iterator<Entry> rowIter;
        private String entityTypeName;
        private TranslatorException exception;
        private Status[] acceptedStatus;
        private Iterator<OProperty<?>> complexValues;
        private EdmDataServices edsMetadata;

        public ODataEntitiesResponse(String uri, Feed feed, String entityTypeName, EdmDataServices edsMetadata, Status... accptedStatus) {
            this.uri = uri;
            this.feed = feed;
            this.entityTypeName = entityTypeName;
            this.rowIter = this.feed.getEntries().iterator();
            this.acceptedStatus = accptedStatus;
            this.edsMetadata = edsMetadata;
        }

        public ODataEntitiesResponse(TranslatorException ex) {
            this.exception = ex;
        }

        public ODataEntitiesResponse() {
        }

        public ODataEntitiesResponse(Iterator<OProperty<?>> complexValues) {
            this.complexValues = complexValues;
        }

        public boolean hasRow() {
            return (this.rowIter != null && this.rowIter.hasNext());
        }

        public boolean hasError() {
            return this.exception != null;
        }

        public TranslatorException getError() {
            return this.exception;
        }

        public List<?> getNextRow(Column[] columns, Class<?>[] expectedType) throws TranslatorException {
            if (this.rowIter != null && this.rowIter.hasNext()) {
                OEntity entity = this.rowIter.next().getEntity();
                ArrayList results = new ArrayList();
                for (int i = 0; i < columns.length; i++) {
                    boolean isComplex = true;
                    String colName = columns[i].getProperty(ODataMetadataProcessor.COLUMN_GROUP, false);
                    if (colName == null) {
                        colName = columns[i].getName();
                        isComplex = false;
                    }
                    Object value = entity.getProperty(colName).getValue();
                    if (isComplex) {
                        List<OProperty<?>> embeddedProperties = (List<OProperty<?>>)value;
                        for (OProperty prop:embeddedProperties) {
                            if (prop.getName().equals(columns[i].getSourceName())) {
                                value = prop.getValue();
                                break;
                            }
                        }
                    }
                    results.add(BaseQueryExecution.this.translator.retrieveValue(value, expectedType[i]));
                }
                fetchNextBatch(!this.rowIter.hasNext(), this.edsMetadata);
                return results;
            }
            else if (this.complexValues != null) {
                HashMap<String, Object> values = new HashMap<String, Object>();
                while(this.complexValues.hasNext()) {
                    OProperty prop = this.complexValues.next();
                    values.put(prop.getName(), prop.getValue());
                }
                ArrayList results = new ArrayList();
                for (int i = 0; i < columns.length; i++) {
                    results.add(BaseQueryExecution.this.translator.retrieveValue(values.get(columns[i].getName()), expectedType[i]));
                }
                this.complexValues = null;
                return results;
            }
            return null;
        }

        // TODO:there is possibility here to async execute this feed
        private void fetchNextBatch(boolean fetch, EdmDataServices edsMetadata) throws TranslatorException {
            if (!fetch) {
                return;
            }

            String next = this.feed.getNext();
            if (next == null) {
                this.feed = null;
                this.rowIter = null;
                return;
            }

            int idx = next.indexOf("$skiptoken="); //$NON-NLS-1$
            if (idx != -1) {

                String skip = null;
                try {
                    skip = next.substring(idx + 11);
                    skip = URLDecoder.decode(skip, Charsets.Upper.UTF_8);
                } catch (UnsupportedEncodingException e) {
                    throw new TranslatorException(e);
                }

                String nextUri = this.uri;
                if (this.uri.indexOf('?') == -1) {
                    nextUri = this.uri + "?$skiptoken="+skip; //$NON-NLS-1$
                }
                else {
                    nextUri = this.uri + "&$skiptoken="+skip; //$NON-NLS-1$
                }
                BinaryWSProcedureExecution execution = executeDirect("GET", nextUri, null, getDefaultHeaders()); //$NON-NLS-1$
                validateResponse(execution);
                Blob blob = (Blob)execution.getOutputParameterValues().get(0);
                ODataVersion version = getODataVersion(execution);

                this.feed = parse(blob, version, this.entityTypeName, edsMetadata);
                this.rowIter = this.feed.getEntries().iterator();

            } else if (next.toLowerCase().startsWith("http")) { //$NON-NLS-1$
                BinaryWSProcedureExecution execution = executeDirect("GET", next, null, getDefaultHeaders()); //$NON-NLS-1$
                validateResponse(execution);
                Blob blob = (Blob)execution.getOutputParameterValues().get(0);
                ODataVersion version = getDataServiceVersion((String)execution.getResponseHeader(ODataConstants.Headers.DATA_SERVICE_VERSION));

                this.feed = parse(blob, version, this.entityTypeName, edsMetadata);
                this.rowIter = this.feed.getEntries().iterator();
            } else {
                throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17001, next));
            }
        }

        private void validateResponse(BinaryWSProcedureExecution execution) throws TranslatorException {
            for (Status expected:this.acceptedStatus) {
                if (execution.getResponseCode() != expected.getStatusCode()) {
                    throw buildError(execution);
                }
            }
        }

        Iterator<Entry> getResultsIter(){
            return this.rowIter;
        }
    }
}
