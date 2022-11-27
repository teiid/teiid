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

package org.teiid.client;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OptionalDataException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.teiid.client.metadata.ParameterInfo;
import org.teiid.client.plan.Annotation;
import org.teiid.client.plan.PlanNode;
import org.teiid.client.util.ExceptionHolder;
import org.teiid.core.TeiidException;
import org.teiid.core.util.ExternalizeUtil;
import org.teiid.core.util.MultiArrayOutputStream;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.netty.handler.codec.serialization.CompactObjectInputStream;
import org.teiid.netty.handler.codec.serialization.CompactObjectOutputStream;


/**
 * Results Message, used by MMStatement to get the query results.
 */
public class ResultsMessage implements Externalizable {

    static final long serialVersionUID = 3546924172976187793L;

    private List<? extends List<?>> results;
    private String[] columnNames;
    private String[] dataTypes;

    /** A description of planning that occurred as requested in the request. */
    private PlanNode planDescription;

    /** An exception that occurred. */
    private TeiidException exception;

    /** Warning could be schema validation errors or partial results warnings */
    private List<Throwable> warnings;

    /** First row index */
    private int firstRow = 0;

    /** Last row index */
    private int lastRow;

    /** Final row index in complete result set, if known */
    private int finalRow = -1;

    /** The parameters of a Stored Procedure */
    private List<ParameterInfo> parameters;

    /** OPTION DEBUG log if OPTION DEBUG was used */
    private String debugLog;

    private byte clientSerializationVersion;

    /**
     * Query plan annotations, if OPTION SHOWPLAN or OPTION PLANONLY was used:
     * Collection of Object[] where each Object[] holds annotation information
     * that can be used to create an Annotation implementation in JDBC.
     */
    private Collection<Annotation> annotations;

    private boolean isUpdateResult;
    private int updateCount = -1;

    private boolean delayDeserialization;
    byte[] resultBytes;

    private MultiArrayOutputStream serializationBuffer;

    public ResultsMessage(){
    }

    public ResultsMessage(List<? extends List<?>> results, String[] columnNames, String[] dataTypes){
        this.results = results;
        setFirstRow( 1 );
        setLastRow( results.size() );

        this.columnNames = columnNames;
        this.dataTypes = dataTypes;
    }

    public List<? extends List<?>> getResultsList() {
        return results;
    }

    public void processResults() throws TeiidSQLException {
        if (results == null && resultBytes != null) {
            try {
                CompactObjectInputStream ois = new CompactObjectInputStream(new ByteArrayInputStream(resultBytes), ResultsMessage.class.getClassLoader());
                results = BatchSerializer.readBatch(ois, dataTypes);
            } catch (IOException e) {
                throw TeiidSQLException.create(e);
            } catch (ClassNotFoundException e) {
                throw TeiidSQLException.create(e);
            } finally {
                resultBytes = null;
            }
        }
    }

    public void setResults(List<?>[] results) {
        this.results = Arrays.asList(results);
    }

    public void setResults(List<? extends List<?>> results) {
        this.results = results;
    }

    public  String[] getColumnNames() {
        return this.columnNames;
    }

    public String[] getDataTypes() {
        return this.dataTypes;
    }

    /**
     * @return
     */
    public TeiidException getException() {
        return exception;
    }

    /**
     * @return
     */
    public int getFinalRow() {
        return finalRow;
    }

    /**
     * @return
     */
    public int getFirstRow() {
        return firstRow;
    }

    /**
     * @return
     */
    public int getLastRow() {
        return lastRow;
    }

    /**
     * @return
     */
    public PlanNode getPlanDescription() {
        return planDescription;
    }

    /**
     * @return
     */
    public List<Throwable> getWarnings() {
        return warnings;
    }

    public void setException(Throwable e) {
        if(e instanceof TeiidException) {
            this.exception = (TeiidException)e;
        } else {
            this.exception = new TeiidException(e, e.getMessage());
        }
    }

    /**
     * @param i
     */
    public void setFinalRow(int i) {
        finalRow = i;
    }

    /**
     * @param i
     */
    public void setFirstRow(int i) {
        firstRow = i;
    }

    /**
     * @param i
     */
    public void setLastRow(int i) {
        lastRow = i;
    }

    /**
     * @param object
     */
    public void setPlanDescription(PlanNode object) {
        planDescription = object;
    }

    /**
     * @param list
     */
    public void setWarnings(List<Throwable> list) {
        warnings = list;
    }

    /**
     * @return
     */
    public List getParameters() {
        return parameters;
    }

    /**
     * @param list
     */
    public void setParameters(List list) {
        parameters = list;
    }

    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

    public void setDataTypes(String[] dataTypes) {
        this.dataTypes = dataTypes;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        columnNames = ExternalizeUtil.readStringArray(in);
        dataTypes = ExternalizeUtil.readStringArray(in);

        // Row data
        results = BatchSerializer.readBatch(in, dataTypes);

        // Plan Descriptions
        planDescription = (PlanNode)in.readObject();

        ExceptionHolder holder = (ExceptionHolder)in.readObject();
        if (holder != null) {
            this.exception = (TeiidException)holder.getException();
        }

        //delayed deserialization
        if (results == null && this.exception == null) {
            int length = in.readInt();
            resultBytes = new byte[length];
            in.readFully(resultBytes);
        }

        List<ExceptionHolder> holderList = (List<ExceptionHolder>)in.readObject();
        if (holderList != null) {
            this.warnings = ExceptionHolder.toThrowables(holderList);
        }

        firstRow = in.readInt();
        lastRow = in.readInt();
        finalRow = in.readInt();

        //Parameters
        parameters = ExternalizeUtil.readList(in, ParameterInfo.class);

        debugLog = (String)in.readObject();
        annotations = ExternalizeUtil.readList(in, Annotation.class);
        isUpdateResult = in.readBoolean();
        if (isUpdateResult) {
            try {
                updateCount = in.readInt();
            } catch (OptionalDataException e) {
            } catch (EOFException e) {
            }
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        ExternalizeUtil.writeArray(out, columnNames);
        ExternalizeUtil.writeArray(out, dataTypes);

        // Results data
        if (delayDeserialization) {
            BatchSerializer.writeBatch(out, dataTypes, null, clientSerializationVersion);
        } else {
            BatchSerializer.writeBatch(out, dataTypes, results, clientSerializationVersion);
        }

        // Plan descriptions
        out.writeObject(this.planDescription);

        if (exception != null) {
            out.writeObject(new ExceptionHolder(exception));
        } else {
            out.writeObject(exception);
        }

        if (delayDeserialization && results != null) {
            serialize(true);
            out.writeInt(serializationBuffer.getCount());
            serializationBuffer.writeTo(out);
            serializationBuffer = null;
        }

        if (this.warnings != null) {
            out.writeObject(ExceptionHolder.toExceptionHolders(this.warnings));
        } else {
            out.writeObject(this.warnings);
        }

        out.writeInt(firstRow);
        out.writeInt(lastRow);
        out.writeInt(finalRow);

        // Parameters
        ExternalizeUtil.writeList(out, parameters);

        out.writeObject(debugLog);
        ExternalizeUtil.writeCollection(out, annotations);
        out.writeBoolean(isUpdateResult);
        if (isUpdateResult) {
            out.writeInt(updateCount);
        }
    }

    /**
     * Serialize the result data
     * @return the size of the data bytes
     * @throws IOException
     */
    public int serialize(boolean keepSerialization) throws IOException {
        if (serializationBuffer == null) {
            serializationBuffer = new MultiArrayOutputStream(1 << 13);
            CompactObjectOutputStream oos = new CompactObjectOutputStream(serializationBuffer);
            BatchSerializer.writeBatch(oos, dataTypes, results, clientSerializationVersion);
            oos.close();
        }
        int result = serializationBuffer.getCount();
        if (!keepSerialization) {
            serializationBuffer = null;
        }
        return result;
    }

    /**
     * @return
     */
    public Collection<Annotation> getAnnotations() {
        return annotations;
    }

    /**
     * @return
     */
    public String getDebugLog() {
        return debugLog;
    }

    /**
     * @param collection
     */
    public void setAnnotations(Collection<Annotation> collection) {
        annotations = collection;
    }

    /**
     * @param string
     */
    public void setDebugLog(String string) {
        debugLog = string;
    }


    /*
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return new StringBuffer("ResultsMessage rowCount=") //$NON-NLS-1$
            .append(results == null ? 0 : results.size())
            .append(" finalRow=") //$NON-NLS-1$
            .append(finalRow)
            .toString();
    }

    public void setUpdateResult(boolean isUpdateResult) {
        this.isUpdateResult = isUpdateResult;
    }

    public boolean isUpdateResult() {
        return isUpdateResult;
    }

    public byte getClientSerializationVersion() {
        return clientSerializationVersion;
    }

    public void setClientSerializationVersion(byte clientSerializationVersion) {
        this.clientSerializationVersion = clientSerializationVersion;
    }

    public void setUpdateCount(int updateCount) {
        this.updateCount = updateCount;
    }

    public int getUpdateCount() {
        return updateCount;
    }

    public void setDelayDeserialization(boolean delayDeserialization) {
        this.delayDeserialization = delayDeserialization;
    }
}

