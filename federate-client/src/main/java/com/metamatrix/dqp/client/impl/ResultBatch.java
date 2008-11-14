/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.dqp.client.impl;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.lob.LobChunkInputStream;
import com.metamatrix.common.types.BlobType;
import com.metamatrix.common.types.ClobType;
import com.metamatrix.common.types.Streamable;
import com.metamatrix.common.types.XMLType;
import com.metamatrix.dqp.client.Results;
import com.metamatrix.dqp.message.ParameterInfo;
import com.metamatrix.dqp.message.ResultsMessage;


/** 
 * @since 4.2
 */
class ResultBatch implements Results {
      
    private long requestID;
    private ConnectionHolder holder;    
    private boolean isLast;
    private int beginRow;
    private int endRow;
    private int rowCount;
    private int columnCount;
    private List[] results;
    private ParameterInfo[] parameterInfo;
    private Map outputParameterMap = new HashMap();
    
    private boolean update;
    private List warnings;
    
    ResultBatch(ResultsMessage results, boolean isUpdate, long requestID, ConnectionHolder holder) {
        this.requestID = requestID;
        this.holder = holder;        
        this.beginRow = results.getFirstRow();
        this.endRow = results.getLastRow();
        this.results = results.getResults();
        this.rowCount = (this.results == null) ? 0 : this.results.length;
        this.isLast = results.getFinalRow() == this.endRow;
        this.columnCount = (results.getColumnNames() == null) ? 0 : results.getColumnNames().length;
        this.warnings = (results.getWarnings() == null) ? Collections.EMPTY_LIST : results.getWarnings();
        this.update = isUpdate;
        
        setParameters(results.getParameters());
    }
    
    /** 
     * @see com.metamatrix.dqp.client.Results#isLast()
     * @since 4.2
     */
    public boolean isLast() {
        return isLast;
    }

    /** 
     * @see com.metamatrix.dqp.client.Results#getBeginRow()
     * @since 4.2
     */
    public int getBeginRow() {
        return beginRow;
    }

    /** 
     * @see com.metamatrix.dqp.client.Results#getEndRow()
     * @since 4.2
     */
    public int getEndRow() {
        return endRow;
    }

    /** 
     * @see com.metamatrix.dqp.client.Results#getRowCount()
     * @since 4.2
     */
    public int getRowCount() {
        return rowCount;
    }

    /** 
     * @see com.metamatrix.dqp.client.Results#getColumnCount()
     * @since 4.2
     */
    public int getColumnCount() {
        return columnCount;
    }

    /** 
     * @see com.metamatrix.dqp.client.Results#getValue(int, int)
     * @since 4.2
     */
    public Object getValue(int row, int column) throws MetaMatrixComponentException, MetaMatrixProcessingException {
        if (rowCount == 0) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ResultBatch.no_rows")); //$NON-NLS-1$
        } else if (row < beginRow || row > endRow) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ResultBatch.invalid_row", row)); //$NON-NLS-1$
        } else if (column < 1 || column > columnCount) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ResultBatch.invalid_column", column)); //$NON-NLS-1$
        }
        
        // check for the lob types
        Object val = getData(row, column);
        try {
            if (val instanceof XMLType) {                
            	LobChunkInputStream reader = new LobChunkInputStream(new StreamingLobChunckProducer(holder.getDqp(), requestID, (Streamable)val));
                return new String(reader.getByteContents(), Charset.forName("UTF-16")); //$NON-NLS-1$
            }
            else if (val instanceof ClobType) {
            	LobChunkInputStream reader = new LobChunkInputStream(new StreamingLobChunckProducer(holder.getDqp(), requestID, (Streamable)val));
                return new String(reader.getByteContents(), Charset.forName("UTF-16")); //$NON-NLS-1$
            }
            else if (val != null && val instanceof BlobType) {
                LobChunkInputStream stream = new LobChunkInputStream(new StreamingLobChunckProducer(holder.getDqp(), requestID, (Streamable)val));
                return stream.getByteContents();                
            }
            else {
                return val;
            }
        } catch (IOException e) {
            throw new MetaMatrixComponentException(e);
        }
    }
    
    private Object getData(int row, int column) {
        return results[row-beginRow].get(column-1);
    }
    
    /** 
     * @see com.metamatrix.dqp.client.Results#getParameterCount()
     * @since 4.3
     */
    public int getParameterCount() {
        return parameterInfo.length;
    }

    /** 
     * @see com.metamatrix.dqp.client.Results#getParameterType(int)
     * @since 4.3
     */
    public int getParameterType(int index) throws MetaMatrixComponentException, MetaMatrixProcessingException {
        validateParameterIndex(index);
        int type = parameterInfo[index-1].getType();
        switch(type) {
            case ParameterInfo.IN: return PARAMETER_TYPE_IN;
            case ParameterInfo.OUT: return PARAMETER_TYPE_OUT;
            case ParameterInfo.INOUT: return PARAMETER_TYPE_INOUT;
            case ParameterInfo.RETURN_VALUE: return PARAMETER_TYPE_RETURN;
            default: throw new MetaMatrixComponentException(AdminPlugin.Util.getString("ResultBatch.invalid_paramtype", type)); //$NON-NLS-1$
        }
    }

    /** 
     * @see com.metamatrix.dqp.client.Results#getOutputParameter(int)
     * @since 4.2
     */
    public Object getOutputParameter(int index) throws MetaMatrixComponentException, MetaMatrixProcessingException {
        if(!isLast) {
            return new MetaMatrixProcessingException(AdminPlugin.Util.getString("ResultBatch.not_last_batch")); //$NON-NLS-1$
        }
        validateParameterIndex(index);
        Integer actualIndex = (Integer)outputParameterMap.get(new Integer(index));
        if (actualIndex == null) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ResultBatch.not_out_param", index)); //$NON-NLS-1$
        }
        int endOfRows = 0;
        int endOfColumns = 0;
        if (rowCount > 0) { // If the endRow value is valid
            endOfRows = endRow;
            endOfColumns = columnCount;
        }
        return getData(endOfRows+actualIndex.intValue(), endOfColumns+actualIndex.intValue());
    }
    
    private void validateParameterIndex(int index) throws MetaMatrixProcessingException {
        if (parameterInfo.length == 0) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ResultBatch.no_params")); //$NON-NLS-1$
        } else if (index < 1 || index > parameterInfo.length) {
            throw new MetaMatrixProcessingException(AdminPlugin.Util.getString("ResultBatch.invalid_paramindex", index)); //$NON-NLS-1$
        }
    }
    
    public boolean isUpdate() {
        return update;
    }

    /** 
     * @see com.metamatrix.dqp.client.Results#getUpdateCount()
     * @since 4.2
     */
    public int getUpdateCount() throws MetaMatrixComponentException {
        if (!update) {
            throw new MetaMatrixComponentException(AdminPlugin.Util.getString("ResultBatch.not_update")); //$NON-NLS-1$
        } else if (results == null || results.length != 1) {
            throw new MetaMatrixComponentException(AdminPlugin.Util.getString("ResultBatch.update_count_unavailable")); //$NON-NLS-1$
        }
        Integer val = (Integer)getData(1,1);
        if (val == null) {
            return 0;
        }
        return val.intValue();
    }

    /** 
     * @see com.metamatrix.dqp.client.Results#getWarnings()
     * @since 4.2
     */
    public Exception[] getWarnings() {
        return (Exception[])warnings.toArray(new Exception[warnings.size()]);
    }
    
    private void setParameters(List parameters) {
        if (parameters == null || parameters.isEmpty()) {
            this.parameterInfo = new ParameterInfo[0];
        } else {
            int outputParameterCount = 0;
            int nonResultSetParameterCount = 0;
            ArrayList paramInfos = new ArrayList(parameters.size());
            for (int i = 0; i < parameters.size(); i++) {
                ParameterInfo info = (ParameterInfo)parameters.get(i);
                int parameterType = info.getType();
                if (parameterType != ParameterInfo.RESULT_SET) {
                    paramInfos.add(info);
                    nonResultSetParameterCount++;
                }
                if (parameterType == ParameterInfo.RETURN_VALUE ||
                    parameterType == ParameterInfo.OUT ||
                    parameterType == ParameterInfo.INOUT) {
                    outputParameterCount++;
                    outputParameterMap.put(new Integer(nonResultSetParameterCount), new Integer(outputParameterCount));
                }
            }
            this.parameterInfo = (ParameterInfo[])paramInfos.toArray(new ParameterInfo[paramInfos.size()]);
        }
        int numOutputParameters = outputParameterMap.size();
        if (numOutputParameters > 0) {
            this.endRow      -= numOutputParameters;
            this.rowCount    -= numOutputParameters;
            this.columnCount -= numOutputParameters;
        }
    }
}
