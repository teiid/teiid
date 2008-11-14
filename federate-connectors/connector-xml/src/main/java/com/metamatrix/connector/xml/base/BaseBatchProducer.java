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

package com.metamatrix.connector.xml.base;

import java.util.ArrayList;
import java.util.List;

import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.basic.BasicBatch;
import com.metamatrix.data.exception.ConnectorException;

public class BaseBatchProducer {

    public BaseBatchProducer() {
        super();
    }

    public static int currentReturnIndex = 0;
    
    public static int getCurrentReturnIndex() {
        return currentReturnIndex;
    }
    
    public static Batch createBatch(List allResultsList, int returnIndex,
            int maxBatch, ExecutionInfo info, ExecutionContext exeCtx,
            ConnectorEnvironment connectorEnv) throws ConnectorException {
        Batch batch = new BasicBatch();
        if(!allResultsList.isEmpty()) {
            ArrayList firstColumn = (ArrayList) allResultsList.get(0);
            while (returnIndex < firstColumn.size()
                    && batch.getRowCount() < maxBatch) {
                ArrayList row = new ArrayList();
                boolean addRowToCollector = true;
                for (int colNum = 0; colNum < allResultsList.size(); colNum++) {
                    Object resultObj;
                    ArrayList result;
                    Object valueObj;
                    resultObj = allResultsList.get(colNum);
                    result = (ArrayList) resultObj;
                    valueObj = result.get(returnIndex);
                    LargeOrSmallString value = (LargeOrSmallString) valueObj;
                    if (!(addRowToCollector = passesCriteriaCheck(info
                            .getCriteria(), value, colNum, exeCtx, connectorEnv))) {
                        break;
                    }
                    setColumnValue(colNum, value, row, info, exeCtx, connectorEnv);
                }
                if (addRowToCollector) {
                    batch.addRow(row);
                }
                returnIndex++;
            }
        }
        currentReturnIndex = returnIndex;
        return batch;
    }

    /**
     * Tests the value against the criteria to determine if the value should be
     * included in the result set.
     * 
     * @param criteriaPairs
     * @param value
     * @param colNum
     * @return
     * @throws ConnectorException
     */
    private static boolean passesCriteriaCheck(List criteriaPairs,
            LargeOrSmallString value, int colNum, ExecutionContext exeCtx,
            ConnectorEnvironment connectorEnv) throws ConnectorException {
        // Need to test this code
        for (int x = 0; x < criteriaPairs.size(); x++) {
            CriteriaDesc criteria = (CriteriaDesc) criteriaPairs.get(x);
            if (colNum == criteria.getColumnNumber()) {
                return ValueConvertor.evaluate(value, criteria, connectorEnv,
                        exeCtx);
            }
        }
        return true;
    }

    /**
     * Takes a value of an arbitrary type and inserts it into the result set
     * based upon the colNum and row values.
     * 
     * @param colNum
     * @param value
     * @param row
     * @throws ConnectorException
     */
    private static void setColumnValue(int colNum, LargeOrSmallString value,
            ArrayList row, ExecutionInfo info, ExecutionContext exeCtx,
            ConnectorEnvironment connectorEnv) throws ConnectorException {
        if (colNum < info.getColumnCount()) {
            // Convert String to appropriate data type
            OutputXPathDesc xpath = (OutputXPathDesc) info
                    .getRequestedColumns().get(colNum);
            Class dataValueType = xpath.getDataType();
            Object valueObj = ValueConvertor.convertLargeOrSmallString(value,
                    dataValueType, connectorEnv, exeCtx);
            row.add(valueObj);
        }
    }
}
