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

import java.io.Serializable;
import java.sql.ResultSet;

import com.metamatrix.dqp.client.RequestInfo;


/** 
 * @since 4.2
 */
public class ServerRequest implements RequestInfo {
    
    //private static final String HEADER = "ServerRequest:";
    
    private String sql;
    private int requestType = RequestInfo.REQUEST_TYPE_STATEMENT;
    private Object[] bindParameters;
    private int cursorType = ResultSet.TYPE_FORWARD_ONLY;
    private int fetchSize;
    private boolean partialResults;
    private boolean xmlValidationMode;
    private String xmlFormat;
    private String xmlStyleSheet;
    private int transactionAutoWrapMode = RequestInfo.AUTOWRAP_OFF;
    private boolean useResultSetCache;
    private Serializable commandPayload;
    
    String getSql() {
        return sql;
    }
    /** 
     * @see com.metamatrix.dqp.client.RequestInfo#setSql(java.lang.String)
     * @since 4.2
     */
    public void setSql(String sql) {
        this.sql = sql;
    }
    
    int getRequestType() {
        return requestType;
    }

    /** 
     * @see com.metamatrix.dqp.client.RequestInfo#setRequestType(int)
     * @since 4.2
     */
    public void setRequestType(int type) {
        this.requestType = type;
    }

    Object[] getBindParameters() {
        return bindParameters;
    }
    /** 
     * @see com.metamatrix.dqp.client.RequestInfo#setBindParameters(java.lang.Object[])
     * @since 4.2
     */
    public void setBindParameters(Object[] params) {
        this.bindParameters = params;
    }

    int getCursorType() {
        return cursorType;
    }
    /** 
     * @see com.metamatrix.dqp.client.RequestInfo#setCursorType(int)
     * @since 4.2
     */
    public void setCursorType(int type) {
        this.cursorType = type;
    }

    int getFetchSize() {
        return fetchSize;
    }
    /** 
     * @see com.metamatrix.dqp.client.RequestInfo#setFetchSize(int)
     * @since 4.2
     */
    public void setFetchSize(int size) {
        this.fetchSize = size;
    }
    
    boolean getPartialResults() {
        return partialResults;
    }

    /** 
     * @see com.metamatrix.dqp.client.RequestInfo#setPartialResults(boolean)
     * @since 4.2
     */
    public void setPartialResults(boolean flag) {
        this.partialResults = flag;
    }
    
    boolean getXMLValidationMode() {
        return xmlValidationMode;
    }

    /** 
     * @see com.metamatrix.dqp.client.RequestInfo#setXMLValidationMode(boolean)
     * @since 4.2
     */
    public void setXMLValidationMode(boolean flag) {
        this.xmlValidationMode = flag;
    }
    
    String getXMLFormat() {
        return xmlFormat;
    }

    /** 
     * @see com.metamatrix.dqp.client.RequestInfo#setXMLFormat(java.lang.String)
     * @since 4.2
     */
    public void setXMLFormat(String format) {
        this.xmlFormat = format;
    }
    
    String getXMLStyleSheet() {
        return xmlStyleSheet;
    }

    /** 
     * @see com.metamatrix.dqp.client.RequestInfo#setXMLStyleSheet(java.lang.String)
     * @since 4.2
     */
    public void setXMLStyleSheet(String styleSheet) {
        this.xmlStyleSheet = styleSheet;
    }

    int getTransactionAutoWrapMode() {
        return transactionAutoWrapMode;
    }
    /** 
     * @see com.metamatrix.dqp.client.RequestInfo#setTransactionAutoWrapMode(int)
     * @since 4.2
     */
    public void setTransactionAutoWrapMode(int autoWrapMode) {
        this.transactionAutoWrapMode = autoWrapMode;
    }
    
    boolean getUseResultSetCache() {
        return useResultSetCache;
    }

    /** 
     * @see com.metamatrix.dqp.client.RequestInfo#setUseResultSetCache(boolean)
     * @since 4.2
     */
    public void setUseResultSetCache(boolean flag) {
        this.useResultSetCache = flag;
    }
    
    Serializable getCommandPayload() {
        return commandPayload;
    }

    /** 
     * @see com.metamatrix.dqp.client.RequestInfo#setCommandPayload(java.io.Serializable)
     * @since 4.2
     */
    public void setCommandPayload(Serializable payload) {
        this.commandPayload = payload;
    }
    
//    String getPortableString() {
//        StringBuffer buf = new StringBuffer("ServerRequest:")
//        .append("sql").append(PortableStringUtil.EQUALS).append(PortableStringUtil.escapeString(sql)).append(PortableStringUtil.PROP_SEPARATOR)
//        .append("requestType").append(PortableStringUtil.EQUALS).append(PortableStringUtil.escapeString(Integer.toString(requestType))).append(PortableStringUtil.PROP_SEPARATOR)
//        .append("cursorType").append(PortableStringUtil.EQUALS).append(PortableStringUtil.escapeString(Integer.toString(cursorType))).append(PortableStringUtil.PROP_SEPARATOR)
//        .append("fetchSize").append(PortableStringUtil.EQUALS).append(PortableStringUtil.escapeString(Integer.toString(fetchSize))).append(PortableStringUtil.PROP_SEPARATOR)
//        .append("partialResults").append(PortableStringUtil.EQUALS).append(PortableStringUtil.escapeString(Boolean.toString(partialResults))).append(PortableStringUtil.PROP_SEPARATOR)
//        .append("xmlValidationMode").append(PortableStringUtil.EQUALS).append(PortableStringUtil.escapeString(Boolean.toString(xmlValidationMode))).append(PortableStringUtil.PROP_SEPARATOR)
//        .append("transactionAutoWrapMode").append(PortableStringUtil.EQUALS).append(PortableStringUtil.escapeString(Integer.toString(transactionAutoWrapMode))).append(PortableStringUtil.PROP_SEPARATOR)
//        .append("useResultSetCache").append(PortableStringUtil.EQUALS).append(PortableStringUtil.escapeString(Boolean.toString(useResultSetCache)));
//        if (xmlFormat != null) {
//            buf.append(PortableStringUtil.PROP_SEPARATOR).append("xmlFormat").append(PortableStringUtil.EQUALS).append(PortableStringUtil.escapeString(xmlFormat));
//        }
//        if (xmlStyleSheet != null) {
//            buf.append(PortableStringUtil.PROP_SEPARATOR).append("xmlStyleSheet").append(PortableStringUtil.EQUALS).append(PortableStringUtil.escapeString(xmlStyleSheet));
//        }
//        if (commandPayload != null) {
//            buf.append(PortableStringUtil.PROP_SEPARATOR).append("commandPayload").append(PortableStringUtil.EQUALS).append(PortableStringUtil.encode(commandPayload));
//        }
//        if (bindParameters != null) {
//            buf.append(PortableStringUtil.PROP_SEPARATOR).append("bindParameters").append(PortableStringUtil.EQUALS).append(PortableStringUtil.encode(bindParameters));
//        }
//        return buf.toString();
//    }
//    
//    static ServerRequest createServerRequestFromPortableString(String portableString) throws MetaMatrixProcessingException {
//        String[] parts = PortableStringUtil.getParts(portableString, PortableStringUtil.PROP_SEPARATOR);
//        if (parts == null || parts.length < 8 || parts.length > 12 || !parts[0].startsWith(HEADER)) {
//            throw new MetaMatrixProcessingException("");
//        }
//        parts[0] = parts[0].substring(HEADER.length());
//        
//        ServerRequest request = new ServerRequest();
//        request.sql = PortableStringUtil.unescapeString(PortableStringUtil.getParts(parts[0], PortableStringUtil.EQUALS)[1]);
//        request.requestType = Integer.parseInt(PortableStringUtil.unescapeString(PortableStringUtil.getParts(parts[1], PortableStringUtil.EQUALS)[1]));
//        request.cursorType = Integer.parseInt(PortableStringUtil.unescapeString(PortableStringUtil.getParts(parts[2], PortableStringUtil.EQUALS)[1]));
//        request.fetchSize = Integer.parseInt(PortableStringUtil.unescapeString(PortableStringUtil.getParts(parts[3], PortableStringUtil.EQUALS)[1]));
//        request.partialResults = Boolean.valueOf(PortableStringUtil.unescapeString(PortableStringUtil.getParts(parts[4], PortableStringUtil.EQUALS)[1])).booleanValue();
//        request.xmlValidationMode = Boolean.valueOf(PortableStringUtil.unescapeString(PortableStringUtil.getParts(parts[5], PortableStringUtil.EQUALS)[1])).booleanValue();
//        request.transactionAutoWrapMode = Integer.parseInt(PortableStringUtil.unescapeString(PortableStringUtil.getParts(parts[6], PortableStringUtil.EQUALS)[1]));
//        request.useResultSetCache = Boolean.valueOf(PortableStringUtil.unescapeString(PortableStringUtil.getParts(parts[7], PortableStringUtil.EQUALS)[1])).booleanValue();
//        
//        for (int i = 8; i < parts.length; i++) {
//            String[] propValPair = PortableStringUtil.getParts(parts[i], PortableStringUtil.EQUALS);
//            if (propValPair[0].equals("xmlFormat")) {
//                request.xmlFormat = PortableStringUtil.unescapeString(propValPair[1]);
//            } else if (propValPair[0].equals("xmlStyleSheet")) {
//                request.xmlStyleSheet = PortableStringUtil.unescapeString(propValPair[1]);
//            } else if (propValPair[0].equals("commandPayload")) {
//                request.commandPayload = (Serializable)PortableStringUtil.decode(propValPair[1]);
//            } else if (propValPair[0].equals("bindParameters")) {
//                request.bindParameters = (Object[])PortableStringUtil.decode(propValPair[1]);
//            }
//        }
//        
//        return request;
//    }

}
