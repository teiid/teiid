/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
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

package org.teiid.query.processor.xquery;

import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.Streamable;
import org.teiid.core.types.XMLTranslator;
import org.teiid.core.types.XMLType;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.xml.XMLUtil;
import org.teiid.query.sql.lang.XQuery;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.util.CommandContext;
import org.teiid.query.util.XMLFormatConstants;
import org.teiid.query.xquery.XQueryExpression;


/**
 * XQuery execution Plan
 */
public class XQueryPlan extends ProcessorPlan {        
    private XQuery xQuery;
    private BufferManager bufferMgr;
    private String xmlFormat;
    private ProcessorDataManager dataManager;

    private int chunkSize = Streamable.STREAMING_BATCH_SIZE_IN_BYTES;

    /**
     * Constructor
     * @param xQuery fully-resolved XQuery command
     * @param xmlPlans Map of XQuery doc() args to XMLPlan for
     * that virtual doc
     */
    public XQueryPlan(XQuery xQuery) {
        super();
        this.xQuery = xQuery;
    }

    /**
     * @see java.lang.Object#clone()
     */
    public XQueryPlan clone() {
        XQuery clonedQuery = (XQuery)this.xQuery.clone();
        return new XQueryPlan(clonedQuery);
    }

    /**
     * @see org.teiid.query.processor.ProcessorPlan#initialize(org.teiid.query.util.CommandContext, org.teiid.query.processor.ProcessorDataManager, org.teiid.common.buffer.BufferManager)
     */
    public void initialize(CommandContext context, ProcessorDataManager dataMgr, BufferManager bufferMgr) {
        setContext(context);
        this.bufferMgr = bufferMgr;
        this.dataManager = dataMgr;
        if(context.getStreamingBatchSize() != 0){
            this.chunkSize = context.getStreamingBatchSize();
        }        
    }

    /**
     * @see org.teiid.query.processor.ProcessorPlan#getOutputElements()
     */
    public List getOutputElements() {
        ArrayList output = new ArrayList(1);
        ElementSymbol xml = new ElementSymbol("xml"); //$NON-NLS-1$
        xml.setType(DataTypeManager.DefaultDataClasses.XML);
        output.add(xml);
        return output;    
    }

    /**
     * @see org.teiid.query.processor.ProcessorPlan#open()
     */
    public void open() throws TeiidComponentException {
    }

    /**
     * @see org.teiid.query.processor.ProcessorPlan#nextBatch()
     */
    public TupleBatch nextBatch()
        throws BlockedException, TeiidComponentException, TeiidProcessingException {
        
    	XQueryExpression expr = this.xQuery.getCompiledXQuery();    
        expr.setXMLFormat(xmlFormat);
        
        SqlEval sqlEval = new SqlEval(this.dataManager, getContext(), this.xQuery.getProcedureGroup(), this.xQuery.getVariables());
        try {
        	XMLTranslator xml = expr.evaluateXQuery(sqlEval);
            TupleBatch batch = packResultsIntoBatch(xml);        
            return batch;
        } finally {
        	sqlEval.close();
        }
    }

    Properties getFormatProperties() {
        Properties props = new Properties();                
        if (XMLFormatConstants.XML_TREE_FORMAT.equals(this.xmlFormat)) {
            props.setProperty("indent", "yes");//$NON-NLS-1$//$NON-NLS-2$
        }
        return props;
    }
    
    /**
     * Each item in the raw results must be placed in a "record" List of
     * length 1, and each of those "records" must be placed in a 
     * "columns" list, which is then added to the batch. 
     * This ignores batch size because all the results are available in
     * memory "atomically" (from the XQueryEngine layer).
     * @param rawResults
     * @return
     * @throws TeiidProcessingException 
     */
    private TupleBatch packResultsIntoBatch(XMLTranslator translator) throws TeiidComponentException, TeiidProcessingException{
        List rows = new ArrayList(1);
        List row = new ArrayList(1);

        SQLXML srcXML = XMLUtil.saveToBufferManager(this.bufferMgr, translator, this.chunkSize);

        XMLType xml = new XMLType(srcXML);
        
        // now build the top batch with information from the saved one.
        row.add(xml);
        rows.add(row);        
        TupleBatch batch = new TupleBatch(1, rows);
        batch.setTerminationFlag(true);
        return batch;
    }

    /**
     * Clean up the tuple source when the plan is closed. 
     * @see org.teiid.query.processor.ProcessorPlan#close()
     */
    public void close() throws TeiidComponentException {
    }

    public String toString() {
        return "XQueryPlan: " + this.xQuery.toString();  //$NON-NLS-1$
    }
   
    /**
     * This method sets whether the documents should be returned in compact
     * format (no extraneous whitespace).  Non-compact format is more human-readable
     * (and bigger).  Additional formats may be possible in future.
     * @param xmlFormat A string giving the format in which xml results need to be returned
     */
    public void setXMLFormat(String xmlFormat) {
        this.xmlFormat = xmlFormat;
    }
}
