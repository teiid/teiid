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

package com.metamatrix.query.processor.xquery;

import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.buffer.BufferManager.TupleSourceStatus;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.Streamable;
import com.metamatrix.common.types.XMLType;
import com.metamatrix.query.processor.BaseProcessorPlan;
import com.metamatrix.query.processor.DescribableUtil;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.dynamic.SqlEval;
import com.metamatrix.query.processor.xml.XMLUtil;
import com.metamatrix.query.sql.lang.XQuery;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.XMLFormatConstants;
import com.metamatrix.query.xquery.XQueryExpression;

/**
 * XQuery execution Plan
 */
public class XQueryPlan extends BaseProcessorPlan {        
    private XQuery xQuery;
    private BufferManager bufferMgr;
    private String xmlFormat;
    private String parentGroup;

    private int chunkSize = Streamable.STREAMING_BATCH_SIZE_IN_BYTES;
    private TupleSourceID resultsTupleSourceId;

    /**
     * Constructor
     * @param xQuery fully-resolved XQuery command
     * @param xmlPlans Map of XQuery doc() args to XMLPlan for
     * that virtual doc
     */
    public XQueryPlan(XQuery xQuery, String parentGroup) {
        super();
        this.xQuery = xQuery;
        this.parentGroup = parentGroup;
    }

    /**
     * @see java.lang.Object#clone()
     */
    public Object clone() {
        XQuery clonedQuery = (XQuery)this.xQuery.clone();
        return new XQueryPlan(clonedQuery, parentGroup);
    }

    /**
     * @see com.metamatrix.query.processor.ProcessorPlan#initialize(com.metamatrix.query.util.CommandContext, com.metamatrix.query.processor.ProcessorDataManager, com.metamatrix.common.buffer.BufferManager)
     */
    public void initialize(CommandContext context, ProcessorDataManager dataMgr, BufferManager bufferMgr) {
        setContext(context);
        this.bufferMgr = bufferMgr;        
        if(context.getStreamingBatchSize() != 0){
            this.chunkSize = context.getStreamingBatchSize();
        }        
    }

    /**
     * @see com.metamatrix.query.processor.ProcessorPlan#getOutputElements()
     */
    public List getOutputElements() {
        ArrayList output = new ArrayList(1);
        ElementSymbol xml = new ElementSymbol("xml"); //$NON-NLS-1$
        xml.setType(DataTypeManager.DefaultDataClasses.XML);
        output.add(xml);
        return output;    
    }

    /**
     * @see com.metamatrix.query.processor.ProcessorPlan#open()
     */
    public void open() throws MetaMatrixComponentException {
        // no plans to open, processing is done in Xquery engine
        if (this.resultsTupleSourceId == null) {
            this.resultsTupleSourceId = XMLUtil.createXMLTupleSource(bufferMgr, getContext().getConnectionID());
        }                
    }

    /**
     * @see com.metamatrix.query.processor.ProcessorPlan#nextBatch()
     */
    public TupleBatch nextBatch()
        throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {
        
    	XQueryExpression expr = this.xQuery.getCompiledXQuery();    
        expr.setXMLFormat(xmlFormat);
        
        SqlEval sqlEval = new SqlEval(bufferMgr, getContext(), this.parentGroup);
        try {
        	SQLXML xml = expr.evaluateXQuery(sqlEval);
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
     */
    private TupleBatch packResultsIntoBatch(SQLXML srcXML) throws MetaMatrixComponentException{
        try {
            List rows = new ArrayList(1);
            List row = new ArrayList(1);

            // this may be little confusing, but the top layer is not immediately going
            // to disk for saving; when it does it only saves the streaming id, not the
            // contents. The below one saves immediately to disk, and when client refers to top
            // id, processor know about the *saved* and gets the contents from it.
            
            // the one which saves to disk
            TupleSourceID savedId = XMLUtil.saveToBufferManager(this.bufferMgr, this.resultsTupleSourceId.getStringID(), srcXML, this.chunkSize);
            
            // here we have 2 options; create xml from original source or from buffer
            // manager. since buffer manager is slow we will choose the first option.
            // incase this xml used in processor it will be faster; if it used in the
            // client using steaming will be slow.
            XMLType xml = new XMLType(srcXML);
            xml.setPersistenceStreamId(savedId.getStringID());
            //XMLValue xml = XMLUtil.getFromBufferManager(this.bufferMgr, savedId, this.chunkSize, getFormatProperties());
            
            // now build the top batch with information from the saved one.
            row.add(xml);
            rows.add(row);        
            TupleBatch batch = new TupleBatch(1, rows);
            batch.setTerminationFlag(true);
            
            // add the top to buffer and return the batch.
            this.bufferMgr.addTupleBatch(this.resultsTupleSourceId, batch);
            this.bufferMgr.setStatus(this.resultsTupleSourceId, TupleSourceStatus.FULL);            
            return batch;
        } catch (TupleSourceNotFoundException e) {
            throw new MetaMatrixComponentException(e);
        }
    }

    /**
     * Clean up the tuple source when the plan is closed. 
     * @see com.metamatrix.query.processor.ProcessorPlan#close()
     */
    public void close() throws MetaMatrixComponentException {
        if (this.resultsTupleSourceId != null) {
            try {
                this.bufferMgr.removeTupleSource(this.resultsTupleSourceId);
            } catch (TupleSourceNotFoundException e) {
                // ignore and go on, may be removed already.
            }
            this.resultsTupleSourceId = null;
        }
    }

    /**
     * @see com.metamatrix.query.processor.Describable#getDescriptionProperties()
     */
    public Map getDescriptionProperties() {
        Map props = new HashMap();
        props.put(PROP_TYPE, "XQuery Plan"); //$NON-NLS-1$
        List children = new ArrayList();
        props.put(PROP_CHILDREN, children);
        props.put(PROP_OUTPUT_COLS, DescribableUtil.getOutputColumnProperties(getOutputElements()));
        return props;
    }

    public String toString() {
        return "XQueryPlan: " + this.xQuery.toString();  //$NON-NLS-1$
    }
   
    /** 
     * @see com.metamatrix.query.processor.ProcessorPlan#getChildPlans()
     * @since 4.2
     */
    public Collection getChildPlans() {
        //return xmlPlans.values();
        return Collections.EMPTY_LIST;
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
