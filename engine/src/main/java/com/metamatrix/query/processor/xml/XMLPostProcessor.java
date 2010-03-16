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

package com.metamatrix.query.processor.xml;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.types.Streamable;
import com.metamatrix.common.types.XMLTranslator;
import com.metamatrix.common.types.XMLType;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.XMLFormatConstants;

/**
 * TODO: sometime later this functionality should be handled with system functions
 */
public class XMLPostProcessor extends ProcessorPlan {
	
    // Post-processing
	private String styleSheet;
	private String xmlFormat;
	private BufferManager bufferMgr;
	private ProcessorPlan plan;
	
	public XMLPostProcessor(ProcessorPlan plan) {
		this.plan = plan;
	}
    
	XMLType postProcessDocument(XMLType xmlDoc) throws MetaMatrixComponentException {
        try {        
        	return transformXML(xmlDoc);
        } catch (SQLException e) {
            throw new MetaMatrixComponentException(e);
        }
    }

	/**
	 * <p> This method is used to transform the XML results obtained, by applying transformations
	 * using any style sheets that are added to the <code>Query</code> object.
	 * @param xmlResults The xml result string that is being transformed using stylesheets
	 * @return The xml string transformed using style sheets.
	 * @throws MetaMatrixComponentException if there is an error trying to perform transformation
	 */
	private XMLType transformXML(XMLType xmlResults) throws SQLException, MetaMatrixComponentException {
		final Source styleSource;
		if (styleSheet != null) {
			Reader styleReader = new StringReader(styleSheet);
			styleSource = new StreamSource(styleReader);
		} else {
			styleSource = null;
		}

		// get a reader object for the xml results
		//Reader xmlReader = new StringReader(xmlResults);
		// construct a Xlan source object for the xml results
		Reader reader = xmlResults.getCharacterStream();
		final Source xmlSource = new StreamSource(reader);
		try {
			// Convert the output target for use in Xalan-J 2
			SQLXML result = XMLUtil.saveToBufferManager(bufferMgr, new XMLTranslator() {
				
				@Override
				public void translate(Writer writer) throws TransformerException {
	                TransformerFactory factory = TransformerFactory.newInstance();
	                Transformer transformer = null;
	                if (styleSource == null) {
	                	transformer = factory.newTransformer();
	                } else {
		                transformer = factory.newTransformer(styleSource);
	                }
	                if (XMLFormatConstants.XML_TREE_FORMAT.equals(xmlFormat)) {
	                	transformer.setOutputProperty(OutputKeys.INDENT, "yes"); //$NON-NLS-1$ 
	                }
	                // Feed the resultant I/O stream into the XSLT processor
					transformer.transform(xmlSource, new StreamResult(writer));
				}
			}, Streamable.STREAMING_BATCH_SIZE_IN_BYTES);
			// obtain the stringified XML results for the
			return new XMLType(result);
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
			}
		}
	}

	/**
	 * <p> This method sets a style sheet to this object. The style sheet is
	 * used to perform transformations on XML results
	 * @param styleSheet A string representing a xslt styleSheet
	 */
	public void setStylesheet(String styleSheet) {
		this.styleSheet = styleSheet;
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

	@Override
	public ProcessorPlan clone() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws MetaMatrixComponentException {
		this.plan.close();
	}

	@Override
	public List getOutputElements() {
		return this.plan.getOutputElements();
	}

	@Override
	public void initialize(CommandContext context,
			ProcessorDataManager dataMgr, BufferManager bufferMgr) {
		this.bufferMgr = bufferMgr;
	}

	@Override
	public TupleBatch nextBatch() throws BlockedException,
			MetaMatrixComponentException, MetaMatrixProcessingException {
		TupleBatch batch = plan.nextBatch();
		List<List<XMLType>> result = new ArrayList<List<XMLType>>(batch.getAllTuples().length);
		for (List tuple : batch.getAllTuples()) {
			result.add(Arrays.asList(postProcessDocument((XMLType)tuple.get(0))));
		}
		TupleBatch resultBatch = new TupleBatch(batch.getBeginRow(), result);
		resultBatch.setTerminationFlag(batch.getTerminationFlag());
		return resultBatch;
	}

	@Override
	public void open() throws MetaMatrixComponentException,
			MetaMatrixProcessingException {
		this.plan.open();
	}

	@Override
	public Map getDescriptionProperties() {
		return this.plan.getDescriptionProperties();
	}
	
	@Override
	public boolean requiresTransaction(boolean transactionalReads) {
		return plan.requiresTransaction(transactionalReads);
	}
	
	@Override
	public void reset() {
		this.plan.reset();
	}

}