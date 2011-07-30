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

package org.teiid.query.processor.xml;

import static org.teiid.query.analysis.AnalysisRecord.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.XMLType;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.util.CommandContext;
import org.xml.sax.Attributes;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;


/**
 * 
 */
public class XMLPlan extends ProcessorPlan {
	
	// State passed during construction
	private XMLProcessorEnvironment env;
    private Program originalProgram;

    // A initial context to be sent to the executing instructions.
    XMLContext context = new XMLContext();

	// State initialized by processor
	private ProcessorDataManager dataMgr;
    private BufferManager bufferMgr;

    private int nextBatchCount = 1;
        
    // Post-processing
	private Collection<SQLXML> xmlSchemas;
	/**  XML results format:  XML results displayed as a tree*/
	public static final String XML_TREE_FORMAT = "Tree"; //$NON-NLS-1$
	/**  XML results format:  XML results displayed in compact form*/
	public static final String XML_COMPACT_FORMAT = "Compact"; //$NON-NLS-1$

    /**
     * Constructor for XMLPlan.
     */
    public XMLPlan(XMLProcessorEnvironment env) {
        this.env = env;
		this.originalProgram = this.env.getCurrentProgram();
    }

    /**
     * @see ProcessorPlan#initialize(ProcessorDataManager, Object)
     */
    public void initialize(CommandContext context, ProcessorDataManager dataMgr, BufferManager bufferMgr) {
    	context = context.clone();
    	setContext(context);
        TempTableStore tempTableStore = new TempTableStore(context.getConnectionID());
        tempTableStore.setParentTempTableStore(context.getTempTableStore());
        context.setTempTableStore(tempTableStore);
        this.dataMgr = dataMgr;
        this.bufferMgr = bufferMgr;
        this.env.initialize(context, this.dataMgr, this.bufferMgr);
    }

    public void reset() {
        super.reset();
        
        nextBatchCount = 1;
        
        this.env = (XMLProcessorEnvironment)this.env.clone();
        
		LogManager.logTrace(LogConstants.CTX_XML_PLAN, "XMLPlan reset"); //$NON-NLS-1$
    }

    public ProcessorDataManager getDataManager() {
        return this.dataMgr;
    }

    /**
     * Get list of resolved elements describing output columns for this plan.
     * @return List of SingleElementSymbol
     */
    public List getOutputElements() {
        ArrayList output = new ArrayList(1);
        ElementSymbol xml = new ElementSymbol("xml"); //$NON-NLS-1$
        xml.setType(DataTypeManager.DefaultDataClasses.XML);
        output.add(xml);
        return output;
    }

    public void open() throws TeiidComponentException {
    }

    /**
     * @see ProcessorPlan#nextBatch()
     */
    public TupleBatch nextBatch()
        throws TeiidComponentException, TeiidProcessingException, BlockedException {
        
        while(true){
        	// do the xml processing.
            ProcessorInstruction inst = env.getCurrentInstruction(this.context);
            while (inst != null){
            	LogManager.logTrace(LogConstants.CTX_XML_PLAN, "Executing instruction", inst); //$NON-NLS-1$
                this.context = inst.process(this.env, this.context);

                //code to check for end of document, set current doc
                //to null, and return the finished doc as a single tuple
                DocumentInProgress doc = env.getDocumentInProgress();
                if (doc != null && doc.isFinished()) {
                    this.env.setDocumentInProgress(null);
                    XMLType xml = new XMLType(doc.getSQLXML());
                    // check to see if we need to do any post validation on the document.
                    if (getContext().validateXML()){
                    	Reader reader;
            			try {
            				reader = xml.getCharacterStream();
            			} catch (SQLException e) {
            				throw new TeiidComponentException(e);
            			}
                    	try {
                    		validateDoc(reader);
                    	} finally {
                    		try {
            					reader.close();
            				} catch (IOException e) {
            				}
                    	}
                    }
        	        TupleBatch batch = new TupleBatch(nextBatchCount++, Arrays.asList(Arrays.asList(xml)));
        	        return batch;
                }
                inst = env.getCurrentInstruction(this.context);
            }
            
        	TupleBatch batch = new TupleBatch(nextBatchCount++, Collections.EMPTY_LIST); 
        	batch.setTerminationFlag(true);
        	return batch;
        }
    }
    
    /**
     * Sets the XML schema
     * @param xmlSchema
     */
    public void setXMLSchemas(Collection<SQLXML> xmlSchema){
    	this.xmlSchemas = xmlSchema;
    }

    /**
     * Returns the XML Schema
     * @return xmlSchema
     */
    public Collection<SQLXML> getXMLSchemas(){
    	return this.xmlSchemas;
    }
    
    /**
     * Validate the document against the Apache Xerces parser
     * The constants in the code are specific to the Apache Xerces parser and must be used
     * Known limitiation is when it is attempted to validate against multiple schemas
     * @param xmlDoc
     * @throws TeiidComponentException if the document cannot be validated against the schema
     *
     */
    private void validateDoc(Reader xmlStream) throws TeiidComponentException {

		// get the schema
		if (xmlSchemas == null || xmlSchemas.isEmpty()){
		    // if there is no schema no need to validate
		    // return a warning saying there is no schema
            TeiidException noSchema = new TeiidComponentException("ERR.015.006.0042", QueryPlugin.Util.getString("ERR.015.006.0042")); //$NON-NLS-1$ //$NON-NLS-2$
			addWarning(noSchema);
			return;
		}
		
		// perform the validation
		HashMap nameSpaceMap = null;
		try{
		    // also find the target name space URIs for the document(s).
		    nameSpaceMap = getTargetNameSpaces(xmlSchemas);
		} catch(TeiidException me){
			addWarning(me);
			nameSpaceMap = new HashMap();
		} 
		
		// Create a SAXParser
		SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        spf.setValidating(true);
        XMLReader reader = null;

		// set the features on the parser
		try{
	        SAXParser parser = spf.newSAXParser();
	        parser.setProperty("http://java.sun.com/xml/jaxp/properties/schemaLanguage", "http://www.w3.org/2001/XMLSchema"); //$NON-NLS-1$ //$NON-NLS-2$
	        parser.setProperty("http://java.sun.com/xml/jaxp/properties/schemaSource", nameSpaceMap.keySet().toArray());  //$NON-NLS-1$
	        reader = parser.getXMLReader();
		} catch (SAXException err) {
            throw new TeiidComponentException(err);
        } catch (ParserConfigurationException err) {
            throw new TeiidComponentException(err);
        }
		
		// place the schema into the customized entity resolver so that we can
		// resolve the schema elements
		EntityResolver xmlEntityResolver = new MultiEntityResolver(nameSpaceMap);
		reader.setEntityResolver(xmlEntityResolver);

		// Create the specialized error handler so that we can get any warnings, 
		// errors, or fatal errors back from validation
		MMErrorHandler errorHandler = new MMErrorHandler();		
		reader.setErrorHandler(errorHandler);
		
		// create the input stream for the xml document to be parsed
		InputSource source = new InputSource(xmlStream);
		
		try{
		    reader.parse(source);
		} catch(SAXException se){
			throw new TeiidComponentException(se);
		} catch(IOException io){
			throw new TeiidComponentException(io);
		}

		// determine if we have any warnings, errors, or fatal errors and report as necessary
		if (errorHandler.hasExceptions()) {
		    List exceptionList = errorHandler.getExceptionList();
		    for (Iterator i = exceptionList.iterator(); i.hasNext();) {
                addWarning((TeiidException)i.next());                
            }		    
		}
    }
    
	/**
	 * This class will be used to peek the contents of the XML document before
	 * full pledged parsing for validation against a DTD or XML Schema is done 
	 */
	static class PeekContentHandler extends DefaultHandler{
		private static final String TARGETNAMESPACE = "targetNamespace"; //$NON-NLS-1$
        String targetNameSpace = null;
	
		/**
		 * walk through the tree and get the target name space of the document
		 */
		public void startElement(final String namespace, final String name,
                final String qualifiedName, final Attributes attrs)
                throws SAXException {
		    
            // Grab the All the namespace declarations from the XML Document
		    for (int i=0; i < attrs.getLength(); i++) {
		        if (attrs.getQName(i).equals(TARGETNAMESPACE)) { 
		            targetNameSpace=attrs.getValue(i);
		        }
		    }            		          
        }		
	}
	
    /**
     * This code will extract the "TargetNameSpace" attribute which specifies
     * the namespace foe the document from the given schema(s) and makes map of
     * namespaces Vs schemas. 
     * @throws SAXException 
     * @throws ParserConfigurationException 
     * @throws IOException 
     */
   	private HashMap getTargetNameSpaces(Collection<SQLXML> schemas) throws TeiidException {
   		HashMap nameSpaceMap = new HashMap();
   		SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        SAXParser parser;
        try {
            parser = spf.newSAXParser();
        } catch (ParserConfigurationException err) {
            throw new TeiidException(err);
        } catch (SAXException err) {
            throw new TeiidException(err);
        }
   		PeekContentHandler pch = new PeekContentHandler();
        
   		for (SQLXML schema : schemas) {
   			InputStream is;
			try {
				is = schema.getBinaryStream();
			} catch (SQLException e) {
				throw new TeiidComponentException(e);
			}
			InputSource source = new InputSource(is);
   	        pch.targetNameSpace = null;
	   		try {
                parser.parse(source, pch);
            } catch (SAXException err) {
                throw new TeiidException(err);
            } catch (IOException err) {
                throw new TeiidComponentException(err);
            } finally {
            	try {
					is.close();
				} catch (IOException e) {
					
				}
            }
	   		
	   		// Record the name space with the schema
	   		if ( pch.targetNameSpace != null) {
	   		    nameSpaceMap.put(pch.targetNameSpace, schema);
	   		}	   		
        }
        return nameSpaceMap;
   	}

    /**
     * This method sets whether the documents should be returned in compact
     * format (no extraneous whitespace).  Non-compact format is more human-readable
     * (and bigger).  Additional formats may be possible in future.
     * @param xmlFormat A string giving the format in which xml results need to be returned
     */
    public void setXMLFormat(String xmlFormat) {
        this.env.setXMLFormat(xmlFormat);
    }

    /**
     * Clean up the tuple source when the plan is closed. 
     * @see org.teiid.query.processor.ProcessorPlan#close()
     */
    public void close() throws TeiidComponentException {
    }

    public String toString() {
        try{
            return "XMLPlan:\n" + ProgramUtil.programToString(this.originalProgram); //$NON-NLS-1$
        } catch (Exception e){
            e.printStackTrace();
            LogManager.logWarning(LogConstants.CTX_XML_PLAN, e,
                                 QueryPlugin.Util.getString("ERR.015.006.0001")); //$NON-NLS-1$
        }
        return "XMLPlan"; //$NON-NLS-1$
    }

	 /**
	  * A helper class to resolve the entities in the schema with their 
	  * associated Target Name Space 
	  */
	 private static class MultiEntityResolver implements EntityResolver {
			private HashMap schemaMap;

		 	public MultiEntityResolver(HashMap map){
		 		this.schemaMap = map;
		 	}
			public InputSource resolveEntity (String publicId, String systemId) {
			    String xsd = (String)schemaMap.get(systemId);
			    if (xsd != null) {
                    StringReader reader = new StringReader(xsd);
                    InputSource source = new InputSource(reader);
                    return source;                     
			    }
			    return null;
			}
	}	 
	/**
	 * Custom Error Handler to report back to the calling validation method
	 * any errors that occur during XML processing
	 */
	private static class MMErrorHandler implements ErrorHandler{
		ArrayList<TeiidException> exceptionList = null;
		
		/**
		 * Keep track of all the exceptions
		 */
		private void addException(TeiidException me) {
		    if (exceptionList == null) {
		        exceptionList = new ArrayList<TeiidException>();
		    }
		    exceptionList.add(me);
		}
		
		public List<TeiidException> getExceptionList() {
		    return exceptionList;
		}
		
		public boolean hasExceptions() {
		    return exceptionList != null && !exceptionList.isEmpty();
		}
		
		public void error(SAXParseException ex){
		    addException(new TeiidComponentException("ERR.015.006.0049", QueryPlugin.Util.getString("ERR.015.006.0048", ex.getMessage()))); //$NON-NLS-1$ //$NON-NLS-2$
		}
		public void fatalError(SAXParseException ex){			
		    addException(new TeiidComponentException("ERR.015.006.0048", QueryPlugin.Util.getString("ERR.015.006.0048", ex.getMessage())));			 //$NON-NLS-1$ //$NON-NLS-2$
		}
		public void warning(SAXParseException ex){
		    addException(new TeiidComponentException("ERR.015.006.0049", QueryPlugin.Util.getString("ERR.015.006.0048", ex.getMessage()))); //$NON-NLS-1$ //$NON-NLS-2$
		}
	}

	/**
     * The plan is only clonable in the pre-execution stage, not the execution state
 	 * (things like program state, result sets, etc). It's only safe to call that method in between query processings,
 	 * in other words, it's only safe to call clone() on a plan after nextTuple() returns null,
 	 * meaning the plan has finished processing.
 	 */
	public XMLPlan clone(){
        XMLPlan xmlPlan = new XMLPlan((XMLProcessorEnvironment)this.env.clone());
        xmlPlan.xmlSchemas = this.xmlSchemas;
        return xmlPlan;
    }

    public PlanNode getDescriptionProperties() {
    	PlanNode node = this.originalProgram.getDescriptionProperties();
    	node.addProperty(PROP_OUTPUT_COLS, AnalysisRecord.getOutputColumnProperties(getOutputElements()));
    	return node;
    }
    
    public GroupSymbol getDocumentGroup() {
        return env.getDocumentGroup();
    }

    
    /** 
     * @return Returns the originalProgram.
     */
    public Program getOriginalProgram() {
        return this.originalProgram;
    }
    
}