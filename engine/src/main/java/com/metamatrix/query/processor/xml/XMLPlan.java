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
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import net.sf.saxon.TransformerFactoryImpl;

import org.xml.sax.Attributes;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BlockedOnMemoryException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.buffer.BufferManager.TupleSourceStatus;
import com.metamatrix.common.lob.LobChunk;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.InvalidReferenceException;
import com.metamatrix.common.types.SQLXMLImpl;
import com.metamatrix.common.types.Streamable;
import com.metamatrix.common.types.XMLType;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.processor.BaseProcessorPlan;
import com.metamatrix.query.processor.DescribableUtil;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.TempTableDataManager;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.tempdata.TempTableStore;
import com.metamatrix.query.tempdata.TempTableStoreImpl;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ErrorMessageKeys;
import com.metamatrix.query.util.LogConstants;
import com.metamatrix.query.util.XMLFormatConstants;
/**
 * 
 */
public class XMLPlan extends BaseProcessorPlan {
	
	// State passed during construction
	private XMLProcessorEnvironment env;
    private Program originalProgram;

    // A initial context to be sent to the executing instructions.
    XMLContext context = new XMLContext();

	// State initialized by processor
	private ProcessorDataManager dataMgr;
    private BufferManager bufferMgr;
    private int chunkSize = Streamable.STREAMING_BATCH_SIZE_IN_BYTES;

    private int nextBatchCount = 1;
        
    // this is tuple source id for the main batch of results, where multiple
    // xml documents are stored. This is different from the individual tuple source
    // id that each XML document is stored. This would be parent tuple source.
    TupleSourceID resultsTupleSourceId = null;
    
    // is document in progress currently?
    boolean docInProgress = false;
    TupleSourceID docInProgressTupleSourceId = null;
    int chunkPosition = 0;
    
    // Post-processing
	private String styleSheet;
    private boolean shouldValidate = false;
	private Collection xmlSchemas;

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
        setContext(context);
        if(context.getStreamingBatchSize() != 0){
        	this.chunkSize = context.getStreamingBatchSize();
        }
        TempTableStore tempTableStore = new TempTableStoreImpl(bufferMgr, context.getConnectionID(), (TempTableStore)context.getTempTableStore());
        //this.dataMgr = new StagingTableDataManager(new TempTableDataManager(dataMgr, tempTableStore), env);
        this.dataMgr = new TempTableDataManager(dataMgr, tempTableStore);
        this.bufferMgr = bufferMgr;
        this.env.initialize(context, this.dataMgr, this.bufferMgr);
    }

    public void reset() {
        super.reset();
        
        styleSheet = null;
        shouldValidate = false;
        
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

    public void open() throws MetaMatrixComponentException {
        if (this.resultsTupleSourceId == null) {
            this.resultsTupleSourceId = XMLUtil.createXMLTupleSource(bufferMgr, getContext().getConnectionID());
        }        
    }

    /**
     * @see ProcessorPlan#nextBatch()
     */
    public TupleBatch nextBatch()
        throws MetaMatrixComponentException, MetaMatrixProcessingException, BlockedException {
        
        // If finished, processXML();
        while(true){
	        List rows = processXML();
	        
            // we have no more to send
	        if (rows == null){
	        	TupleBatch batch = new TupleBatch(nextBatchCount++, Collections.EMPTY_LIST); 
	        	batch.setTerminationFlag(true);
                addBatchToBufferManager(batch);
	        	return batch;
	        }
            
            // we got one more to send.
    	    List listOfRows = new ArrayList();
        	listOfRows.add(rows);
        
	        TupleBatch batch = new TupleBatch(nextBatchCount++, listOfRows);
            // when true; multiple doc return fails.
    	    batch.setTerminationFlag(false);
            addBatchToBufferManager(batch);
        	return batch;
        }
    }

    
    void addBatchToBufferManager(TupleBatch batch) throws MetaMatrixComponentException {        
        try {
            this.bufferMgr.addTupleBatch(this.resultsTupleSourceId, batch);
            if (batch.getTerminationFlag()) {
                this.bufferMgr.setStatus(this.resultsTupleSourceId, TupleSourceStatus.FULL);
            }
        } catch (TupleSourceNotFoundException e) {
            throw new MetaMatrixComponentException(e);
        } 
    }
    
    /**
     * <p>Process the XML, using the stack of Programs supplied by the
     * ProcessorEnvironment.  With each pass through the loop, the
     * current Program is gotten off the top of the stack, and the
     * current instruction is gotten from that program; each call
     * to an instruction's process method may alter the Program
     * Stack and/or the current instruction pointer of a Program,
     * so it's important that this method's loop refer to the
     * call stack of the ProcessorEnvironment each time, and not
     * cache things in local variables.  If the current Program's
     * current instruction is null, then it's time to pop that
     * Program off the stack.</p>
     *
     * <p>This method will return a single tuple (List) for
     * each XML document chunk in the result sets. There may be
     * many XML documents, if the root of the document model has
     * a mapping class (i.e. result set) associated with it.</p>
     *
     * @return List a single tuple that contains the ValueID
     * of the XML document for the first document  chunk, or empty row
     * for the rest of the document chunk; or will return null when 
     * there are no more documents to return
     */
    private List processXML()
        throws MetaMatrixComponentException, MetaMatrixProcessingException, BlockedException {



        boolean postProcess = (this.styleSheet != null || this.shouldValidate);
        // Note that we need to stream the document right away, as multiple documents
        // results are generated from the same "env" object. So the trick here is either
        // post process will stream it, or save to buffer manager to stream the document
        // right away so that we give way to next batch call. (this due to bad design of xml model)
        // also note that there could be "inst" object alive but no more chunks; that is reason
        // for "if" null block
        LobChunk chunk = getNextXMLChunk(this.chunkSize);  
        XMLType xml = null;            
        
        if (chunk == null) {
            return null;
        }
        
        // if the chunk size is less than one chunk unit then send this as string based xml.
        if (!this.docInProgress && chunk.isLast()) {
            xml = new XMLType(new SQLXMLImpl(new String(chunk.getBytes(), Charset.forName("UTF-16")), getProperties())); //$NON-NLS-1$
        }
        else {
            
            // if this is the first chunk, then create a tuple source id for this sequence of chunks
            if (!this.docInProgress) {
                this.docInProgress = true;
                this.docInProgressTupleSourceId = XMLUtil.createXMLTupleSource(this.bufferMgr, this.resultsTupleSourceId.getStringID());
                this.chunkPosition = 1;
            }
            
            // now save the chunk of data to the buffer manager and move on.
            this.bufferMgr.addStreamablePart(this.docInProgressTupleSourceId, chunk, chunkPosition++);
                                
            // now document is finished, so create a xml object and return to the client.
            if (chunk.isLast()) {
                this.bufferMgr.setStatus(this.docInProgressTupleSourceId, TupleSourceStatus.FULL);
                
                // we want this to be naturally feed by chunks whether inside
                // or out side the processor
                xml = new XMLType();
                xml.setPersistenceStreamId(this.docInProgressTupleSourceId.getStringID());
                
                //reset current document state.
                this.docInProgress = false;
                this.docInProgressTupleSourceId = null;
                this.chunkPosition = 0;
            }
            else {
                throw BlockedOnMemoryException.INSTANCE;
            }
        }

        // check to see if we need to do any post validation on the document.
        if (postProcess){
            xml = postProcessDocument(xml, getProperties());
        }
                                
        List row = new ArrayList(1);
        row.add(xml);                    
        return row;
    }
        
    
    /**
     * This methods gets the next XML chunk object from the document in progree object. Thi used by the 
     * DocInProgressXMLTranslator to tuen this into a reader. 
     * @return char[] of data of given size; if less than size is returned, it will be treated as
     * document is finished or null if no chunk is available, just like blocked exception.
     */
    LobChunk getNextXMLChunk(int size) throws MetaMatrixComponentException, MetaMatrixProcessingException, BlockedException {

        // do the xml processing.
        ProcessorInstruction inst = env.getCurrentInstruction();
        while (inst != null){
        	LogManager.logTrace(LogConstants.CTX_QUERY_PLANNER, "Executing instruction", inst); //$NON-NLS-1$
            this.context = inst.process(this.env, this.context);

            //code to check for end of document, set current doc
            //to null, and return the finished doc as a single tuple
            DocumentInProgress doc = env.getDocumentInProgress();
            if (doc != null) {            
                //chunk size 0 mean no limit; get the whole document
                char[] chunk = doc.getNextChunk(size);
                if (chunk != null) {
                    if (doc.isFinished()) {
                        this.env.setDocumentInProgress(null);
                    }
                    byte[] bytes = Arrays.copyOfRange(new String(chunk).getBytes(Charset.forName("UTF-16")), 2, chunk.length * 2 + 2); //$NON-NLS-1$
                    return new LobChunk(bytes, doc.isFinished()); 
                }                
            }
            inst = env.getCurrentInstruction();
        }
        return null;
    }
        
    private Properties getProperties() {
        Properties props = new Properties();                
        if (XMLFormatConstants.XML_TREE_FORMAT.equals(this.env.getXMLFormat())) {
            props.setProperty("indent", "yes");//$NON-NLS-1$//$NON-NLS-2$
        }                
        return props;
    }
    
	private XMLType postProcessDocument(XMLType xmlDoc, Properties props) throws MetaMatrixComponentException {
        Reader source = null;

        try {        
            try {
                source = xmlDoc.getCharacterStream();
            } catch (InvalidReferenceException e) {
                xmlDoc = XMLUtil.getFromBufferManager(this.bufferMgr, new TupleSourceID(xmlDoc.getPersistenceStreamId()), props);
                source = xmlDoc.getCharacterStream();
            }
        
            // Validate against schema
            if(this.shouldValidate) {
                validateDoc(source);
            }

            // Apply XSLT
            xmlDoc = transformXML(xmlDoc, props);
            return xmlDoc;
        } catch (SQLException e) {
            throw new MetaMatrixComponentException(e);
        }
    }

    /**
     * Sets the XML schema
     * @param xmlSchema
     */
    public void setXMLSchemas(Collection xmlSchema){
    	this.xmlSchemas = xmlSchema;
    }

    /**
     * Returns the XML Schema
     * @return xmlSchema
     */
    public Collection getXMLSchemas(){
    	return this.xmlSchemas;
    }
    
    /**
     * Validate the document against the Apache Xerces parser
     * The constants in the code are specific to the Apache Xerces parser and must be used
     * Known limitiation is when it is attempted to validate against multiple schemas
     * @param xmlDoc
     * @throws MetaMatrixComponentException if the document cannot be validated against the schema
     *
     */
    private void validateDoc(Reader xmlStream) throws MetaMatrixComponentException {

		// get the schema
		if (xmlSchemas == null || xmlSchemas.isEmpty()){
		    // if there is no schema no need to validate
		    // return a warning saying there is no schema
            MetaMatrixException noSchema = new MetaMatrixComponentException(ErrorMessageKeys.PROCESSOR_0042, QueryExecPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0042));
			addWarning(noSchema);
			return;
		}
		
		// perform the validation
		HashMap nameSpaceMap = null;
		try{
		    // also find the target name space URIs for the document(s).
		    nameSpaceMap = getTargetNameSpaces(xmlSchemas);
		} catch(MetaMatrixException me){
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
            throw new MetaMatrixComponentException(err);
        } catch (ParserConfigurationException err) {
            throw new MetaMatrixComponentException(err);
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
			throw new MetaMatrixComponentException(se);
		} catch(IOException io){
			throw new MetaMatrixComponentException(io);
		}

		// determine if we have any warnings, errors, or fatal errors and report as necessary
		if (errorHandler.hasExceptions()) {
		    List exceptionList = errorHandler.getExceptionList();
		    for (Iterator i = exceptionList.iterator(); i.hasNext();) {
                addWarning((MetaMatrixException)i.next());                
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
   	private HashMap getTargetNameSpaces(Collection schemas) throws MetaMatrixException {
   		HashMap nameSpaceMap = new HashMap();
   		SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        SAXParser parser;
        try {
            parser = spf.newSAXParser();
        } catch (ParserConfigurationException err) {
            throw new MetaMatrixException(err);
        } catch (SAXException err) {
            throw new MetaMatrixException(err);
        }
   		PeekContentHandler pch = new PeekContentHandler();
        
   		Iterator it = schemas.iterator();       
        while (it.hasNext()) {
            String schema = (String)it.next();
			StringReader reader = new StringReader(schema);
			InputSource source = new InputSource(reader);
   	        pch.targetNameSpace = null;
	   		try {
                parser.parse(source, pch);
            } catch (SAXException err) {
                throw new MetaMatrixException(err);
            } catch (IOException err) {
                throw new MetaMatrixException(err);
            }
	   		
	   		// Record the name space with the schema
	   		if ( pch.targetNameSpace != null) {
	   		    nameSpaceMap.put(pch.targetNameSpace, schema);
	   		}	   		
        }
        return nameSpaceMap;
   	}

	/**
	 * <p> This method is used to transform the XML results obtained, by applying transformations
	 * using any style sheets that are added to the <code>Query</code> object.
	 * @param xmlResults The xml result string that is being transformed using stylesheets
	 * @return The xml string transformed using style sheets.
	 * @throws MetaMatrixComponentException if there is an error trying to perform transformation
	 */
	private XMLType transformXML(XMLType xmlResults, Properties props) throws SQLException, MetaMatrixComponentException {

		// perform transformation only if a style sheet is specified
		if(styleSheet != null && styleSheet.trim().length() > 0) {

			// get a reader object for the style sheet
			Reader styleReader = new StringReader(styleSheet);
			// construct a Xlan source object for the syle sheet
			Source styleSource = new StreamSource(styleReader);

			// get a reader object for the xml results
			//Reader xmlReader = new StringReader(xmlResults);
			// construct a Xlan source object for the xml results
			Source xmlSource = new StreamSource(xmlResults.getCharacterStream());

			// Convert the output target for use in Xalan-J 2
			StringWriter resultOut = new StringWriter();
			StreamResult result = new StreamResult(resultOut);

			try {
				// get the Xalan-J 2 XSLT transformer
                TransformerFactory factory = new TransformerFactoryImpl();
                Transformer transformer = factory.newTransformer(styleSource);

			    /*
                 * To use this line, the system property "javax.xml.transform.TransformerFactory"
                 * needs to be set to "com.icl.saxon.TransformerFactoryImpl" or desired
                 * TransformerFactory classname.  See com.metamatrix.jdbc.TestXMLQuery
			     */
                //Transformer transformer = TransformerFactory.newInstance().newTransformer(styleSource);

                // Feed the resultant I/O stream into the XSLT processor
				transformer.transform(xmlSource, result);
			} catch(Exception e) {
				throw new MetaMatrixComponentException(e, ErrorMessageKeys.PROCESSOR_0046, QueryExecPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0046));
			}

			// obtain the stringified XML results for the
			xmlResults = new XMLType(new SQLXMLImpl(resultOut.toString(), props));
		}

		return xmlResults;
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
        this.env.setXMLFormat(xmlFormat);
    }

    /**
     * Set the validation mode on this plan.  By default validation is off.
     * @param validate True to validate, false to not validate
     */
    public void setShouldValidate(boolean validate) {
        this.shouldValidate = validate;
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

    public String toString() {
        try{
            return "XMLPlan:\n" + ProgramUtil.programToString(this.originalProgram); //$NON-NLS-1$
        } catch (Exception e){
            e.printStackTrace();
            LogManager.logWarning(LogConstants.CTX_XML_PLAN, e,
                                 QueryExecPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0001));
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
		ArrayList exceptionList = null;
		
		/**
		 * Keep track of all the exceptions
		 */
		private void addException(MetaMatrixException me) {
		    if (exceptionList == null) {
		        exceptionList = new ArrayList();
		    }
		    exceptionList.add(me);
		}
		
		public List getExceptionList() {
		    return exceptionList;
		}
		
		public boolean hasExceptions() {
		    return exceptionList != null && !exceptionList.isEmpty();
		}
		
		public void error(SAXParseException ex){
		    addException(new MetaMatrixComponentException(ErrorMessageKeys.PROCESSOR_0049, QueryExecPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0048, ex.getMessage())));
		}
		public void fatalError(SAXParseException ex){			
		    addException(new MetaMatrixComponentException(ErrorMessageKeys.PROCESSOR_0048, QueryExecPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0048, ex.getMessage())));			
		}
		public void warning(SAXParseException ex){
		    addException(new MetaMatrixComponentException(ErrorMessageKeys.PROCESSOR_0049, QueryExecPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0048, ex.getMessage())));
		}
	}

	/**
     * The plan is only clonable in the pre-execution stage, not the execution state
 	 * (things like program state, result sets, etc). It's only safe to call that method in between query processings,
 	 * in other words, it's only safe to call clone() on a plan after nextTuple() returns null,
 	 * meaning the plan has finished processing.
 	 */
	public Object clone(){
        XMLPlan xmlPlan = new XMLPlan((XMLProcessorEnvironment)this.env.clone());
        return xmlPlan;
    }

    /*
     * @see com.metamatrix.query.processor.Describable#getDescriptionProperties()
     */
    public Map getDescriptionProperties() {
        Map props = this.originalProgram.getDescriptionProperties();
//      Get relational plans to get description props and add them to children
        Collection childRelationalPlans = env.getChildPlans();
        ArrayList children = (ArrayList) props.get(PROP_CHILDREN);
        if(childRelationalPlans!=null) {
	        Iterator childRelationalPlansIterator = childRelationalPlans.iterator();
	        while(childRelationalPlansIterator.hasNext()) {
	            ProcessorPlan relationalPlan = (ProcessorPlan) childRelationalPlansIterator.next();
	            Map planProperties = relationalPlan.getDescriptionProperties();
	            children.add(planProperties);
	        }
        }
        props.put(PROP_CHILDREN, children);
        props.put(PROP_TYPE, "XML Plan"); //$NON-NLS-1$
        props.put(PROP_OUTPUT_COLS, DescribableUtil.getOutputColumnProperties(getOutputElements()));

        return props;
    }
    
    /** 
     * @see com.metamatrix.query.processor.ProcessorPlan#getChildPlans()
     * @since 4.2
     */
    public Collection getChildPlans() {
        return this.env.getChildPlans();
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