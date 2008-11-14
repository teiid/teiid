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

package com.metamatrix.soap.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.metamatrix.jdbc.MMResultSet;
import com.metamatrix.soap.SOAPPlugin;

/**
 * ResultDocuments is an organized way of structuring XML documents
 * The following illustrates an example of an XML document
 * <xmldoc>
 * 	<xmlschema isprimary="true">
 * 		<xml .... </xml>
 * 	</xmlschema>
 * </xmldoc>
 * 
 * 
 * Stored Procedure Parameters are an organized way of structuring data necessary
 * for executing a stored procedure (SP) call against the MetaMatrix server
 * The following illustrates an example of 2 stored procedure parameters with a value of 1 and null
 * <paramaters>
 * 	<param index="1">1</param>
 * 	<param index="2"></param>
 * </parameters>
 * 
 * 
 * Tabular Results (TR) is an organized way of structuring results data.  The following example illustrates
 * the TR format with full metadata from a ResultSet with two columns and 3 rows of data.  
 * <resultset>
 *		<metadata elementCount=�2� >
 *		<column datatype="float" VDBName=�MyVDB� VDBVersion=�1� groupName=�MyTable� 
 *       		precision=�1�  radix=�� scale=�2� searchable=�true� isAutoIncrementing=�false� 
 *				isCaseSensitive=�false� isCurrency=�false� isNullable=�true� isSigned=�true�>FloatNum
 *		</column>
 *
 *		<column datatype="int" VDBName=�MyVDB� VDBVersion=�1� groupName=�MyTable� 
 *				precision=�1� scale=�0� radix=�� searchable=�true� isAutoIncrementing=�false� 
 *				isCaseSensitive=�false� isCurrency=�false� isNullable=�true� isSigned=�true�>IntNum
 *		</column>
 *		</metadata>
 *		<data>
 *			<row><cell>5.0</cell><cell>5</cell</row>
 *			<row><cell>8.0</cell><cell>5</cell></row>
 *			<row><cell>3.5</cell><cell>6</cell></row>
 *		</data>
 *	</resultset>
 *  
 * The following example illustrates limited metadata (which is the default) of the same result set
 * 
 * 	<resultset>
 *			<metadata elementCount=�2�>
 *				<column datatype="float">FloatNum</column>
 *				<column datatype="int">IntNum</column>
 *			</metadata>
 *			<data>
 *				<row><cell>5.0</cell><cell>5</cell</row>
 *				<row><cell>8.0</cell><cell>5</cell></row>
 *				<row><cell>3.5</cell><cell>6</cell></row>
 *			</data>
 *		</resultset>
 *
 * 
 */
public class XMLTranslator {

	/**
	 * Constructor for XMLTranslator.
	 */
	public XMLTranslator() {
		super();
	}
	
	private Document document;	
	
	/**
	 * Translate a results document into a w3c Element 
	 * @param documents The documents to translate
	 * @param returnSchema whether to include the schema within the document
	 * @return resultsElement
	 * @throws SOAPTranslationException if a translation error occurs
	 * 
	 */
	public Element translateResultDocuments(ResultSet documents) throws SQLException, XMLTranslationException {
		Element resultsElement = getNewElement(XMLTranslator.Constant_TAGS.XMLDOCUMENTS_RESULTS_TAG);			
		try{
			while(documents.next()){
				//create a document element
				Element documentElement = getNewElement(XMLTranslator.Constant_TAGS.XMLDOCUMENTS.XMLDOCUMENTS_TAG);	
				// get the document
				String documentString = documents.getString(1);
				documentString = documentString.substring(XMLTranslator.Constant_TAGS.XML_TAG.length());
				// create a text node
				Node documentNode = getNewTextNode(XMLTranslator.Constant_TAGS.XMLDOCUMENTS.XMLDOCUMENTS_TAG);
				documentNode.setNodeValue(documentString);
				// add the node to the document element
				documentElement.appendChild(documentNode);
				// add the element to the results
				resultsElement.appendChild(documentElement);
			}
		} catch(Exception e){
			throw new XMLTranslationException(e,ErrorMessageKeys.xml_0009, SOAPPlugin.Util.getString(ErrorMessageKeys.xml_0009));
		}

		resultsElement = addWarningsAndExceptions(resultsElement, documents);		
		return resultsElement;
	}
	
	/**
	 * Translate the schema into a w3c Element 
	 * @param schema The String schema of the the XML document
	 * @boolean isPrimary whether this schema is the priimary document
	 * @return Element
	 * @throws XMLTranslationException
	 * 
	 */
	public Element translateXMLSchema(String schema, boolean isPrimary) throws XMLTranslationException {
		if(schema == null){
			throw new XMLTranslationException(ErrorMessageKeys.xml_0010, SOAPPlugin.Util.getString(ErrorMessageKeys.xml_0010));	
		}
		Element schemaElement = getNewElement(XMLTranslator.Constant_TAGS.XMLSCHEMA.XMLSCHEMA_TAG);
		// get a new text node
		Node xmlNode = getNewTextNode(XMLTranslator.Constant_TAGS.XMLSCHEMA.XMLSCHEMA_TAG);
		xmlNode.setNodeValue(schema);
		schemaElement.setAttribute(XMLTranslator.Constant_TAGS.XMLSCHEMA.Attributes.IS_PRIMARY_TAG,""+isPrimary); //$NON-NLS-1$
		// add the schema to the element
		schemaElement.appendChild(xmlNode);
		return schemaElement;
	}
	
	/**
	 * This method will translate a list of XML String Schemas into a w3c Element 
	 * If the schema is null then it will return a schema of "Schema is not available" in the message node
	 * @param schemas a list of XML String schemas
	 * @return Element containing all the schemas
	 * @throws XMLTranslationException
	 * 
	 */
	public Element translateXMLSchemas(Element root, Collection schemas) throws XMLTranslationException {
		if(root == null){
			root = getNewElement(XMLTranslator.Constant_TAGS.XMLSCHEMA.XMLSCHEMA_TAG);			
		}
		if(schemas == null){
			Element schemaElement = translateXMLSchema(SOAPPlugin.Util.getString(ErrorMessageKeys.xml_0011), true);	
			root.appendChild(schemaElement);												
		} else {		
			// create a new element
			Iterator iter = schemas.iterator();
			boolean isPrimary = true;
			while(iter.hasNext()){
				String schema = (String)iter.next();
				Element schemaElement = translateXMLSchema(schema, isPrimary);			
				// add the element to the main element
				root.appendChild(schemaElement);
				isPrimary = false;
			}
		}
		return root;
	}
	
	
	/**
	 * Translates a literal XML parameter element into a Map with Int
	 * @param parameters (index and value) parameters in an XML format
	 */
	public Map translateStoredProcedureParameters(Element parameters) throws XMLTranslationException{
		Map parameterMap = new HashMap();
		// get the list of children
		NodeList list = parameters.getChildNodes();
		int numberOfParameters = list.getLength();
		Integer index = null;
		Object value = null;
		for(int itemIndex=0;itemIndex<numberOfParameters;itemIndex++){
			Node parameterNode = list.item(itemIndex);
			// get the index attribute
			NamedNodeMap attributes = parameterNode.getAttributes();
			Node indexAttribute = attributes.getNamedItem(XMLTranslator.Constant_TAGS.StoredProcedureParameters.Param.Attributes.INDEX_TAG);
			if(indexAttribute != null){
				// get the index value
				String indexString = indexAttribute.getNodeValue();
				try{
					index = new Integer(indexString);
				} catch(NumberFormatException nfe){
					throw new XMLTranslationException(nfe,ErrorMessageKeys.xml_0012, SOAPPlugin.Util.getString(ErrorMessageKeys.xml_0012, indexString));
				}
			} else {
				throw new XMLTranslationException(ErrorMessageKeys.xml_0013, SOAPPlugin.Util.getString(ErrorMessageKeys.xml_0013));
			}
			// now get the value
			Node valueNode = parameterNode.getFirstChild();
			if(valueNode != null){
				value = valueNode.getNodeValue();
			}
			// now put the index and value into the Map
			parameterMap.put(index,value);
		}	
		return parameterMap;	
	}	
	/**
	 * This method will translate MetaMatrix parameters into a literal XML Element containing 
	 * The list of parameters will be a map of Integer indexes to Object values correlating to the
	 * StoredProcedure's modeled information
	 * @param parameters
	 * @return literal xml element containing the parameters
	 * @throws XMLTranslationException if an error occurs
	 * 
	 */
	public Element translateParameters(Map parameters) throws XMLTranslationException{
		if(parameters == null){
			// this procedure might not have any parameters so just create an empty list
			parameters = new HashMap();				
		}
		Element parametersElement = getNewElement(XMLTranslator.Constant_TAGS.StoredProcedureParameters.PARAMETERS_TAG);					
		try{
			Iterator iter = parameters.keySet().iterator();
			while(iter.hasNext()){
				Integer index = (Integer)iter.next();
				Object value = parameters.get(index);
				// construct a parameter element
				Element parameter = constructParameterElement(index, value);									
				// add the parameter to the root element
				parametersElement.appendChild(parameter);
			}
		} catch(Exception e){
			throw new XMLTranslationException(e,ErrorMessageKeys.xml_0014, SOAPPlugin.Util.getString(ErrorMessageKeys.xml_0014, e.getMessage()));	
		}
		return parametersElement;
		
	}
	/**
	 * Construct a metadata parameter which is used for the metadata method getMetdata
	 * @param parameterName the name of the parameter
	 * @param filter the value to filter on
	 * @return Elemetn
	 * @throws XMLTranslationException
	 */
	public Element createMetadataParameter(String parameterName, String filter) throws XMLTranslationException{
		if(parameterName == null){
			throw new XMLTranslationException(ErrorMessageKeys.xml_0015, SOAPPlugin.Util.getString(ErrorMessageKeys.xml_0015));			
		}
		Element parametersElement = getNewElement(XMLTranslator.Constant_TAGS.MetadataParameter.METADATA_PARAMETER_TAG);
		parametersElement.setAttribute(XMLTranslator.Constant_TAGS.MetadataParameter.Attributes.NAME_TAG,parameterName);
		
		Node valueNode = getNewTextNode(XMLTranslator.Constant_TAGS.MetadataParameter.METADATA_PARAMETER_TAG);
		if(filter == null){
			filter = new String();	
		}
		valueNode.setNodeValue(filter);
		parametersElement.appendChild(valueNode);
		return parametersElement;
	} 
	
	/**
	 * Gets the appropriate metadata parameter element based on it it's name
	 * @param parameterName
	 * @return Element
	 * @throws XMLTranslationException
	 */
	public Element getMetadataParameterElement(Element rootElement, String parameterName) throws XMLTranslationException{
		NodeList list = rootElement.getChildNodes();
		int size = list.getLength();
		for(int i=0;i<size;i++){
			Node nodeToCheck = list.item(i);

			if(nodeToCheck.getNodeType() == Node.ELEMENT_NODE){
				String attributeValue = ((Element)nodeToCheck).getAttribute(XMLTranslator.Constant_TAGS.MetadataParameter.Attributes.NAME_TAG);
				if(parameterName.equalsIgnoreCase(attributeValue)){
					return (Element)nodeToCheck;	
				} 				
			}
		}	

		return rootElement;
	}
	/**
	 * Get the metadata parameter name of this element
	 * @return the metadata parameter name
	 * @throws XMLTranslationException
	 */
	public String getMetadataParameterName(Element metadataParameterElement) throws XMLTranslationException{
		return metadataParameterElement.getAttribute(XMLTranslator.Constant_TAGS.MetadataParameter.Attributes.NAME_TAG);		
	}
	/**
	 * Gets the metadata parameter value for a given element
	 * @param metadataParameterElemetn
	 * @throws XMLTranslationException
	 */
	public String getMetadataParameterValue(Element metadataParameterElement) throws XMLTranslationException{
		String value = null;
		Node textNode = metadataParameterElement.getFirstChild();
		if(textNode != null){
			value = textNode.getNodeValue();
		}
		return  value;
	}
	
	/**
	 * Construct a parameter element given an Integer index and Object value
	 * @param index
	 * @param value
	 * @return constructed Literal XML element
	 * @throws XMLTranslationException if an error occurs
	 * 
	 */
	private Element constructParameterElement(Integer index, Object value) throws XMLTranslationException{
		if(index == null){
			throw new XMLTranslationException(ErrorMessageKeys.xml_0016, SOAPPlugin.Util.getString(ErrorMessageKeys.xml_0016));	
		}
		Element parameterElement = getNewElement(XMLTranslator.Constant_TAGS.StoredProcedureParameters.Param.PARAM_TAG);							
		// add the attribute of index
		parameterElement.setAttribute(XMLTranslator.Constant_TAGS.StoredProcedureParameters.Param.Attributes.INDEX_TAG,index.toString());
		// create a value node
		Node valueNode = getNewTextNode(XMLTranslator.Constant_TAGS.StoredProcedureParameters.Param.PARAM_TAG);
		if(value == null){
			// create an empty string to represent a NULL value
			value = new String();	
		}
		valueNode.setNodeValue(value.toString());
		parameterElement.appendChild(valueNode);
		return parameterElement;		
	}
	
	
	/**
	 * Given an integer count of the rows affected, create a Literal XML Element 
	 * to be returned
	 * @param rowsAffected
	 * @return Element
	 * @throws XMLTranslationException if an error occurs
	 * 
	 */
	public Element translateRowsAffected(int rowsAffected) throws XMLTranslationException {
		Element rowsAffectedElement = getNewElement(XMLTranslator.Constant_TAGS.ROWS_AFFECTED_TAG);
		Node rowsAffectedNode = getNewTextNode(XMLTranslator.Constant_TAGS.ROWS_AFFECTED_TAG);
		rowsAffectedNode.setNodeValue(""+rowsAffected); //$NON-NLS-1$
		rowsAffectedElement.appendChild(rowsAffectedNode);		
		return rowsAffectedElement;	
	}
	

	/**
	 * Given a MetaMatrix Results object generate the Tabuluar Results
	 * 
	 */
	public  Element translateTabularResults(ResultSet results, boolean fullMetadata) throws SQLException, XMLTranslationException {
		// create the element to return
		Element resultsElement =  getNewElement(XMLTranslator.Constant_TAGS.RESULT_SET_TAG);
		try {
			Element metadataElement = createMetadataElement(results.getMetaData());
			
			List listOfColumnElements = createColumnElements(results.getMetaData(),fullMetadata);
			Iterator iter = listOfColumnElements.iterator();
			while(iter.hasNext()){
				Element columnElement = (Element)iter.next();
				// add the column element information to the results element
				metadataElement.appendChild(columnElement);
			}
			
			// add the metadata element to the results element
			resultsElement.appendChild(metadataElement);
			
			Element dataElement = createDataElement((MMResultSet)results);
			// add the dataElement to the results Element
			resultsElement.appendChild(dataElement);
		
	 	} catch (Exception e){
			throw new XMLTranslationException(e,ErrorMessageKeys.xml_0017, SOAPPlugin.Util.getString(ErrorMessageKeys.xml_0017, e.getMessage()));	
		}
		resultsElement = addWarningsAndExceptions(resultsElement, results);
		return resultsElement;
	}
	
	/**
	 * Given a MetaMatrix Results object generate the Tabuluar Results
	 * 
	 */
	public  Element translateTabularResults(ResultSet results, boolean fullMetadata, int[] rows) throws SQLException, XMLTranslationException {
		// create the element to return
		Element resultsElement =  getNewElement(XMLTranslator.Constant_TAGS.RESULT_SET_TAG);
		try {
			Element metadataElement = createMetadataElement(results.getMetaData());
			
			List listOfColumnElements = createColumnElements(results.getMetaData(), fullMetadata);
			Iterator iter = listOfColumnElements.iterator();			
			while(iter.hasNext()){
				Element columnElement = (Element)iter.next();
				// add the column element information to the results element
				metadataElement.appendChild(columnElement);
			}
			
			// add the metadata element to the results element
			resultsElement.appendChild(metadataElement);
			
			Element dataElement = createDataElement((MMResultSet)results, rows);
			// add the dataElement to the results Element
			resultsElement.appendChild(dataElement);
		
	 	} catch (Exception ste){
			throw new XMLTranslationException(ste,ErrorMessageKeys.xml_0017, SOAPPlugin.Util.getString(ErrorMessageKeys.xml_0017, ste.getMessage()));	
		}
		resultsElement = addWarningsAndExceptions(resultsElement, results);
		return resultsElement;
	}	

	private Element addWarningsAndExceptions(Element root, ResultSet results) throws SQLException, XMLTranslationException {
		// add any warnings
		Element warningElement = translateWarnings(results);
		if(warningElement != null){
			root.appendChild(warningElement);
		}
		return root;
	}
	/**
	 * Helper method to produce a warning element based on results
	 * will return null if there is no warning
	 * @param results
	 * @return element (or null if no warning exists)
	 * @throws XMLTranslationException if an error occurs
	 */
	private Element translateWarnings(ResultSet results) throws SQLException, XMLTranslationException {
		Element warningElement = null;
        SQLWarning warning = results.getWarnings();
        if(warning != null){
            // create the root warning element
            warningElement = getNewElement(XMLTranslator.Constant_TAGS.WARNINGS_TAG);
            for (;warning != null;
                  warning = warning.getNextWarning()) {
              String message = warning.getMessage();
              // create the warning element
              Element warningChild = getNewElement(XMLTranslator.Constant_TAGS.WARNING_TAG);            
              // create the text node containing the warning message
              Node childTextNode = getNewTextNode(XMLTranslator.Constant_TAGS.WARNING_TAG);             
              childTextNode.setNodeValue(message);
              warningChild.appendChild(childTextNode);
              warningElement.appendChild(warningChild);
            }
        }

		return warningElement;	
	}

	/**
	 * This method will create a list of Column element and add all of it's attributes to each one
	 * @param metadata
	 * @param fullMetadata
	 * @throws XMLTranslationException
	 * 
	 */
	private  List createColumnElements(ResultSetMetaData metadata, boolean fullMetadata) throws SQLException, XMLTranslationException {
		List listOfColumns = new ArrayList();
		int count = metadata.getColumnCount();
		for(int index=0;index<count;index++){
			Element columnElement = createColumnElement((com.metamatrix.jdbc.api.ResultSetMetaData)metadata,fullMetadata,index);
			listOfColumns.add(columnElement);
		}	
		return listOfColumns;
	}
	
	/**
	 * This method will create a data element 
	 * e.g.
	 * 		<data>
	 *				<row><cell>5.0</cell><cell>5</cell</row>
	 *				<row><cell>8.0</cell><cell>5</cell></row>
	 *				<row><cell>3.5</cell><cell>6</cell></row>
	 *			</data>
	 * @param results
	 * @return data Element
	 * @throws XMLTranslationException
	 * 
	 */
	private  Element createDataElement(MMResultSet results) throws XMLTranslationException {
		// create the data tag 
		Element dataElement = getNewElement(XMLTranslator.Constant_TAGS.Data.DATA_TAG);
		try{
			while(results.next()){
				List record = results.getCurrentRecord();
				Element rowElement = createRowElement(record);
				// add the row to the data Element
				dataElement.appendChild(rowElement);
			}
		} catch(Exception e){
			throw new XMLTranslationException(e.getMessage());
		}
		return dataElement;
	}

	private  Element createDataElement(MMResultSet results, int[] rows) throws XMLTranslationException {
		if(rows == null) {
			return createDataElement(results);
		}
		// create the data tag 
		Element dataElement = getNewElement(XMLTranslator.Constant_TAGS.Data.DATA_TAG);
		try{
			for(int i=0; i < rows.length; i++) {
				int rowIndex = rows[i];
				// move the cursor to the next row to be exported
				while(results.getRow() != rowIndex) {
					results.next();
				}
				List record = results.getCurrentRecord();
				Element rowElement = createRowElement(record);
				// add the row to the data Element
				dataElement.appendChild(rowElement);
			}
		} catch(Exception e){
			throw new XMLTranslationException(e.getMessage());
		}
		return dataElement;
	}
	
	/**
	 * This will construct a row element for the current row
	 * @param List of records for the current row
	 * @return Element (row Element)
	 * @throws XMLTranslationException
	 */
	private  Element createRowElement(List record) throws XMLTranslationException {
		// create the data tag 
		Element rowElement = getNewElement(XMLTranslator.Constant_TAGS.Data.Row.ROW_TAG);
		// now for each column create a cell and add it to the row element
		Iterator iter = record.iterator();
		while(iter.hasNext()){
			Element cellElement = getNewElement(XMLTranslator.Constant_TAGS.Data.Row.Cell.CELL_TAG);			
			Object value = iter.next();
			// create the cell element
			Node cellNode = getNewTextNode(XMLTranslator.Constant_TAGS.Data.Row.Cell.CELL_TAG);
			if(value == null){
				value = new String();	
			}
			cellNode.setNodeValue(value.toString());
			// add the cell to the row Element
			cellElement.appendChild(cellNode);
			rowElement.appendChild(cellElement);
		}
		return rowElement;	
	}
	

	/**
	 * This method will create a column element and all of it's attributes to it
	 * @param metadata
	 * @param fullMetadata
	 * @param index
	 * @return columnElement
	 * @throws XMLTranslationException if a MetadataAccessException occurs
	 * 
	 */
	private  Element createColumnElement(com.metamatrix.jdbc.api.ResultSetMetaData metadata, boolean fullMetadata, int index)
        throws SQLException, XMLTranslationException {
		Element columnElement = getNewElement(XMLTranslator.Constant_TAGS.Column.COLUMN_TAG);

//		try{
			columnElement.setAttribute(XMLTranslator.Constant_TAGS.Column.COLUMN_ATTRIBUTE_TAGS.DATATYPE,metadata.getColumnTypeName(index));				
			// create a text node
			Node columnNameNode = getNewTextNode(XMLTranslator.Constant_TAGS.Column.COLUMN_ATTRIBUTE_TAGS.DATATYPE);
			columnNameNode.setNodeValue(metadata.getColumnName(index));
			columnElement.appendChild(columnNameNode);
			
			if(fullMetadata){	
				// add the following attributes
				columnElement.setAttribute(XMLTranslator.Constant_TAGS.Column.COLUMN_ATTRIBUTE_TAGS.VDBNAME,metadata.getVirtualDatabaseName(index));
				columnElement.setAttribute(XMLTranslator.Constant_TAGS.Column.COLUMN_ATTRIBUTE_TAGS.VDBVERSION,metadata.getVirtualDatabaseVersion(index));
				columnElement.setAttribute(XMLTranslator.Constant_TAGS.Column.COLUMN_ATTRIBUTE_TAGS.GROUPNAME,metadata.getCatalogName(index));
				columnElement.setAttribute(XMLTranslator.Constant_TAGS.Column.COLUMN_ATTRIBUTE_TAGS.PRECISION,""+metadata.getPrecision(index)); //$NON-NLS-1$
				columnElement.setAttribute(XMLTranslator.Constant_TAGS.Column.COLUMN_ATTRIBUTE_TAGS.RADIX,"10"); //$NON-NLS-1$
				columnElement.setAttribute(XMLTranslator.Constant_TAGS.Column.COLUMN_ATTRIBUTE_TAGS.SCALE,""+metadata.getScale(index)); //$NON-NLS-1$
                columnElement.setAttribute(XMLTranslator.Constant_TAGS.Column.COLUMN_ATTRIBUTE_TAGS.SEARCHABLE,""+metadata.isSearchable(index)); //$NON-NLS-1$
				columnElement.setAttribute(XMLTranslator.Constant_TAGS.Column.COLUMN_ATTRIBUTE_TAGS.ISAUTOINCREMENTING,""+metadata.isAutoIncrement(index)); //$NON-NLS-1$
				columnElement.setAttribute(XMLTranslator.Constant_TAGS.Column.COLUMN_ATTRIBUTE_TAGS.ISCASESENSITIVE,""+metadata.isCaseSensitive(index)); //$NON-NLS-1$
				columnElement.setAttribute(XMLTranslator.Constant_TAGS.Column.COLUMN_ATTRIBUTE_TAGS.ISCURRENCY,""+metadata.isCurrency(index)); //$NON-NLS-1$
				columnElement.setAttribute(XMLTranslator.Constant_TAGS.Column.COLUMN_ATTRIBUTE_TAGS.ISNULLABLE,""+metadata.isNullable(index)); //$NON-NLS-1$
				columnElement.setAttribute(XMLTranslator.Constant_TAGS.Column.COLUMN_ATTRIBUTE_TAGS.ISSIGNED,""+metadata.isSigned(index)); //$NON-NLS-1$
				columnElement.setAttribute(XMLTranslator.Constant_TAGS.Column.COLUMN_ATTRIBUTE_TAGS.ISUPDATEABLE,""+metadata.isWritable(index));			 //$NON-NLS-1$
			} 	
//		}catch(MetadataAccessException mae){
//			throw new XMLTranslationException(mae);	
//		}
		return columnElement;
	}
	/**
	 * This method will create a metadata element and add all of it's attributes to it
	 * @param metadata
	 * @param fullMetadata
	 * @throws XMLTranslationException
	 * 
	 */
	private  Element createMetadataElement(ResultSetMetaData metadata) throws SQLException, XMLTranslationException {
		
		// Create the metadata element
		Element metadataElement = getNewElement(XMLTranslator.Constant_TAGS.Metadata.METADATA_TAG);	
		// add it's attributes
		metadataElement = addMetadataElementAttributes(metadataElement,metadata);
		
		return metadataElement;
	}
	
	/**
	 * This will construct an element with a namespace of metadata and an elementCount attribute
	 * @param metadata
	 * @throws XMLTranslationException if an error occurs
	 * an example is:
	 * <metadata elementCount=�2�>
	 */
	private  Element addMetadataElementAttributes(Element metadataElement, ResultSetMetaData metadata) throws SQLException {

		String elementCount = new String(""+ metadata.getColumnCount()); //$NON-NLS-1$
		metadataElement.setAttribute(XMLTranslator.Constant_TAGS.Metadata.Attributes.ELEMENT_COUNT_TAG,elementCount);
		return metadataElement;
	}
	
	
	/**
	 * This method will create a new w3c Element with a given name
	 * @param elementName
	 * @return Element
	 * @throws XMLTranslationException if it cannot create an Element
	 * 
	 */
	public  Element getNewElement(String elementName)  throws XMLTranslationException {
		if(elementName == null){
			throw new XMLTranslationException(ErrorMessageKeys.xml_0018, SOAPPlugin.Util.getString(ErrorMessageKeys.xml_0018) );
		}
		Document doc = null;
		try{
			doc = getDocument();		
		} catch(ParserConfigurationException pce){
			throw new XMLTranslationException(pce,ErrorMessageKeys.xml_0019, SOAPPlugin.Util.getString(ErrorMessageKeys.xml_0019, pce.getMessage()));	
		}
		return doc.createElement(elementName);		
	}
		
	protected  Document getDocument() throws ParserConfigurationException{
		if(document == null){
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			document = builder.newDocument();			
		}
		return document;	
	}

	/**
	 * This method will create a new w3c Node with a given name
	 * @param nodeName
	 * @return Node
	 * @throws XMLTranslationException if it cannot create a Node
	 * 
	 */	
	protected  Node getNewTextNode(String nodeName) throws XMLTranslationException {
		if(nodeName == null){
			throw new XMLTranslationException(ErrorMessageKeys.xml_0020, SOAPPlugin.Util.getString(ErrorMessageKeys.xml_0020));
		}
		Document doc = null;
		try{
			doc = getDocument();
		} catch(ParserConfigurationException pce){
			throw new XMLTranslationException(pce,ErrorMessageKeys.xml_0019, SOAPPlugin.Util.getString(ErrorMessageKeys.xml_0019, pce.getMessage()));	
		}
		return doc.createTextNode(nodeName);
	}
	
	
	public String translateElementToString(Node element)throws XMLTranslationException {
		StringBuffer result = new StringBuffer();	
		if(element.getNodeType() == Node.TEXT_NODE){
			result.append(element.getNodeValue());			
		} else {
			if(element.getParentNode() == null) {
				result.append(XMLTranslator.Constant_TAGS.XML_TAG);
			}
			result.append("<"); //$NON-NLS-1$
			result.append(element.getNodeName());
			// check for attributes
			NamedNodeMap map = element.getAttributes();
			if(map !=null){
				int attributes = map.getLength();
				for(int index=0;index<attributes;index++){
					// get each attribute
					Node attr = map.item(index);
					// get the name of the attribute
					result.append(" "); //$NON-NLS-1$
					result.append(attr.getNodeName());
					result.append("="); //$NON-NLS-1$
					result.append("\""); //$NON-NLS-1$
					result.append(attr.getNodeValue());
					result.append("\""); //$NON-NLS-1$
					
					if(index < attributes-1){
						result.append(" ");			 //$NON-NLS-1$
					}
				}
			}
			result.append(">"); //$NON-NLS-1$
			// now get any text for this node
	
			NodeList list = element.getChildNodes();
			int numberOfChildren = list.getLength();
			for(int i=0;i<numberOfChildren;i++){
				String child = translateElementToString(list.item(i));
				result.append(child);	
			}
			result.append("</"); //$NON-NLS-1$
			result.append(element.getNodeName());
			result.append(">"); //$NON-NLS-1$
		}
		
		return result.toString();
	}
	/**
	 * This class contains the Constant tag names used in construction of the XML results
	 * 
	 * @author Andrew Martello
	 * @since May 15, 2002
	 */
	public static final class Constant_TAGS {
		
		public static final String XML_TAG = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"; //$NON-NLS-1$
		
		public static final String ROWS_AFFECTED_TAG = "rowsaffected"; //$NON-NLS-1$
		
		public static final String RESULT_SET_TAG = "resultset"; //$NON-NLS-1$
		
		public static final String WARNINGS_TAG = "warnings"; //$NON-NLS-1$
		
		public static final String WARNING_TAG	= "warning"; //$NON-NLS-1$
				
		public static final String EXCEPTION_TAG = "exception"; //$NON-NLS-1$
						
		public static final String XMLDOCUMENTS_RESULTS_TAG = "xmlresults"; //$NON-NLS-1$
								
		public static final class XMLDOCUMENTS{
			public static final String XMLDOCUMENTS_TAG = "xmldoc";	 //$NON-NLS-1$
			
		}
		public static final class XMLSCHEMA{
			public static final String XMLSCHEMA_TAG = "xmlschema"; //$NON-NLS-1$
			public static final class Attributes{
				public static final String IS_PRIMARY_TAG = "isprimary";	 //$NON-NLS-1$
			}			
		}		
		
		public static final class StoredProcedureParameters{
			public static final String PARAMETERS_TAG = SOAPConstants.STORED_PROCEDURE_PARAMETERS.PARAMETERS_TAG;
			public static final class Param{
				public static final String PARAM_TAG = SOAPConstants.STORED_PROCEDURE_PARAMETERS.Param.PARAM_TAG;
				public static final class Attributes{
					public static final String INDEX_TAG = SOAPConstants.STORED_PROCEDURE_PARAMETERS.Param.Attributes.INDEX_TAG;
				}					
			}				
		}
		
		public static final class Metadata{
			public static final String METADATA_TAG = "metadata"; //$NON-NLS-1$
			public static final class Attributes{
				// how many elements are in the tabular results
				public static final String ELEMENT_COUNT_TAG		= "elementcount"; //$NON-NLS-1$
			}
		}		
		
		public static final class MetadataParameter{
			public static final String METADATA_PARAMETER_TAG = "metadataparameter"; //$NON-NLS-1$
			public static final class Attributes{
				public static final String NAME_TAG = "name";	 //$NON-NLS-1$
			}	
		}
		public static final class Column{
		
			public static final String COLUMN_TAG = "column"; //$NON-NLS-1$
			/**
			 * The following class contains the tag names for the metadata information
			 * that will be in the Tabular Results 
			 */
			public static final class COLUMN_ATTRIBUTE_TAGS{
				// the data type of the column
				public static final String DATATYPE				= "datatype"; //$NON-NLS-1$
				
				// the virtual database of the particular column
				public static final String VDBNAME				= "vdbname"; //$NON-NLS-1$
				
				// the virtual databse version of the particular column 
				public static final String VDBVERSION			= "vdbversion"; //$NON-NLS-1$
				
				// the name of the table
				public static final String GROUPNAME			= "groupname"; //$NON-NLS-1$
				
				// the number of decimal digits
				public static final String PRECISION				= "precision"; //$NON-NLS-1$
				
				// the column's radix 10 or 2
				public static final String RADIX						= "radix"; //$NON-NLS-1$
				
				// the column's scale
				public static final String SCALE					= "scale"; //$NON-NLS-1$
				
				// whether this column is searchable or not
				public static final String SEARCHABLE		= "searchable"; //$NON-NLS-1$
				
				// whether this column is auto incrementing or not
				public static final String ISAUTOINCREMENTING	= "isautoincrementing"; //$NON-NLS-1$
				
				// whether this column is case sensitive or not
				public static final String ISCASESENSITIVE			= "iscasesensitive"; //$NON-NLS-1$
				
				// whether this column can be a currency value
				public static final String ISCURRENCY			= "iscurrency"; //$NON-NLS-1$
				
				// whether this column can be nullable or not
				public static final String ISNULLABLE			= "isnullable"; //$NON-NLS-1$
				
				// whether this column is signed or not
				public static final String ISSIGNED				= "issigned"; //$NON-NLS-1$
				
				// whether this column is updateable or not
				public static final String ISUPDATEABLE		= "isupdateable"; //$NON-NLS-1$
			}
		}		
		
		public static final class Data {
			public static final String DATA_TAG = "data"; //$NON-NLS-1$
			
			public static final class Row {
				public static final String ROW_TAG 	= "row"; //$NON-NLS-1$
				
				public static final class Cell {
					public static final String CELL_TAG = "cell";	 //$NON-NLS-1$
				}					
			}			
		}		
	}
}
