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

package org.teiid.test.client.ctc;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.*;

import org.jdom.Attribute;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.internal.core.xml.SAXBuilderHelper;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.test.client.QuerySQL;
import org.teiid.test.client.QueryTest;
import org.teiid.test.client.ctc.QueryResults.ColumnInfo;
import org.teiid.test.framework.TestLogger;
import org.teiid.test.framework.exception.TransactionRuntimeException;
import org.teiid.test.util.StringUtil;



/**
 * <P> This program helps in parsing XML Query and Results files into
 * map objects containing individual queries/ResultSets</P>
 *
 * <P> This program is useful to convert the JDBC ResultSet objects into
 * XML format. We physically walk through the ResultSet object and use JDOM to
 * convert the ResultSets into XML. This also helps convert Exceptions into XML
 * format.</P>
 */
@SuppressWarnings("nls")
public class XMLQueryVisitationStrategy {

    //the row from which we start converting ResultSets to XML
    private static final int START_ROW = 1;

    // list containing datatypes of field identifiers

    public XMLQueryVisitationStrategy() {
    }

    /**
     * Consume an XML Query File and produce a Map containing queries, with
     * queryNames/IDs as Keys.
     * <br>
     * @param queryFile the XML file object that is to be parsed
     * @return the List containing quers.
     * @exception JDOMException if there is an error consuming the message.
     */
    public List parseXMLQueryFile(String queryScenarioID, File queryFile, String querySetID) throws IOException, JDOMException {

	List<QueryTest> queries = new LinkedList();
//        HashMap queryMap = new HashMap();
        SAXBuilder builder = SAXBuilderHelper.createSAXBuilder(false);
        Document queryDocument = builder.build(queryFile);
        List queryElements = queryDocument.getRootElement().getChildren(TagNames.Elements.QUERY);
        Iterator iter = queryElements.iterator();
        while ( iter.hasNext() ) {
            Element queryElement = (Element) iter.next();
            String queryName = queryElement.getAttributeValue(TagNames.Attributes.NAME);
            if ( queryElement.getChild(TagNames.Elements.EXCEPTION) == null ) {
        	String uniqueID = querySetID + "_" + queryName;
        	
		List<Element> parmChildren = queryElement.getChildren(TagNames.Elements.SQL);
        	
		if (parmChildren == null || parmChildren.isEmpty()) {
        	    TestLogger.logDebug("=======  Single QueryTest ");
        	    QuerySQL sql = createQuerySQL(queryElement);
         	    
        	    QueryTest q = new QueryTest(queryScenarioID, uniqueID, querySetID, new QuerySQL[] {sql}, false);
        	    queries.add(q);
        	} else {
        	    TestLogger.logDebug("=======  QueryTest has multiple sql statements");
         		QuerySQL[] querysql = new QuerySQL[parmChildren.size()];
        		int c = 0;
        		
        		final Iterator<Element> sqliter = parmChildren.iterator();
        		while ( sqliter.hasNext() ) {
        			final Element sqlElement = (Element) sqliter.next();
        			QuerySQL sql = createQuerySQL(sqlElement);
        			querysql[c] = sql;
        			c++;	
        		}
        		QueryTest q = new QueryTest(queryScenarioID, uniqueID, querySetID, querysql, false);
        		queries.add(q);
               		
        		
       	    
        	}
 //               queryMap.put(queryName, query);
            } else {
                Element exceptionElement = queryElement.getChild(TagNames.Elements.EXCEPTION);
                String exceptionType = exceptionElement.getChild(TagNames.Elements.CLASS).getTextTrim();
                
                String uniqueID = querySetID + "_" + queryName;
                QuerySQL sql = new QuerySQL(exceptionType, null);
                
                QueryTest q = new QueryTest(queryScenarioID, uniqueID, querySetID, new QuerySQL[] {sql}, true);
                queries.add(q);

                
 //               queryMap.put(queryName, exceptionType);
            }
        }
        return queries;
    }
    
    private QuerySQL createQuerySQL(Element queryElement) {
 	    String query = queryElement.getTextTrim();
 	    	    
	    Object[] parms = getParms(queryElement);
	    	    
	    QuerySQL sql = new QuerySQL(query, parms);
	    
 	    String updateCnt = queryElement.getAttributeValue(TagNames.Attributes.UPDATE_CNT);
 	    if (updateCnt != null && updateCnt.trim().length() > 0) {
 		int cnt = Integer.parseInt(updateCnt);
 		sql.setUpdateCnt(cnt);
 	    }
 	    
 	    String rowCnt = queryElement.getAttributeValue(TagNames.Attributes.TABLE_ROW_COUNT);
 	    if (rowCnt != null && rowCnt.trim().length() > 0) {
 		int cnt = Integer.parseInt(rowCnt);
 		sql.setRowCnt(cnt);
 	    }
	    
	    return sql;	
    }
    
    private Object[] getParms(Element parent) {
	List<Element> parmChildren = parent.getChildren(TagNames.Elements.PARM);
	if (parmChildren == null) {
	    return null;
	}
	
	Object[] parms = new Object[parmChildren.size()];
	int i = 0;
	final Iterator<Element> iter = parmChildren.iterator();
	while ( iter.hasNext() ) {
		final Element parmElement = (Element) iter.next();
		try {
		    Object parm = createParmType(parmElement);
		    parms[i] = parm;
		    i++;
		} catch (JDOMException e) {
		    throw new TransactionRuntimeException(e);
		}		
	}
	
	
	
	return parms;
    }
    
    private Object createParmType(Element cellElement) throws JDOMException {

        Object cellObject = null;
        
        final String typeName = cellElement.getAttributeValue(TagNames.Attributes.TYPE);
 
        if ( typeName.equalsIgnoreCase(TagNames.Elements.BOOLEAN) ) {
            cellObject = consumeMsg((Boolean) cellObject, cellElement);
        } else if ( typeName.equalsIgnoreCase(TagNames.Elements.STRING) ) {
            cellObject = consumeMsg((String) cellObject, cellElement);
        } else if ( typeName.equalsIgnoreCase(TagNames.Elements.CHAR) ) {
            cellObject = consumeMsg((Character) cellObject, cellElement);
        } else if ( typeName.equalsIgnoreCase(TagNames.Elements.BYTE) ) {
            cellObject = consumeMsg((Byte) cellObject, cellElement);
        } else if ( typeName.equalsIgnoreCase(TagNames.Elements.DOUBLE) ) {
            cellObject = consumeMsg((Double) cellObject, cellElement);
        } else if ( typeName.equalsIgnoreCase(TagNames.Elements.DATE) ) {
            cellObject = consumeMsg((java.sql.Date) cellObject, cellElement);
        } else if ( typeName.equalsIgnoreCase(TagNames.Elements.TIME) ) {
            cellObject = consumeMsg((Time) cellObject, cellElement);
        } else if ( typeName.equalsIgnoreCase(TagNames.Elements.TIMESTAMP) ) {
            cellObject = consumeMsg((Timestamp) cellObject, cellElement);
        } else if ( typeName.equalsIgnoreCase(TagNames.Elements.FLOAT) ) {
            cellObject = consumeMsg((Float) cellObject, cellElement);
        } else if ( typeName.equalsIgnoreCase(TagNames.Elements.BIGDECIMAL) ) {
            cellObject = consumeMsg((BigDecimal) cellObject, cellElement);
        } else if ( typeName.equalsIgnoreCase(TagNames.Elements.BIGINTEGER) ) {
            cellObject = consumeMsg((BigInteger) cellObject, cellElement);
        } else if ( typeName.equalsIgnoreCase(TagNames.Elements.INTEGER) ) {
            cellObject = consumeMsg((Integer) cellObject, cellElement);
        } else if ( typeName.equalsIgnoreCase(TagNames.Elements.LONG) ) {
            cellObject = consumeMsg((Long) cellObject, cellElement);
        } else if ( typeName.equalsIgnoreCase(TagNames.Elements.SHORT) ) {
            cellObject = consumeMsg((Short) cellObject, cellElement);
        } else if ( typeName.equalsIgnoreCase(TagNames.Elements.OBJECT) ) {
            cellObject = consumeMsg((String) cellObject, cellElement);
        }

        return cellObject;
    }

    /**
     * Consume an XML results File and produce a Map containing query results
     * as List objects, with resultNames/IDs as Keys.
     * <br>
     * @param resultsFile the XML file object that is to be parsed
     * @return the Map containig results.
     * @exception JDOMException if there is an error consuming the message.
     */
    public ResultsHolder parseXMLResultsFile(final File resultsFile) throws IOException, JDOMException {

        QueryResults queryResults;
        ResultsHolder expectedResults = null;

        final SAXBuilder builder = SAXBuilderHelper.createSAXBuilder(false);
        final Document resultsDocument = builder.build(resultsFile);
        final String query = resultsDocument.getRootElement().getChildText(TagNames.Elements.QUERY);
        final List resultElements = resultsDocument.getRootElement().getChildren(TagNames.Elements.QUERY_RESULTS);
        final Iterator iter = resultElements.iterator();
        while ( iter.hasNext() ) {
            final Element resultElement = (Element) iter.next();
            final String resultName = resultElement.getAttributeValue(TagNames.Attributes.NAME);
            queryResults = consumeMsg(new QueryResults(), resultElement);
            if ( queryResults.getFieldCount() != 0 ) {
                //
                // We've got a ResultSet
                //
                expectedResults = new ResultsHolder( TagNames.Elements.QUERY_RESULTS );
                expectedResults.setQueryID( resultName );
                expectedResults.setQuery(query);
                expectedResults.setIdentifiers( queryResults.getFieldIdents() );
                expectedResults.setTypes( queryResults.getTypes() );
                if ( queryResults.getRecordCount() > 0 ) {
                    expectedResults.setRows(queryResults.getRecords());
                }
            } else {
                //
                // We've got an exception
                //
                expectedResults = new ResultsHolder( TagNames.Elements.EXCEPTION );
                expectedResults.setQueryID( resultName );
                expectedResults.setQuery(query);

                final Element exceptionElement = resultElement.getChild(TagNames.Elements.EXCEPTION);
                if ( exceptionElement != null ) {
                    expectedResults.setExceptionClassName(exceptionElement.getChild(TagNames.Elements.CLASS).getTextTrim());
                    String msg = exceptionElement.getChild(TagNames.Elements.MESSAGE).getTextTrim();   
                    expectedResults.setExceptionMsg(StringUtil.removeChars(msg, new char[] {'\r'}));
                }
            }
        }
        return expectedResults;
    }

    /**
     * Consume an XML results File, produce results as JDOM and add results to the given parent.
     * <br>
     * @param resultsFile the XML file object that is to be parsed
     * @param parent the parent Element to assign results to
     * @return the modified parent
     * @exception JDOMException if there is an error consuming the message.
     */
    public Element parseXMLResultsFile(File resultsFile, Element parent) throws IOException, JDOMException {

        SAXBuilder builder = SAXBuilderHelper.createSAXBuilder(false);
        Document resultsDocument = builder.build(resultsFile);
        List resultElements = resultsDocument.getRootElement().getChildren(TagNames.Elements.QUERY_RESULTS);
        Iterator iter = resultElements.iterator();
        while ( iter.hasNext() ) {
            Element resultElement = (Element) iter.next();
            if ( resultElement.getChild(TagNames.Elements.SELECT) == null ) {
                // We've got an exception
                Element exceptionElement = resultElement.getChild(TagNames.Elements.EXCEPTION);
                if ( exceptionElement != null ) {
                    // ---------------------------------
                    // Add the ExceptionType element ...
                    // ---------------------------------
                    Element typeElement = new Element(TagNames.Elements.EXCEPTION_TYPE);
                    typeElement.setText(exceptionElement.getChild(TagNames.Elements.EXCEPTION_TYPE).getTextTrim());
                    parent.addContent(typeElement);

                    // ---------------------------
                    // Add the Message element ...
                    // ---------------------------
                    Element messageElement = new Element(TagNames.Elements.MESSAGE);    
                    String msg = exceptionElement.getChild(TagNames.Elements.MESSAGE).getTextTrim();   
                    
                    messageElement.setText(StringUtil.removeChars(msg, new char[] {'\r'}));
                    parent.addContent(messageElement);

                    // -------------------------
                    // Add the Class element ...
                    // -------------------------
                    Element classElement = new Element(TagNames.Elements.CLASS);
                    classElement.setText(exceptionElement.getChild(TagNames.Elements.CLASS).getTextTrim());
                    parent.addContent(classElement);
                }
            } else {
                // We've got results

                // -------------------------------
                // Read the SELECT elements
                // -------------------------------
                Element selectElement = resultElement.getChild(TagNames.Elements.SELECT);
                resultElement.removeChild(TagNames.Elements.SELECT);
                parent.addContent(selectElement);

                // -------------------------------
                // Read the TABLE of data
                // -------------------------------
                Element tableElement = resultElement.getChild(TagNames.Elements.TABLE);
                resultElement.removeChild(TagNames.Elements.TABLE);
                parent.addContent(tableElement);
            }
        }
        return parent;
    }

    /*********************************************************************************************
     *********************************************************************************************
     CONSUME METHODS
     *********************************************************************************************
     ********************************************************************************************/

    /**
     * Generate XML for an exception in Object form.
     *
     * @param ex
     * @param exceptionElement
     * @return The JDOM exception element.
     */
    public static Element jdomException(Throwable ex, Element exceptionElement) {
        // ---------------------------------
        // Add the ExceptionType element ...
        // ---------------------------------
        String className = ex.getClass().getName();
        int index = className.lastIndexOf('.');
        if ( index != -1 && (++index) < className.length() ) {
            className = className.substring(index);
        }
        Element typeElement = new Element(TagNames.Elements.EXCEPTION_TYPE);
        typeElement.setText(className);
        exceptionElement.addContent(typeElement);

        // ---------------------------
        // Add the Message element ...
        // ---------------------------
 
        Element messageElement = new Element(TagNames.Elements.MESSAGE);
        messageElement.setText(StringUtil.removeChars(ex.getMessage(), new char[] {'\r'}));     
        		
        exceptionElement.addContent(messageElement);

        // -------------------------
        // Add the Class element ...
        // -------------------------
        Element classElement = new Element(TagNames.Elements.CLASS);
        classElement.setText(ex.getClass().getName());
        exceptionElement.addContent(classElement);

        return exceptionElement;
    }

    /**
     * Consume an XML message and update the specified QueryResults instance.
     * <br>
     * @param object the instance that is to be updated with the XML message data.
     * @param resultsElement the XML element that contains the data
     * @return the updated instance.
     */
    private QueryResults consumeMsg(QueryResults object, Element resultsElement) throws JDOMException {
        // -----------------------
        // Process the element ...
        // -----------------------
        QueryResults results = object;
        if ( results == null ) {
            results = new QueryResults();
        }

        if ( resultsElement.getChild(TagNames.Elements.SELECT) == null ) {
            return results;
        }
        // -------------------------------
        // Read the SELECT elements
        // -------------------------------
        Element selectElement = resultsElement.getChild(TagNames.Elements.SELECT);
        Select select = new Select();
        select = consumeMsg(select, selectElement);

        List listOfElementSymbols = select.getSymbols();
        Iterator elementSymbolItr = listOfElementSymbols.iterator();
        Collection collectionOfColumnInfos = new ArrayList();
        while ( elementSymbolItr.hasNext() ) {
            ElementSymbol elementSymbol = (ElementSymbol) elementSymbolItr.next();
            Class elementType = elementSymbol.getType();
            String dataType = DataTypeManager.getDataTypeName(elementType);
            ColumnInfo columnInfo = new ColumnInfo(elementSymbol.getName(), dataType, elementType);
            collectionOfColumnInfos.add(columnInfo);
        }
        // Save column info
        results.addFields(collectionOfColumnInfos);
        // -------------------------------
        // Read the TABLE of data
        // -------------------------------

        Element tableElement = resultsElement.getChild(TagNames.Elements.TABLE);
        List tableRows = tableElement.getChildren(TagNames.Elements.TABLE_ROW);
        if ( tableRows.size() > 0 ) {
            Iterator rowIter = tableRows.iterator();

            while ( rowIter.hasNext() ) {
                Element rowElement = (Element) rowIter.next();
                List cellElements = rowElement.getChildren(TagNames.Elements.TABLE_CELL);
                Iterator cellIter = cellElements.iterator();
                // Read cells of the table
                ArrayList row = new ArrayList();
                Object evalue = null;
                while ( cellIter.hasNext() ) {
                    Element cellElement = (Element) cellIter.next();
                    if ( cellElement.getTextTrim().equalsIgnoreCase(TagNames.Elements.NULL) ) {
                        row.add(null);
                    } else {
                        Element cellChildElement = (Element) cellElement.getChildren().get(0);
                        evalue = consumeMsg(cellChildElement);
                        row.add(evalue);
                    }
                }
                // Save row
                results.addRecord(row);
            }
        }
        return results;
    }

    /**
     * Consume an XML message and update the specified Select instance.
     * <br>
     * @param object the instance that is to be updated with the XML message data.
     * @param selectElement the XML element that contains the data
     * @return the updated instance.
     * @exception JDOMException if there is an error consuming the message.
     */
    private Select consumeMsg(Select object, Element selectElement) throws JDOMException {

        Select select = (object != null) ? (Select) object : new Select();
        // --------------------------------
        // Read the DISTINCT attribute
        // --------------------------------

        String distinct = selectElement.getAttributeValue(TagNames.Attributes.DISTINCT);
        if ( distinct != null ) {
            if ( distinct.equalsIgnoreCase("true") ) { //$NON-NLS-1$
                select.setDistinct(true);
            }
        }

        // --------------------------------
        // Read the STAR attribute
        // --------------------------------

        String star = selectElement.getAttributeValue(TagNames.Attributes.STAR);
        if ( star != null ) {
            if ( star.equalsIgnoreCase("true") ) { //$NON-NLS-1$
                if ( selectElement.getChildren() != null ) {
                    throw new JDOMException("No children expected when star is chosen."); //$NON-NLS-1$
                }
                return select;
            }
        }

        // --------------------------------
        // Read the IDENTIFIER elements ...
        // --------------------------------
        List idents = selectElement.getChildren();
        Iterator identIter = idents.iterator();
        while ( identIter.hasNext() ) {
            Element dataElement = (Element) identIter.next();
            Attribute dataType = dataElement.getAttribute(TagNames.Attributes.TYPE);
            // add the dataType of the element to the list containing dataTypes
            ElementSymbol nodeID = new ElementSymbol(dataElement.getText());
            Class nodeType = (Class) TagNames.TYPE_MAP.get(dataType.getValue());
            if (nodeType == null)  {
                throw new JDOMException("Unknown class for type \"" + dataType.getValue() + "\"."); //$NON-NLS-1$ //$NON-NLS-2$
            }
            nodeID.setType(nodeType);
            select.addSymbol(nodeID);
        }

        return select;
    }


    /**
     * Produce a JDOM Element for the instance of any Object.
     * <br>
     * @param cellElement the XML element that is to produce the XML message.
     * @return the root element of the XML segment that was produced.
     * @exception JDOMException if there is an error producing XML.
     */
    private Object consumeMsg(Element cellElement) throws JDOMException {

        Object cellObject = null;;
        String cellName = cellElement.getName();

        if ( cellName.equalsIgnoreCase(TagNames.Elements.BOOLEAN) ) {
            cellObject = consumeMsg((Boolean) cellObject, cellElement);
        } else if ( cellName.equalsIgnoreCase(TagNames.Elements.STRING) ) {
            cellObject = consumeMsg((String) cellObject, cellElement);
        } else if ( cellName.equalsIgnoreCase(TagNames.Elements.CHAR) ) {
            cellObject = consumeMsg((Character) cellObject, cellElement);
        } else if ( cellName.equalsIgnoreCase(TagNames.Elements.BYTE) ) {
            cellObject = consumeMsg((Byte) cellObject, cellElement);
        } else if ( cellName.equalsIgnoreCase(TagNames.Elements.DOUBLE) ) {
            cellObject = consumeMsg((Double) cellObject, cellElement);
        } else if ( cellName.equalsIgnoreCase(TagNames.Elements.DATE) ) {
            cellObject = consumeMsg((java.sql.Date) cellObject, cellElement);
        } else if ( cellName.equalsIgnoreCase(TagNames.Elements.TIME) ) {
            cellObject = consumeMsg((Time) cellObject, cellElement);
        } else if ( cellName.equalsIgnoreCase(TagNames.Elements.TIMESTAMP) ) {
            cellObject = consumeMsg((Timestamp) cellObject, cellElement);
        } else if ( cellName.equalsIgnoreCase(TagNames.Elements.FLOAT) ) {
            cellObject = consumeMsg((Float) cellObject, cellElement);
        } else if ( cellName.equalsIgnoreCase(TagNames.Elements.BIGDECIMAL) ) {
            cellObject = consumeMsg((BigDecimal) cellObject, cellElement);
        } else if ( cellName.equalsIgnoreCase(TagNames.Elements.BIGINTEGER) ) {
            cellObject = consumeMsg((BigInteger) cellObject, cellElement);
        } else if ( cellName.equalsIgnoreCase(TagNames.Elements.INTEGER) ) {
            cellObject = consumeMsg((Integer) cellObject, cellElement);
        } else if ( cellName.equalsIgnoreCase(TagNames.Elements.LONG) ) {
            cellObject = consumeMsg((Long) cellObject, cellElement);
        } else if ( cellName.equalsIgnoreCase(TagNames.Elements.SHORT) ) {
            cellObject = consumeMsg((Short) cellObject, cellElement);
        } else if ( cellName.equalsIgnoreCase(TagNames.Elements.OBJECT) ) {
            cellObject = consumeMsg((String) cellObject, cellElement);
        } else {
        	cellObject = consumeMsg(cellObject, cellElement);
        }

        return cellObject;
    }

    /**
     * Consume an XML message and update the specified Boolean instance.
     * <br>
     * @param object the instance that is to be updated with the XML message data.
     * @param cellElement the XML element that contains the data
     * @return the updated instance.
     * @exception JDOMException if there is an error consuming the message.
     */
    private Object consumeMsg(Boolean object, Element cellElement) throws JDOMException {

        // -----------------------
        // Process the element ...
        // -----------------------
        boolean result = false;
        String value = cellElement.getTextTrim();
        if ( value.equalsIgnoreCase(TagNames.Values.TRUE) ) {
            result = true;
        } else if ( value.equalsIgnoreCase(TagNames.Values.FALSE) ) {
            result = false;
        } else {
            throw new JDOMException("Invalid value for " + cellElement.getName() + //$NON-NLS-1$
                                    " element: \"" + value + "\" must be either \"" + //$NON-NLS-1$ //$NON-NLS-2$
                                    TagNames.Values.TRUE + "\" or \"" + //$NON-NLS-1$
                                    TagNames.Values.FALSE + "\""); //$NON-NLS-1$
        }

        return new Boolean(result);
    }

    /**
     * Consume an XML message and update the specified java.sql.Date instance.
     * <br>
     * @param object the instance that is to be updated with the XML message data.
     * @param cellElement the XML element that contains the data
     * @return the updated instance.
     * @exception JDOMException if there is an error consuming the message.
     */
    private Object consumeMsg(java.sql.Date object, Element cellElement) throws JDOMException {

        // -----------------------
        // Process the element ...
        // -----------------------
        java.sql.Date result;
        try {
            result = java.sql.Date.valueOf(cellElement.getTextTrim());
        } catch ( Exception e ) {
            throw new JDOMException("Invalid input format ", e); //$NON-NLS-1$
        }
        return result;
    }

    /**
     * Consume an XML message and update the specified Time instance.
     * <br>
     * @param object the instance that is to be updated with the XML message data.
     * @param cellElement the XML element that contains the data
     * @return the updated instance.
     * @exception JDOMException if there is an error consuming the message.
     */
    private Object consumeMsg(Time object, Element cellElement) throws JDOMException {

        // -----------------------
        // Process the element ...
        // -----------------------
        Time result;
        try {
            result = Time.valueOf(cellElement.getTextTrim());
        } catch ( Exception e ) {
            throw new JDOMException("Invalid input format ", e); //$NON-NLS-1$
        }
        return result;
    }

    /**
     * Consume an XML message and update the specified Timestamp instance.
     * <br>
     * @param object the instance that is to be updated with the XML message data.
     * @param cellElement the XML element that contains the data
     * @return the updated instance.
     * @exception JDOMException if there is an error consuming the message.
     */
    private Object consumeMsg(Timestamp object, Element cellElement) throws JDOMException {

        // -----------------------
        // Process the element ...
        // -----------------------
        Timestamp result;
        try {
            result = Timestamp.valueOf(cellElement.getTextTrim());
        } catch ( Exception e ) {
            throw new JDOMException("Invalid input format ", e); //$NON-NLS-1$
        }

        return result;
    }

    /**
     * Consume an XML message and update the specified Double instance.
     * <br>
     * @param object the instance that is to be updated with the XML message data.
     * @param cellElement the XML element that contains the data
     * @return the updated instance.
     * @exception JDOMException if there is an error consuming the message.
     */
    private Object consumeMsg(Double object, Element cellElement) throws JDOMException {

        // -----------------------
        // Process the element ...
        // -----------------------
        String strElement = cellElement.getTextTrim();
        Double result;

        if ( strElement.equals("NaN") ) { //$NON-NLS-1$
            result = new Double(Double.NaN);
        } else if ( strElement.equals("-Infinity") ) { //$NON-NLS-1$
            result = new Double(Double.NEGATIVE_INFINITY);
        } else if ( strElement.equals("Infinity") ) { //$NON-NLS-1$
            result = new Double(Double.POSITIVE_INFINITY);
        } else {
            try {
                result = Double.valueOf(strElement);
            } catch ( NumberFormatException e ) {
                throw new JDOMException("Unable to parse the value for " + cellElement.getName() + //$NON-NLS-1$
                                        " element: " + strElement, e); //$NON-NLS-1$
            }
        }
        return result;
    }

    /**
     * Consume an XML message and update the specified Float instance.
     * <br>
     * @param object the instance that is to be updated with the XML message data.
     * @param cellElement the XML element that contains the data
     * @return the updated instance.
     * @exception JDOMException if there is an error consuming the message.
     */
    private Object consumeMsg(Float object, Element cellElement) throws JDOMException {

        // -----------------------
        // Process the element ...
        // -----------------------
        String strElement = cellElement.getTextTrim();
        Float result;

        if ( strElement.equals("NaN") ) { //$NON-NLS-1$
            result = new Float(Float.NaN);
        } else if ( strElement.equals("-Infinity") ) { //$NON-NLS-1$
            result = new Float(Float.NEGATIVE_INFINITY);
        } else if ( strElement.equals("Infinity") ) { //$NON-NLS-1$
            result = new Float(Float.POSITIVE_INFINITY);
        } else {
            try {
                result = Float.valueOf(strElement);
            } catch ( NumberFormatException e ) {
                throw new JDOMException("Unable to parse the value for " + cellElement.getName() + //$NON-NLS-1$
                                        " element: " + strElement, e); //$NON-NLS-1$
            }
        }
        return result;
    }

    /**
     * Consume an XML message and update the specified BigDecimal instance.
     * <br>
     * @param object the instance that is to be updated with the XML message data.
     * @param cellElement the XML element that contains the data
     * @return the updated instance.
     * @exception JDOMException if there is an error consuming the message.
     */
    private Object consumeMsg(BigDecimal object, Element cellElement) throws JDOMException {

        // -----------------------
        // Process the element ...
        // -----------------------
        BigDecimal result;
        try {
            result = new BigDecimal(cellElement.getTextTrim());
        } catch ( NumberFormatException e ) {
            throw new JDOMException("Unable to parse the value for " + cellElement.getName() + //$NON-NLS-1$
                                    " element: " + cellElement.getTextTrim(), e); //$NON-NLS-1$
        }
        return result;
    }

    /**
     * Consume an XML message and update the specified BigInteger instance.
     * <br>
     * @param object the instance that is to be updated with the XML message data.
     * @param cellElement the XML element that contains the data
     * @return the updated instance.
     * @exception JDOMException if there is an error consuming the message.
     */
    private Object consumeMsg(BigInteger object, Element cellElement) throws JDOMException {

        // -----------------------
        // Process the element ...
        // -----------------------
        BigInteger result;
        try {
            result = new BigInteger(cellElement.getTextTrim());
        } catch ( NumberFormatException e ) {
            throw new JDOMException("Unable to parse the value for " + cellElement.getName() + //$NON-NLS-1$
                                    " element: " + cellElement.getTextTrim(), e); //$NON-NLS-1$
        }
        return result;
    }

    /**
     * Consume an XML message and update the specified String instance.
     * <br>
     * @param object the instance that is to be updated with the XML message data.
     * @param cellElement the XML element that contains the data
     * @return the updated instance.
     * @exception JDOMException if there is an error consuming the message.
     */
    private Object consumeMsg(String object, Element cellElement) throws JDOMException {

        // -----------------------
        // Process the element ...
        // -----------------------

        return cellElement.getText();
    }

    /**
     * Consume an XML message and update the specified Character instance.
     * <br>
     * @param object the instance that is to be updated with the XML message data.
     * @param cellElement the XML element that contains the data
     * @return the updated instance.
     * @exception JDOMException if there is an error consuming the message.
     */
    private Object consumeMsg(Character object, Element cellElement) throws JDOMException {

        // -----------------------
        // Process the element ...
        // -----------------------
        Character result;
        try {
            if ( cellElement.getTextTrim().length() == 0 ) {
                return null;
            }
            result = new Character(cellElement.getTextTrim().charAt(0));
        } catch ( NumberFormatException e ) {
            throw new JDOMException("Unable to parse the value for " + cellElement.getName() + //$NON-NLS-1$
                                    " element: " + cellElement.getTextTrim(), e); //$NON-NLS-1$
        }
        return result;
    }

    /**
     * Consume an XML message and update the specified Byte instance.
     * <br>
     * @param object the instance that is to be updated with the XML message data.
     * @param cellElement the XML element that contains the data
     * @return the updated instance.
     * @exception JDOMException if there is an error consuming the message.
     */
    private Object consumeMsg(Byte object, Element cellElement) throws JDOMException {

        // -----------------------
        // Process the element ...
        // -----------------------
        Byte result;
        try {
            result = new Byte(cellElement.getTextTrim());
        } catch ( NumberFormatException e ) {
            throw new JDOMException("Unable to parse the value for " + cellElement.getName() + //$NON-NLS-1$
                                    " element: " + cellElement.getTextTrim(), e); //$NON-NLS-1$
        }
        return result;
    }

    /**
     * Consume an XML message and update the specified Integer instance.
     * <br>
     * @param object the instance that is to be updated with the XML message data.
     * @param cellElement the XML element that contains the data
     * @return the updated instance.
     * @exception JDOMException if there is an error consuming the message.
     */
    private Object consumeMsg(Integer object, Element cellElement) throws JDOMException {

        // -----------------------
        // Process the element ...
        // -----------------------
        Integer result;
        try {
            result = Integer.valueOf(cellElement.getTextTrim());
        } catch ( NumberFormatException e ) {
            throw new JDOMException("Unable to parse the value for " + cellElement.getName() + //$NON-NLS-1$
                                    " element: " + cellElement.getTextTrim(), e); //$NON-NLS-2$ //$NON-NLS-1$ //$NON-NLS-1$ //$NON-NLS-1$ //$NON-NLS-1$
        }
        return result;
    }

    /**
     * Consume an XML message and update the specified Long instance.
     * <br>
     * @param object the instance that is to be updated with the XML message data.
     * @param cellElement the XML element that contains the data
     * @return the updated instance.
     * @exception JDOMException if there is an error consuming the message.
     */
    private Object consumeMsg(Long object, Element cellElement) throws JDOMException {

        // -----------------------
        // Process the element ...
        // -----------------------
        Long result;
        try {
            result = Long.valueOf(cellElement.getTextTrim());
        } catch ( NumberFormatException e ) {
            throw new JDOMException("Unable to parse the value for " + cellElement.getName() + //$NON-NLS-1$
                                    " element: " + cellElement.getTextTrim(), e); //$NON-NLS-2$ //$NON-NLS-1$ //$NON-NLS-1$ //$NON-NLS-1$ //$NON-NLS-1$
        }
        return result;
    }
    
    
    /**
     * Consume an XML message and update the specified Byte instance.
     * <br>
     * @param object the instance that is to be updated with the XML message data.
     * @param cellElement the XML element that contains the data
     * @return the updated instance.
     * @exception JDOMException if there is an error consuming the message.
     */
    @SuppressWarnings("unused")
	private Object consumeMsg(Object object, Element cellElement) throws JDOMException {

//        // -----------------------
//        // Process the element ...
//        // -----------------------
//        Byte result;
//        try {
//            result = new Byte(cellElement.getTextTrim());
//        } catch ( NumberFormatException e ) {
//            throw new JDOMException("Unable to parse the value for " + cellElement.getName() + //$NON-NLS-1$
//                                    " element: " + cellElement.getTextTrim(), e); //$NON-NLS-1$
//        }
//        return result;
        
    	return cellElement.getText();
        // ----------------------
        // Create the Object element ...
        // ----------------------
//        Element objectElement = new Element(TagNames.Elements.OBJECT);
//        
//        String result = null;
//        if (object instanceof Blob || object instanceof Clob || object instanceof SQLXML) {
//       	 
//        	if (object instanceof Clob){
//        		Clob c = (Clob)object;
//        		try {
//        			result = ObjectConverterUtil.convertToString(c.getAsciiStream());
//					
//				} catch (Throwable e) {
//					// TODO Auto-generated catch block
//					throw new SQLException(e);
//				}
//        	} else if (object instanceof Blob){
//            		Blob b = (Blob)object;
//            		try {
//            			result = ObjectConverterUtil.convertToString(b.getBinaryStream());
//						
//					} catch (Throwable e) {
//						// TODO Auto-generated catch block
//						throw new SQLException(e);
//					}
//            } else if (object instanceof SQLXML){
//            	SQLXML s = (SQLXML)object;
//        		try {
//        			result = ObjectConverterUtil.convertToString(s.getBinaryStream());
//					
//				} catch (Throwable e) {
//					// TODO Auto-generated catch block
//					throw new SQLException(e);
//				}
//            } 
//        } else {
//        	result = object.toString();
//        }
//        
// //       System.out.println("ProductObject (before encoding): " + object.toString() );
// //       try {
//            objectElement.setText(result);
//            	//	URLEncoder.encode(object.toString(), "UTF-8"));
// //       } catch (UnsupportedEncodingException e) {
//            // UTF-8 is supported natively by all jvms
// //       }
////        System.out.println("ProductObject (after encoding): " + objectElement.getText() );
//
//        
//        if ( parent != null ) {
//            objectElement = parent.addContent(objectElement);
//        }
//
//        return objectElement;

    }

    /**
     * Consume an XML message and update the specified Long instance.
     * <br>
     * @param object the instance that is to be updated with the XML message data.
     * @param cellElement the XML element that contains the data
     * @return the updated instance.
     * @exception JDOMException if there is an error consuming the message.
     */
    private Object consumeMsg(Short object, Element cellElement) throws JDOMException {

        // -----------------------
        // Process the element ...
        // -----------------------
        Short result;
        try {
            result = Short.valueOf(cellElement.getTextTrim());
        } catch ( NumberFormatException e ) {
            throw new JDOMException("Unable to parse the value for " + cellElement.getName() + //$NON-NLS-1$
                                    " element: " + cellElement.getTextTrim(), e); //$NON-NLS-2$ //$NON-NLS-1$ //$NON-NLS-1$ //$NON-NLS-1$ //$NON-NLS-1$
        }
        return result;
    }

    /*********************************************************************************************
     *********************************************************************************************
     PRODUCE METHODS
     *********************************************************************************************
     ********************************************************************************************/

    /**
     * Produce a JDOM Element for an instance of a JDBC ResultSet object.
     * <br>
     * @param object for which the JDOM Element is to be produced.
     * @return the JDOM element of the ResultSet object that was converted to XML.
     * @exception JDOMException if there is an error producing XML.
     * @exception JDOMException if there is an error producing XML.
     * @exception SQLException if there is an error walking through the ResultSet object.
     */
    public Element produceResults(ResultSet object) throws JDOMException, SQLException {

        // When no begin and end
        return produceResults(object, START_ROW, Integer.MAX_VALUE);
    }

    /**
     * Produce a JDOM Element for an instance of Results object.
     * <br>
     * @param object for which the JDOM Element is to be produced.
     * @param beginRow The starting row from which the results are to be converted to XML.
     * @param endRow The row until which the results are to be converted to XML.
     * @return the JDOM element of the results object that was converted to XML.
     * @exception JDOMException if there is an error producing XML.
     * @exception SQLException if there is an error walking through the ResultSet object.
     */
    private Element produceResults(ResultSet object, int beginRow, int endRow)
            throws JDOMException, SQLException {

    	if (object.isClosed()) {
            throw new SQLException(
            "ResultSet is closed at this point, unable to product results"); //$NON-NLS-1$
    		
    	}
    	
        if ( beginRow < START_ROW ) {
            throw new IllegalArgumentException(
                    "The starting row cannot be less than 1."); //$NON-NLS-1$
        } else if ( beginRow > endRow ) {
            throw new IllegalArgumentException(
                    "The starting row cannot be less than the ending row."); //$NON-NLS-1$
        }

        int currentRow = object.getRow() + 1;

        if ( beginRow > currentRow ) {
            while ( !object.isLast() && currentRow != beginRow ) {
                object.next();
                currentRow++;
            }

        } else if ( beginRow < currentRow ) {
            while ( !object.isFirst() && currentRow != beginRow ) {
                object.previous();
                currentRow--;
            }
        }

        return produceMsg(object, endRow);
    }

    /**
     * Produce a JDOM Element for an instance of a JDBC ResultSet object.
     * <br>
     * @param object for which the JDOM Element is to be produced.
     * @param endRow The row until which the results are to be converted to XML.
     * @return the JDOM element of the results object that was converted to XML.
     * @exception JDOMException if there is an error producing XML.
     * @exception SQLException if there is an error walking through the ResultSet object.
     */
    private Element produceMsg(ResultSet object, int endRow) throws JDOMException, SQLException {

        // -----------------------------------
        // Create the QueryResults element ...
        // -----------------------------------
        Element resultsElement = new Element(TagNames.Elements.QUERY_RESULTS);

        // -----------------------------------
        // Add the Select (header) element ...
        // -----------------------------------
        try {
            ResultSetMetaData rmdata = object.getMetaData();
            List identList = new ArrayList(rmdata.getColumnCount());
            for ( int i = 1; i <= rmdata.getColumnCount(); i++ ) {
                identList.add(new ElementSymbol(rmdata.getColumnName(i)));
            }
            Select select = new Select(identList);
            resultsElement = produceMsg(select, rmdata, resultsElement);

            // -------------------------
            // Add the Table element ...
            // -------------------------
            resultsElement.addContent(new Element(TagNames.Elements.TABLE));
            Element tableElement = resultsElement.getChild(TagNames.Elements.TABLE);
            int rowCount = 0;
            int colCount = rmdata.getColumnCount();

            while ( object.next() && (object.getRow() <= endRow) ) {

                // -------------------------
                // Add the ROW element ...
                // -------------------------
                Element rowElement = new Element(TagNames.Elements.TABLE_ROW);

                for ( int i = 1; i <= colCount; i++ ) {
                    // -------------------------
                    // Add the Cell element ...
                    // -------------------------
                    Element cellElement = new Element(TagNames.Elements.TABLE_CELL);
                    Object cellValue = object.getObject(i);
                    if ( cellValue != null ) {
                        cellElement = produceMsg(cellValue, cellElement);
                    } else {
                        cellElement = cellElement.addContent(TagNames.Elements.NULL);
                    }
                    rowElement.addContent(cellElement);
                }
                tableElement.addContent(rowElement);
                rowCount++;
            }
            Attribute rowCountAttribute = new Attribute(TagNames.Attributes.TABLE_ROW_COUNT,
                                                        Integer.toString(rowCount));
            Attribute columnCountAttribute = new Attribute(TagNames.Attributes.TABLE_COLUMN_COUNT,
                                                           Integer.toString(colCount));
            tableElement.setAttribute(rowCountAttribute);
            tableElement.setAttribute(columnCountAttribute);
        } catch ( SQLException e ) {
            // error while reading results
            throw(e);
        }

        return resultsElement;
    }

    /**
     * Produce a JDOM Element for an instance of a JDBC ResultSet object.
     * <br>
     * @param object for which the JDOM Element is to be produced.
     * @return the JDOM element of the results object that was converted to XML.
     * @exception JDOMException if there is an error producing XML.
     * @exception SQLException if there is an error walking through the ResultSet object.
     */
    public Element produceMsg(ResultSet object, Element resultsElement) throws JDOMException, SQLException {

        // -----------------------------------
        // Add the Select (header) element ...
        // -----------------------------------
        try {
            ResultSetMetaData rmdata = object.getMetaData();
            List identList = new ArrayList(rmdata.getColumnCount());
            for ( int i = 1; i <= rmdata.getColumnCount(); i++ ) {
                identList.add(new ElementSymbol(rmdata.getColumnName(i)));
            }
            Select select = new Select(identList);
            resultsElement = produceMsg(select, rmdata, resultsElement);

            // -------------------------
            // Add the Table element ...
            // -------------------------
            resultsElement.addContent(new Element(TagNames.Elements.TABLE));
            Element tableElement = resultsElement.getChild(TagNames.Elements.TABLE);
            int rowCount = 0;
            int colCount = rmdata.getColumnCount();

            while ( object.next() ) {

                // -------------------------
                // Add the ROW element ...
                // -------------------------
                Element rowElement = new Element(TagNames.Elements.TABLE_ROW);

                for ( int i = 1; i <= colCount; i++ ) {
                    // -------------------------
                    // Add the Cell element ...
                    // -------------------------
                    Element cellElement = new Element(TagNames.Elements.TABLE_CELL);
                    Object cellValue = object.getObject(i);
                    if ( cellValue != null ) {
                        cellElement = produceMsg(cellValue, cellElement);
                    } else {
                        cellElement = cellElement.addContent(TagNames.Elements.NULL);
                    }
                    rowElement.addContent(cellElement);
                }
                tableElement.addContent(rowElement);
                rowCount++;
            }
            Attribute rowCountAttribute = new Attribute(TagNames.Attributes.TABLE_ROW_COUNT,
                                                        Integer.toString(rowCount));
            Attribute columnCountAttribute = new Attribute(TagNames.Attributes.TABLE_COLUMN_COUNT,
                                                           Integer.toString(colCount));
            tableElement.setAttribute(rowCountAttribute);
            tableElement.setAttribute(columnCountAttribute);
        } catch ( SQLException e ) {
            // error while reading results
            throw(e);
        }

        return resultsElement;
    }

    /**
     * Produce a JDOM Element for the instance of any Object.
     * <br>
     * @param object the instance for which the message is to be produced.
     * @param parent the XML element that is to be the parent of the produced XML message.
     * @return the root element of the XML segment that was produced.
     * @exception JDOMException if there is an error producing XML.
     */
    public Element produceMsg(Object object, Element parent) throws JDOMException, SQLException {
        if ( object == null ) {
            throw new IllegalArgumentException("Null object reference."); //$NON-NLS-1$
        }
        Element element = null;

        if ( object instanceof Boolean ) {
            element = produceMsg((Boolean) object, parent);
        } else if ( object instanceof String ) {
            element = produceMsg((String) object, parent);
        } else if ( object instanceof Character ) {
            element = produceMsg((Character) object, parent);
        } else if ( object instanceof Byte ) {
            element = produceMsg((Byte) object, parent);
        } else if ( object instanceof Double ) {
            element = produceMsg((Double) object, parent);
        } else if ( object instanceof java.sql.Date ) {
            element = produceMsg((java.sql.Date) object, parent);
        } else if ( object instanceof Time ) {
            element = produceMsg((Time) object, parent);
        } else if ( object instanceof Timestamp ) {
            element = produceMsg((Timestamp) object, parent);
        } else if ( object instanceof Float ) {
            element = produceMsg((Float) object, parent);
        } else if ( object instanceof BigDecimal ) {
            element = produceMsg((BigDecimal) object, parent);
        } else if ( object instanceof BigInteger ) {
            element = produceMsg((BigInteger) object, parent);
        } else if ( object instanceof Integer ) {
            element = produceMsg((Integer) object, parent);
        } else if ( object instanceof Long ) {
            element = produceMsg((Long) object, parent);
        } else if ( object instanceof Short ) {
            element = produceMsg((Short) object, parent);
        } else if ( object instanceof Throwable ) {
            element = produceMsg((Throwable) object, parent);
        } else {
            element = produceObject(object, parent);
        }

        return element;
    }

    /**
     * new ----
     * @param select
     * @param rmdata
     * @param parent
     * @return
     * @throws JDOMException
     */
    private Element produceMsg(Select select, ResultSetMetaData rmdata, Element parent)
            throws JDOMException {

        // -----------------------------------
        // Create the Select element ...
        // -----------------------------------

        Element selectElement = new Element(TagNames.Elements.SELECT);

        // ---------------------------------
        // Create the DISTINCT attribute ...
        // ---------------------------------
        boolean distinct = select.isDistinct();
        if ( distinct ) {
            Attribute distinctAttribute = new Attribute(TagNames.Attributes.DISTINCT, "true"); //$NON-NLS-1$
            selectElement.setAttribute(distinctAttribute);
        } // else default is false so no need

        // ----------------------------------
        // Create the STAR attribute ...
        // ----------------------------------
        if ( select.isStar() ) {
            Attribute starAttribute = new Attribute(TagNames.Attributes.STAR, "true"); //$NON-NLS-1$
            selectElement.setAttribute(starAttribute);
        }

        // --------------------------------
        // Create the DATANODE elements ...
        // --------------------------------
        int col = 0;
        Iterator iter = select.getSymbols().iterator();
        while ( iter.hasNext() ) {
            Element dataElement = new Element(TagNames.Elements.DATA_ELEMENT);
            ElementSymbol symbol = (ElementSymbol) iter.next();
            String elementName = symbol.getName();
            Attribute dataType = null;
            try {
                dataType = new Attribute(TagNames.Attributes.TYPE, rmdata.getColumnTypeName(++col));
            } catch ( SQLException e ) {
                //
            }
            dataElement.setAttribute(dataType);
            dataElement.setText(elementName);
            selectElement.addContent(dataElement);
        }
        if ( parent != null ) {
            selectElement = parent.addContent(selectElement);
        }

        return selectElement;
    }

    /**
     * Produce an XML message for an instance of the Object.
     * <br>
     * @param object the instance for which the message is to be produced.
     * @param parent the XML element that is to be the parent of the produced XML message.
     * @return the root element of the XML segment that was produced.
     * @exception JDOMException if there is an error producing the message.
     */
    private Element produceObject(Object object, Element parent) throws JDOMException, SQLException {

         // ----------------------
        // Create the Object element ...
        // ----------------------
        Element objectElement = new Element(TagNames.Elements.OBJECT);
        
        String result = null;
        if (object instanceof Blob || object instanceof Clob || object instanceof SQLXML) {
       	 
        	if (object instanceof Clob){
        		Clob c = (Clob)object;
        		try {
        			result = ObjectConverterUtil.convertToString(c.getAsciiStream());
					
				} catch (Throwable e) {
					// TODO Auto-generated catch block
					throw new SQLException(e);
				}
        	} else if (object instanceof Blob){
            		Blob b = (Blob)object;
            		try {
            			result = ObjectConverterUtil.convertToString(b.getBinaryStream());
						
					} catch (Throwable e) {
						// TODO Auto-generated catch block
						throw new SQLException(e);
					}
            } else if (object instanceof SQLXML){
            	SQLXML s = (SQLXML)object;
        		try {
        			result = ObjectConverterUtil.convertToString(s.getBinaryStream());
					
				} catch (Throwable e) {
					// TODO Auto-generated catch block
					throw new SQLException(e);
				}
            } 
        } else {
        	result = object.toString();
        }
        
         objectElement.setText(result);

        
        if ( parent != null ) {
            objectElement = parent.addContent(objectElement);
        }

        return objectElement;
    }

    /**
     * Produce an XML message for an instance of the String.
     * <br>
     * @param object the instance for which the message is to be produced.
     * @param parent the XML element that is to be the parent of the produced XML message.
     * @return the root element of the XML segment that was produced.
     * @exception JDOMException if there is an error producing the message.
     */
    private Element produceMsg(String object, Element parent) throws JDOMException {

        // ----------------------
        // Create the String element ...
        // ----------------------
        Element stringElement = new Element(TagNames.Elements.STRING);
        stringElement.setText(object);
        if ( parent != null ) {
            stringElement = parent.addContent(stringElement);
        }

        return stringElement;
    }

    /**
     * Produce an XML message for an instance of the Character.
     * <br>
     * @param object the instance for which the message is to be produced.
     * @param parent the XML element that is to be the parent of the produced XML message.
     * @return the root element of the XML segment that was produced.
     * @exception JDOMException if there is an error producing the message.
     */
    private Element produceMsg(Character object, Element parent) throws JDOMException {

        // ----------------------
        // Create the Character element ...
        // ----------------------
        Element charElement = new Element(TagNames.Elements.CHAR);
               
        String v = object.toString();
        if (v != null && v.length() != 0) {
            
	    String toReplace = new String( new Character( (char)0x0).toString() );
	    v.replaceAll(toReplace," ");
	    charElement.setText(v.trim());

        }
        if ( parent != null ) {
            charElement = parent.addContent(charElement);
        }


        return charElement;
    }

    /**
     * Produce an XML message for an instance of the Byte.
     * <br>
     * @param object the instance for which the message is to be produced.
     * @param parent the XML element that is to be the parent of the produced XML message.
     * @return the root element of the XML segment that was produced.
     * @exception JDOMException if there is an error producing the message.
     */
    private Element produceMsg(Byte object, Element parent) throws JDOMException {

        // ----------------------
        // Create the Byte element ...
        // ----------------------
        Element byteElement = new Element(TagNames.Elements.BYTE);
        byteElement.setText(object.toString());
        if ( parent != null ) {
            byteElement = parent.addContent(byteElement);
        }

        return byteElement;
    }

    /**
     * Produce an XML message for an instance of the Boolean.
     * <br>
     * @param object the instance for which the message is to be produced.
     * @param parent the XML element that is to be the parent of the produced XML message.
     * @return the root element of the XML segment that was produced.
     * @exception JDOMException if there is an error producing the message.
     */
    private Element produceMsg(Boolean object, Element parent) throws JDOMException {

        // ----------------------
        // Create the Boolean element ...
        // ----------------------
        Element booleanElement = new Element(TagNames.Elements.BOOLEAN);

        if ( object.booleanValue() == true ) {
            booleanElement.setText(TagNames.Values.TRUE);
        } else {
            booleanElement.setText(TagNames.Values.FALSE);
        }

        if ( parent != null ) {
            booleanElement = parent.addContent(booleanElement);
        }

        return booleanElement;
    }

    /**
     * Produce an XML message for an instance of the Float.
     * <br>
     * @param object the instance for which the message is to be produced.
     * @param parent the XML element that is to be the parent of the produced XML message.
     * @return the root element of the XML segment that was produced.
     * @exception JDOMException if there is an error producing the message.
     */
    private Element produceMsg(Float object, Element parent) throws JDOMException {

        // ----------------------
        // Create the Float element ...
        // ----------------------
        Element floatElement = new Element(TagNames.Elements.FLOAT);
        floatElement.setText(object.toString());
        if ( parent != null ) {
            floatElement = parent.addContent(floatElement);
        }

        return floatElement;
    }

    /**
     * Produce an XML message for an instance of the Double.
     * <br>
     * @param object the instance for which the message is to be produced.
     * @param parent the XML element that is to be the parent of the produced XML message.
     * @return the root element of the XML segment that was produced.
     * @exception JDOMException if there is an error producing the message.
     */
    private Element produceMsg(Double object, Element parent) throws JDOMException {

        // ----------------------
        // Create the Double element ...
        // ----------------------
        Element doubleElement = new Element(TagNames.Elements.DOUBLE);
        doubleElement.setText(object.toString());
        if ( parent != null ) {
            doubleElement = parent.addContent(doubleElement);
        }

        return doubleElement;
    }

    /**
     * Produce an XML message for an instance of the BigDecimal.
     * <br>
     * @param object the instance for which the message is to be produced.
     * @param parent the XML element that is to be the parent of the produced XML message.
     * @return the root element of the XML segment that was produced.
     * @exception JDOMException if there is an error producing the message.
     */
    private Element produceMsg(BigDecimal object, Element parent) throws JDOMException {

        // ----------------------
        // Create the BigDecimal element ...
        // ----------------------
        Element bigDecimalElement = new Element(TagNames.Elements.BIGDECIMAL);
        bigDecimalElement.setText(object.toString());
        if ( parent != null ) {
            bigDecimalElement = parent.addContent(bigDecimalElement);
        }

        return bigDecimalElement;
    }

    /**
     * Produce an XML message for an instance of the BigInteger.
     * <br>
     * @param object the instance for which the message is to be produced.
     * @param parent the XML element that is to be the parent of the produced XML message.
     * @return the root element of the XML segment that was produced.
     * @exception JDOMException if there is an error producing the message.
     */
    private Element produceMsg(BigInteger object, Element parent) throws JDOMException {

        // ----------------------
        // Create the BigInteger element ...
        // ----------------------
        Element bigIntegerElement = new Element(TagNames.Elements.BIGINTEGER);
        bigIntegerElement.setText(object.toString());
        if ( parent != null ) {
            bigIntegerElement = parent.addContent(bigIntegerElement);
        }

        return bigIntegerElement;
    }

    /**
     * Produce an XML message for an instance of the java.sql.Date.
     * <br>
     * @param object the instance for which the message is to be produced.
     * @param parent the XML element that is to be the parent of the produced XML message.
     * @return the root element of the XML segment that was produced.
     * @exception JDOMException if there is an error producing the message.
     */
    private Element produceMsg(java.sql.Date object, Element parent) throws JDOMException {

        // ----------------------
        // Create the java.sql.Date element ...
        // ----------------------
        Element sqldateElement = new Element(TagNames.Elements.DATE);
        sqldateElement.setText(object.toString());
        if ( parent != null ) {
            sqldateElement = parent.addContent(sqldateElement);
        }

        return sqldateElement;
    }

    /**
     * Produce an XML message for an instance of the Time.
     * <br>
     * @param object the instance for which the message is to be produced.
     * @param parent the XML element that is to be the parent of the produced XML message.
     * @return the root element of the XML segment that was produced.
     * @exception JDOMException if there is an error producing the message.
     */
    private Element produceMsg(Time object, Element parent) throws JDOMException {

        // ----------------------
        // Create the Time element ...
        // ----------------------
        Element timeElement = new Element(TagNames.Elements.TIME);
        timeElement.setText(object.toString());
        if ( parent != null ) {
            timeElement = parent.addContent(timeElement);
        }

        return timeElement;
    }

    /**
     * Produce an XML message for an instance of the Timestamp.
     * <br>
     * @param object the instance for which the message is to be produced.
     * @param parent the XML element that is to be the parent of the produced XML message.
     * @return the root element of the XML segment that was produced.
     * @exception JDOMException if there is an error producing the message.
     */
    private Element produceMsg(Timestamp object, Element parent) throws JDOMException {

        // ----------------------
        // Create the Timestamp element ...
        // ----------------------
        Element timestampElement = new Element(TagNames.Elements.TIMESTAMP);
        timestampElement.setText(object.toString());
        if ( parent != null ) {
            timestampElement = parent.addContent(timestampElement);
        }

        return timestampElement;
    }

    /**
     * Produce an XML message for an instance of the Integer.
     * <br>
     * @param object the instance for which the message is to be produced.
     * @param parent the XML element that is to be the parent of the produced XML message.
     * @return the root element of the XML segment that was produced.
     * @exception JDOMException if there is an error producing the message.
     */
    private Element produceMsg(Integer object, Element parent) throws JDOMException {

        // ----------------------
        // Create the Integer element ...
        // ----------------------
        Element integerElement = new Element(TagNames.Elements.INTEGER);
        integerElement.setText(object.toString());
        if ( parent != null ) {
            integerElement = parent.addContent(integerElement);
        }

        return integerElement;
    }

    /**
     * Produce an XML message for an instance of the Long.
     * <br>
     * @param object the instance for which the message is to be produced.
     * @param parent the XML element that is to be the parent of the produced XML message.
     * @return the root element of the XML segment that was produced.
     * @exception JDOMException if there is an error producing the message.
     */
    private Element produceMsg(Long object, Element parent) throws JDOMException {

        // ----------------------
        // Create the Long element ...
        // ----------------------
        Element longElement = new Element(TagNames.Elements.LONG);
        longElement.setText(object.toString());
        if ( parent != null ) {
            longElement = parent.addContent(longElement);
        }

        return longElement;
    }

    /**
     * Produce an XML message for an instance of the Short.
     * <br>
     * @param object the instance for which the message is to be produced.
     * @param parent the XML element that is to be the parent of the produced XML message.
     * @return the root element of the XML segment that was produced.
     * @exception JDOMException if there is an error producing the message.
     */
    private Element produceMsg(Short object, Element parent) throws JDOMException {

        // ----------------------
        // Create the Long element ...
        // ----------------------
        Element shortElement = new Element(TagNames.Elements.SHORT);
        shortElement.setText(object.toString());
        if ( parent != null ) {
            shortElement = parent.addContent(shortElement);
        }

        return shortElement;
    }

    /**
     * Produce an XML message for an instance of the SQLException.
     * <br>
     * @param object the instance for which the message is to be produced.
     * @param parent the XML element that is to be the parent of the produced XML message.
     * @return the root element of the XML segment that was produced.
     * @exception JDOMException if there is an error producing the message.
     */
    private Element produceMsg(Throwable object, Element parent) throws JDOMException {

        Throwable exception = object;
        Element exceptionElement = null;

        // --------------------------------
        // Create the Exception element ...
        // --------------------------------
        exceptionElement = new Element(TagNames.Elements.EXCEPTION);

        // ---------------------------------
        // Add the ExceptionType element ...
        // ---------------------------------
        String className = exception.getClass().getName();
        int index = className.lastIndexOf('.');
        if ( index != -1 && (++index) < className.length() ) {
            className = className.substring(index);
        }
        Element typeElement = new Element(TagNames.Elements.EXCEPTION_TYPE);
        typeElement.setText(className);
        exceptionElement.addContent(typeElement);

        // ---------------------------
        // Add the Message element ...
        // ---------------------------
        Element messageElement = new Element(TagNames.Elements.MESSAGE);
        messageElement.setText(StringUtil.removeChars(exception.getMessage(), new char[] {'\r'}));
         
        exceptionElement.addContent(messageElement);

        // -------------------------
        // Add the Class element ...
        // -------------------------
        Element classElement = new Element(TagNames.Elements.CLASS);
        classElement.setText(exception.getClass().getName());
        exceptionElement.addContent(classElement);

        if ( parent != null ) {
            exceptionElement = parent.addContent(exceptionElement);
        }

        return exceptionElement;
    }
}
