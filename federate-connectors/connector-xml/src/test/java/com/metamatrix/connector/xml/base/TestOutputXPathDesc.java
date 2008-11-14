/*
 * © 2007 Varsity Gateway LLC. All Rights Reserved.
 */

package com.metamatrix.connector.xml.base;

import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.ICompareCriteria;
import com.metamatrix.data.language.ICriteria;
import com.metamatrix.data.language.IElement;
import com.metamatrix.data.language.IExpression;
import com.metamatrix.data.language.ILiteral;
import com.metamatrix.data.language.IQuery;
import com.metamatrix.data.language.ISelect;
import com.metamatrix.data.language.ISelectSymbol;
import com.metamatrix.data.language.LanguageUtil;
import com.metamatrix.data.metadata.runtime.Element;
import com.metamatrix.data.metadata.runtime.MetadataID;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;

/**
 * created by JChoate on Jun 27, 2005
 *
 */
public class TestOutputXPathDesc extends TestCase {

    
	private static String vdbPath;
    private static final String QUERY = "select OutputColumn from CriteriaDescTable where"
    		+ " OutputColumn in ('foo') order by OutputColumn";
   
    static {
    	vdbPath = ProxyObjectFactory.getStateCollegeVDBLocation();
    }
    
//  removing hansel while testing clover    
    
/*    
    public static Test suite() {
    	return new CoverageDecorator(OutputXPathDescTest.class, new Class[] {OutputXPathDesc.class});    	
    }
*/    
    
    /**
     * Constructor for OutputXPathDescTest.
     * @param arg0
     */
    public TestOutputXPathDesc(String arg0) {
        super(arg0);
    }

    /*
     * Class under test for void OutputXPathDesc(Element)
     */
    public void testOutputXPathDescElement() {
               
        IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        ISelect select = query.getSelect();
        List symbols = select.getSelectSymbols();
        ISelectSymbol selectSymbol = (ISelectSymbol) symbols.get(0);
        IExpression expr = selectSymbol.getExpression();
        Element element = null;
        try {
            if (expr instanceof IElement) {
                MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();    
                element = (Element) metadata.getObject(elementID); 
            } else {
                fail("select symbols is not an element");
            }        
            OutputXPathDesc desc = new OutputXPathDesc(element);
            assertNull(desc.getCurrentValue());
            assertNotNull(desc.getDataType());;
        } catch (ConnectorException ex) {
            ex.printStackTrace();
            fail(ex.getMessage());
        }
        
    }
    
    public void testOutputXPathDescParam() {
        try {
        	String query = "select RequiredDefaultedParam from CriteriaDescTable where RequiredDefaultedParam in ('foo')";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
            OutputXPathDesc desc = new OutputXPathDesc(elem);
            assertNotNull("OutputXPathDesc is null", desc);
        } catch (ConnectorException ce) {
            ce.printStackTrace();
            fail(ce.getMessage());
        }
    }

    
    public void testOutputXPathDescNoXPath() {
        try {
        	String query = "select OutputColumnNoXPath from CriteriaDescTable";
        	IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
        	final int colLocation = 0;
        	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
        	IExpression expr = symbol.getExpression();
        	RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
        	Element elem = (Element) metadata.getObject(elementID);
            OutputXPathDesc desc = new OutputXPathDesc(elem);
            fail("should not be able to create OuputXPathDesc with no XPath");
        } catch (ConnectorException ce) {
        	assertNotNull(ce);
        }
    }
    /*
     * Class under test for void OutputXPathDesc(ILiteral)
     */
    public void testOutputXPathDescILiteral() { 
    	String strLiteral = "MetaMatrix";
    	String strQuery = "Select Company_id from Company where Company_id = '" + strLiteral + "'";
    	IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, strQuery);
    	ICriteria crits = query.getWhere();
    	List criteriaList = LanguageUtil.separateCriteriaByAnd(crits);
        ICompareCriteria compCriteria = (ICompareCriteria) criteriaList.get(0);                  	
        ILiteral literal = (ILiteral) compCriteria.getRightExpression();
        try {
        	OutputXPathDesc desc = new OutputXPathDesc(literal);
        	assertNotNull(desc);
        	assertEquals(strLiteral, desc.getCurrentValue().toString());
        	assertEquals(strLiteral.getClass(), desc.getDataType());
        } catch (ConnectorException ce) {
        	ce.printStackTrace();
        	fail(ce.getMessage());
        }
    }
    
    public void testOutputXPathDescILiteralNullValue() { 
    	String strLiteral = "MetaMatrix";
    	String strQuery = "Select Company_id from Company where Company_id = '" + strLiteral + "'";
    	IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, strQuery);
    	ICriteria crits = query.getWhere();
    	List criteriaList = LanguageUtil.separateCriteriaByAnd(crits);
        ICompareCriteria compCriteria = (ICompareCriteria) criteriaList.get(0);                  	
        ILiteral literal = (ILiteral) compCriteria.getRightExpression();
        literal.setValue(null);
        try {
        	OutputXPathDesc desc = new OutputXPathDesc(literal);
        	assertNotNull(desc);
        	assertNull(desc.getCurrentValue());
        	assertEquals(strLiteral.getClass(), desc.getDataType());
        } catch (ConnectorException ce) {
        	ce.printStackTrace();
        	fail(ce.getMessage());
        }
    }

    public void testSetAndGetCurrentValue() {
        IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        ISelect select = query.getSelect();
        List symbols = select.getSelectSymbols();
        ISelectSymbol selectSymbol = (ISelectSymbol) symbols.get(0);
        IExpression expr = selectSymbol.getExpression();
        Element element = null;
        try {
            if (expr instanceof IElement) {
                MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();    
                element = (Element) metadata.getObject(elementID); 
            } else {
                fail("select symbols is not an element");
            }        
            OutputXPathDesc desc = new OutputXPathDesc(element);
            String myVal = "myValue";
            desc.setCurrentValue(myVal);
            assertEquals(myVal, desc.getCurrentValue());
        } catch (ConnectorException ex) {
            ex.printStackTrace();
            fail(ex.getMessage());
        }        
    }

    public void testGetDataType() {
        IQuery query = ProxyObjectFactory.getDefaultIQuery(vdbPath, QUERY);
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        ISelect select = query.getSelect();
        List symbols = select.getSelectSymbols();
        ISelectSymbol selectSymbol = (ISelectSymbol) symbols.get(0);
        IExpression expr = selectSymbol.getExpression();
        Element element = null;
        try {
            if (expr instanceof IElement) {
                MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();    
                element = (Element) metadata.getObject(elementID); 
            } else {
                fail("select symbols is not an element");
            }        
            OutputXPathDesc desc = new OutputXPathDesc(element);         
            assertNotNull(desc.getDataType());
            assertEquals(String.class, desc.getDataType());
        } catch (ConnectorException ex) {
            ex.printStackTrace();
            fail(ex.getMessage());
        }        
    }

}
