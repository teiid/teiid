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

package org.teiid.translator.xml;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.teiid.language.BaseInCondition;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.language.Expression;
import org.teiid.language.In;
import org.teiid.language.LanguageUtil;
import org.teiid.language.Literal;
import org.teiid.language.Select;
import org.teiid.language.Comparison.Operator;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.translator.TranslatorException;


public class CriteriaDesc extends ParameterDescriptor {
    // TODO: a lot of this is HTTP/SOAP specific (i.e., do not apply to files).
    // There are various
    // places that we work around that (extension properties being null),
    // but it should be handled properly by moving the code into
    // HTTP and SOAP specific places

    // Describes an item in the parameter or criteria structure, and all the
    // attributes of it
    // Added to allow all the attributes of criteira to be managed in a single
    // structure,
    // and to make the code more manageable

    private List m_values;

    private int m_currentIndexInValuesList = 0;

    private Boolean m_parentAttribute = null;

    private Boolean m_enumeratedAttribute = null;

    private Boolean m_allowEmptyValue = null;

    private String m_dataAttributeName = null;

    private Properties m_additionalAttributes = null;

    private String m_inputXpath = null;
    
    private String nativeType = null;

    private boolean m_unlimited = false;

    private boolean m_enumerated = false;
    
    private boolean m_multiElement = false;

    private boolean m_soapArrayElement = false;

    private boolean m_simpleSoapElement;

    public static final String PARM_REQUIRED_VALUE_COLUMN_PROPERTY_NAME = "RequiredValue"; //$NON-NLS-1$

    public static final String PARM_ALLOWS_EMPTY_VALUES_COLUMN_PROPERTY_NAME = "AllowEmptyInputElement"; //$NON-NLS-1$

    public static final String PARM_HAS_MULTIPLE_VALUES_COLUMN_PROPERTY_NAME = "MultipleValues"; //$NON-NLS-1$

    public static final String PARM_HAS_MULTIPLE_VALUES_COMMA_DELIMITED_NAME = "CommaDelimited"; //$NON-NLS-1$

    public static final String PARM_HAS_MULTIPLE_VALUES_MULTI_ELEMENT_NAME = "MultiElement"; //$NON-NLS-1$
    
    public static final String PARM_IS_SIMPLE_SOAP_ARRAY_ELEMENT_NAME = "SimpleSoapArrayElement"; //$NON-NLS-1$
    
    public static final String PARM_IS_COMPLEX_SOAP_ARRAY_ELEMENT_NAME = "ComplexSoapArrayElement"; //$NON-NLS-1$

    public static final String PARM_XPATH_INPUT_COLUMN_PROPERTY_NAME = "XPathForInputParameter"; //$NON-NLS-1$

    public static final String PARM_AS_PARENT_ATTRIBUTE_COLUMN_PROPERTY_NAME = "AttributeOfParent"; //$NON-NLS-1$

    public static final String PARM_AS_NAMED_ATTRIBUTE_COLUMN_PROPERTY_NAME = "DataAttributeName"; //$NON-NLS-1$

    //use static method do see if a criteria object needs to be created
    public static CriteriaDesc getCriteriaDescForColumn(Column element,
            Select query) throws TranslatorException {
    	
    	CriteriaDesc retVal = null;
        List values = parseCriteriaToValues(element, query);

        if (values.size() == 0) {
            if (testForParam(element)) {
            	//add default value to values if it exists
            	//throws exception if no default specified and its required
                handleDefaultValue(element, values);                                
            }
        }
        //values.size may have changed if default value was added above
        //so we retest
        if (values.size() > 0 || findAllowEmptyValue(element)) {
            	retVal = new CriteriaDesc(element, values);
                retVal.setNativeType(element.getNativeType());
        }
        return retVal;
    }
    
    private static void handleDefaultValue(Column element, List values) throws TranslatorException {
        Object defaultVal = element.getDefaultValue();
        if (defaultVal != null) {
            values.add(defaultVal);
        } else if (findIsRequired(element)) {
            throw new TranslatorException(
                    XMLPlugin.getString("CriteriaDesc.value.not.found.for.param") //$NON-NLS-1$
                            + element.getName());
        }        
    }

    private static boolean findIsRequired(Column element)
            throws TranslatorException {
            String value = element.getProperties().get(
                    PARM_REQUIRED_VALUE_COLUMN_PROPERTY_NAME);
			return Boolean.valueOf(value);
    }

	private static boolean findAllowEmptyValue(Column element)
            throws TranslatorException {
            String value = element.getProperties().get(
                    PARM_ALLOWS_EMPTY_VALUES_COLUMN_PROPERTY_NAME);
            return Boolean.valueOf(value);
    }

    /**
     * @see com.metamatrix.server.datatier.SynchConnectorConnection#submitRequest(java.lang.Object)
     */
    public CriteriaDesc(Column myElement, List myValues)
            throws TranslatorException {

        super(myElement);
		m_values = myValues;
		final String enumerated = PARM_HAS_MULTIPLE_VALUES_COMMA_DELIMITED_NAME; 
		final String multiElement = PARM_HAS_MULTIPLE_VALUES_MULTI_ELEMENT_NAME; 
        String multiplicityStr = getElement().getProperties().get(
				PARM_HAS_MULTIPLE_VALUES_COLUMN_PROPERTY_NAME);
        if (multiplicityStr == null) {
            multiplicityStr = ""; //$NON-NLS-1$
        }
		if (multiplicityStr.equals(enumerated)) {
			m_unlimited = true;
			m_enumerated = true;
		} else if (multiplicityStr.equals(multiElement)) {
			m_unlimited = true;
			m_enumerated = false;
			m_multiElement = true;
        } else if (multiplicityStr.equals(PARM_IS_SIMPLE_SOAP_ARRAY_ELEMENT_NAME) ||
                multiplicityStr.equals(PARM_IS_COMPLEX_SOAP_ARRAY_ELEMENT_NAME)) {
            m_unlimited = true;
            m_enumerated = false;
            m_multiElement = true;
            m_soapArrayElement = true;
            if (multiplicityStr.equals(PARM_IS_SIMPLE_SOAP_ARRAY_ELEMENT_NAME)) {
                m_simpleSoapElement = true;
            } else {
                m_simpleSoapElement = false;
            }
		} else {
			m_unlimited = false;
			m_enumerated = false;
		}
    }
    
    private CriteriaDesc(CriteriaDesc other) {
    	//from ParameterDescriptor
        setXPath(other.getXPath());
        setIsParameter(other.isParameter());
        setIsResponseId(other.isResponseId());
        setIsLocation(other.isLocation());
        setColumnName(String.valueOf(other.getColumnName()));
        setColumnNumber(other.getColumnNumber());
        setElement(other.getElement());
        setNativeType(other.getNativeType());
        
    	m_parentAttribute = other.m_parentAttribute == null ? null : Boolean.valueOf(other.m_parentAttribute);
    	m_enumeratedAttribute = other.m_enumeratedAttribute == null ? null : Boolean.valueOf(other.m_enumeratedAttribute);
    	m_allowEmptyValue = other.m_allowEmptyValue == null ? null : Boolean.valueOf(other.m_allowEmptyValue);
    	m_dataAttributeName = other.m_dataAttributeName == null ? null : String.valueOf(other.m_dataAttributeName);
    	m_additionalAttributes = other.m_additionalAttributes == null ? null : new Properties(other.m_additionalAttributes);
    	m_inputXpath = other.m_inputXpath == null ? null : String.valueOf(other.m_inputXpath);
    	m_unlimited = other.m_unlimited;
    	m_enumerated = other.m_enumerated;
    	m_multiElement = other.m_multiElement;
        m_soapArrayElement = other.m_soapArrayElement;
    	//don't copy the values
    	m_values = new ArrayList();    	    	    	
    }

    public String getInputXpath() throws TranslatorException {
        if (m_inputXpath == null) {
            findInputXPath();
        }
        return m_inputXpath;
    }

    private void findInputXPath() throws TranslatorException {
    	m_inputXpath = getElement().getProperties().get(PARM_XPATH_INPUT_COLUMN_PROPERTY_NAME);
        if (m_inputXpath == null || m_inputXpath.trim().length() == 0) {
            m_inputXpath = getColumnName();
        }
    }

    public boolean isUnlimited() {
        return m_unlimited;
    }

    public boolean isMultiElement() {
        return m_multiElement;
    }
    
    public boolean isSOAPArrayElement() {
        return m_soapArrayElement;
    }

    public boolean isAutoIncrement() throws TranslatorException {
        return getElement().isAutoIncremented();
    }

    //from model extensions
    public boolean isParentAttribute() throws TranslatorException {
        if (m_parentAttribute == null) {
            findParentAttribute();
        }
        return m_parentAttribute.booleanValue();
    }

    private void findParentAttribute() throws TranslatorException {
    	String value = getElement().getProperties()
    			.get(PARM_AS_PARENT_ATTRIBUTE_COLUMN_PROPERTY_NAME);
		m_parentAttribute = Boolean.valueOf(value);
    }

    //from model extensions
    public boolean isEnumeratedAttribute() throws TranslatorException {
        return m_enumerated;
    }

    //from model extensions
    public boolean allowEmptyValue() throws TranslatorException {

        if (m_allowEmptyValue == null) {
            findAllowEmptyValue();
        }
        return m_allowEmptyValue.booleanValue();
    }

    private void findAllowEmptyValue() throws TranslatorException {
    	String value = getElement().getProperties()
    			.get(PARM_ALLOWS_EMPTY_VALUES_COLUMN_PROPERTY_NAME);
		m_allowEmptyValue = Boolean.valueOf(value);
    }

    //from model extensions
    public boolean isDataInAttribute() throws TranslatorException {
        String dataAttribute = getDataAttributeName();
        if (dataAttribute.trim().length() == 0) {
            return false;
        }
        return true;
    }

    //from model extensions
    public String getDataAttributeName() throws TranslatorException {

        if (m_dataAttributeName == null) {
            findDataAttributeName();
        }
        return m_dataAttributeName;
    }

    private void findDataAttributeName() throws TranslatorException {
    	m_dataAttributeName = getElement().getProperties().get(
    			PARM_AS_NAMED_ATTRIBUTE_COLUMN_PROPERTY_NAME);
        if (m_dataAttributeName == null) {
            m_dataAttributeName = ""; //$NON-NLS-1$
        }
    }

    public List getValues() {
        return m_values;
    }

    public int getNumberOfValues() {
        return m_values.size();
    }

    //This code manages the looping of parameters to make multiple server
    // calls.
    //The current index is used to get a value to be added to the parmString
	public String getCurrentIndexValue() throws TranslatorException {
		final int initialBufferSize = 1000;
		if (m_values.size() == 0 && allowEmptyValue()) {
			return ""; //$NON-NLS-1$
		}
		if (isEnumeratedAttribute()) {
			StringBuffer sb = new StringBuffer(initialBufferSize);
			String startChar = ""; //$NON-NLS-1$
			for (int x = 0; x < m_values.size(); x++) {
				sb.append(startChar);
				sb.append(m_values.get(x));
				startChar = ","; //$NON-NLS-1$
			}
			return sb.toString();
		}
		if (m_values.size() > 0) {
			return (String) m_values.get(m_currentIndexInValuesList);
		}
		return null;
	}
    //if so, it is incremented
    public boolean incrementIndex() throws TranslatorException {
        if (isEnumeratedAttribute()) {
            return false;
        } else if (m_currentIndexInValuesList < m_values.size() - 1) {
            m_currentIndexInValuesList++;
            return true;
        } else {
            return false;
        }
    }

    public void resetIndex() {
        m_currentIndexInValuesList = 0;
    }

    private static ArrayList parseCriteriaToValues(Column element, Select query) throws TranslatorException {

        String fullName = element.getFullName().trim().toUpperCase();
        ArrayList parmPair = new ArrayList();
        if (element.getSearchType() == SearchType.Searchable
                || element.getSearchType() == SearchType.All_Except_Like) {
            // Check and set criteria for the IData input
            Condition criteria = query.getWhere();
            List<Condition> criteriaList = LanguageUtil.separateCriteriaByAnd(criteria);
            for(Condition criteriaSeg: criteriaList) {
                if (criteriaSeg instanceof Comparison) {
                    Comparison compCriteria = (Comparison) criteriaSeg; 
                    if (compCriteria.getOperator() == Operator.EQ) {                    	
                        Expression lExpr = compCriteria.getLeftExpression();
                        Expression rExpr = compCriteria.getRightExpression();
                        handleCompareCriteria(lExpr, rExpr, fullName, parmPair);
                    }
                } else if (criteriaSeg instanceof BaseInCondition) {
                    handleInCriteria((BaseInCondition) criteriaSeg, fullName, parmPair);
                    
                }
            }
        }
        return parmPair;
    }
    
    private static void handleInCriteria(BaseInCondition baseInCriteria, String fullName, ArrayList parmPair) {
        Expression expr = baseInCriteria.getLeftExpression();
        if (expr instanceof ColumnReference) {
            if (nameMatch(expr, fullName)) {
                Iterator vIter = null;

                vIter = ((In) baseInCriteria).getRightExpressions().iterator();
                while (vIter.hasNext()) {
                    Literal val = (Literal) vIter.next();
                    String constantValue = val.getValue()
                            .toString();
                    constantValue = stringifyCriteria(constantValue);
                    parmPair.add(constantValue);
                }        
            }
        }
    }
    
    private static void handleCompareCriteria(Expression lExpr, Expression rExpr, String fullName, ArrayList parmPair) {         
        checkElement(lExpr, rExpr, fullName, parmPair);
        checkElement(rExpr, lExpr, fullName, parmPair);                                                        
    }
    
    private static void checkElement(Expression expression, Expression literalCandidate, String fullName, ArrayList parmPair) {
        if (expression instanceof ColumnReference) {
            if ((nameMatch(expression, fullName))
                    && (literalCandidate instanceof Literal)) {
                String constantValue = ((Literal) literalCandidate)
                        .getValue().toString();
                constantValue = stringifyCriteria(constantValue);
                parmPair.add(constantValue);
            }
        }        
    }

    public static boolean nameMatch(Expression expr, String elementName) {
        ColumnReference exprElement = (ColumnReference) expr;
        String symbolName = exprElement.getName().toUpperCase().trim();
        String tempElementName = elementName.toUpperCase();
        int indx = symbolName.lastIndexOf("."); //$NON-NLS-1$
        //. has to be there
        symbolName = symbolName.substring(indx + 1);
        indx = elementName.lastIndexOf("."); //$NON-NLS-1$
        //. has to be there
        tempElementName = tempElementName.substring(indx + 1);
        return symbolName.equals(tempElementName);
    }

    public static String stringifyCriteria(String startCriteria) {
        int indx = 0;
        String cStr = startCriteria;
        indx = cStr.indexOf("'"); //$NON-NLS-1$
        if (indx == 0) {
            int indx2 = cStr.substring(1).indexOf("'"); //$NON-NLS-1$
            if (indx2 >= 0) {
                return cStr.substring(1, indx2 + 1);
            }
        }
        indx = cStr.indexOf('"');
        if (indx == 0) {
            int indx2 = cStr.substring(1).indexOf('"');
            if (indx2 > 0) {
                return cStr.substring(1, indx2 + 1);
            }
        }
        return cStr;
    }

	public CriteriaDesc cloneWithoutValues() {		
		CriteriaDesc newDesc = new CriteriaDesc(this);
		return newDesc;
	}

	public void setValue(int i, Object object) {
		if(m_values.size() <= i) {
			m_values.add(i, object);
		} else {
			m_values.set(i, object);
		}
	}
    
    private void setNativeType(String nativeType) {
        this.nativeType = nativeType;
    }
    
    public String getNativeType() {
        return nativeType;
    }

    public boolean isSimpleSoapElement() {
        return m_simpleSoapElement;
    }
}
