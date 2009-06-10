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

package com.metamatrix.connector.xml.base;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;
import org.teiid.connector.api.ConnectorException;

import com.metamatrix.connector.xml.Constants;
import com.metamatrix.connector.xml.http.Messages;

public class ExecutionInfo {
// TODO:Refactor.  This class was defined when the multiple reqests to an XML service
// (read HTTP) to satisfy an IN clause were abstacted beneath a single call to Executor.getResult().
// This case is now satified within the execute call by making a single call to Executor.getResult()
// for each paramter of the IN clause.
	
    private List m_columns;
    private int m_columnCount;
    private List m_params;
    private List m_criteria;
    private Properties m_otherProps;
    private Properties m_schemaProps;
    private String m_tablePath;
    private String m_location;
	private Map<String, String> m_prefixToNamespace;
	private Map<String, String> m_namespaceToPrefix;
    
    public ExecutionInfo() { 
        m_columnCount = 0;
        m_columns = new ArrayList();
        m_params = new ArrayList();
        m_criteria = new ArrayList();
        m_otherProps = new Properties();
        m_schemaProps = new Properties();
        m_tablePath = ""; //$NON-NLS-1$
    }
    
    
    public String getTableXPath() {
        return m_tablePath;   
    }
    
    public String getLocation()
    {
        return m_location;   
    }
    
    public List getRequestedColumns() {
        return m_columns;   
    }
    
    
    public int getColumnCount() {
     return m_columnCount;   
    }
    
    
    public List getParameters() {
        return m_params;
    }
    
    
    public List getCriteria() {
        return m_criteria;
    }
    

    public Properties getOtherProperties() {
        return m_otherProps;
    }

    public void setTableXPath(String path)  {
        if (path == null || path.trim().length() == 0) {
         m_tablePath = null;   
        } else {
        	m_tablePath = path;
        }
    }
    
    public void setLocation(String location)
    {
        m_location = location;   
    }
    
    public void setRequestedColumns(List columns) {
        m_columns = columns;   
    }
    
    
    public void setColumnCount(int count) {
      m_columnCount = count;   
    }
    
    
    public void setParameters(List params) {
        m_params = params;
    }
    
    
    public void setCriteria(List criteria) {
        m_criteria = criteria;
    }
    

    public void setOtherProperties(Properties props) {
    	if (props == null) {
    		m_otherProps = new Properties();
    	} else {
    		m_otherProps = props;	
    	}        
    }

	// It is not enforced that there is only one ResponseId in the parameters, but it is
	// a valid assumption and the modelgenerators will never create a model that has more 
	// than one.  We could enforce this with a real metamodel.
	public CriteriaDesc getResponseIDCriterion() {
		CriteriaDesc result = null;
		Iterator iter = m_params.iterator();
		while(iter.hasNext()) {
			CriteriaDesc criterion = (CriteriaDesc)iter.next();
			if(criterion.isResponseId()) {
				result = criterion;
				break;
			}
		}
		return result;
	}

	public Map<String, String> getNamespaceToPrefixMap() throws ConnectorException {
		if(null != m_namespaceToPrefix) {
			return m_namespaceToPrefix;
		} else {
			getPrefixToNamespacesMap();
			return m_namespaceToPrefix;
		}
	}
	
	public Map<String, String> getPrefixToNamespacesMap() throws ConnectorException {
		if (null != m_prefixToNamespace) {
			return m_prefixToNamespace;
		} else {
			m_prefixToNamespace = new HashMap<String, String>();
			m_namespaceToPrefix = new HashMap<String, String>();
			String namespacePrefixes = getOtherProperties().getProperty(
					Constants.NAMESPACE_PREFIX_PROPERTY_NAME);
			if (namespacePrefixes == null
					|| namespacePrefixes.trim().length() == 0) {
				return null;
			}
			String prefix = null;
			String uri = null;
			try {
				String xml = "<e " + namespacePrefixes + "/>"; //$NON-NLS-1$ //$NON-NLS-2$
				Reader reader = new StringReader(xml);
				SAXBuilder builder = new SAXBuilder();
				Document domDoc = builder.build(reader);
				Element elem = domDoc.getRootElement();
				List namespaces = elem.getAdditionalNamespaces();
				for (Iterator iter = namespaces.iterator(); iter.hasNext();) {
					Object o = iter.next();
					Namespace namespace = (Namespace) o;
					prefix = namespace.getPrefix();
					uri = namespace.getURI();
					m_prefixToNamespace.put(prefix, uri);
					m_namespaceToPrefix.put(uri, prefix);
				}
			} catch (JDOMException e) {
				String rawMsg = Messages
						.getString("Executor.jaxen.error.on.namespace.pairs"); //$NON-NLS-1$
				Object[] objs = new Object[2];
				objs[0] = prefix;
				objs[1] = uri;
				String msg = MessageFormat.format(rawMsg, objs);
				throw new ConnectorException(e, msg);
			} catch (IOException e) {
				throw new ConnectorException(e, e.getMessage());
			}
			return m_prefixToNamespace;
		}
	}

	public void setSchemaProperties(Properties schemaProperties) {
		m_schemaProps = schemaProperties;
	}
	
	public Properties getSchemaProperties() {
		return m_schemaProps;
	}    
}
