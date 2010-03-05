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

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;
import org.teiid.connector.api.ConnectorException;

import com.metamatrix.connector.xml.Constants;

public class ExecutionInfo {
// TODO:Refactor.  This class was defined when the multiple reqests to an XML service
// (read HTTP) to satisfy an IN clause were abstacted beneath a single call to Executor.getResult().
// This case is now satified within the execute call by making a single call to Executor.getResult()
// for each paramter of the IN clause.
	
    private List columns;
    private int columnCount;
    private List<CriteriaDesc> params;
    private List<CriteriaDesc> criteria;
    private Map<String, String> otherProps;
    private String tablePath;
    private String location;
	private Map<String, String> prefixToNamespace;
	private Map<String, String> namespaceToPrefix;
    
    public ExecutionInfo() { 
        this.columnCount = 0;
        this.columns = new ArrayList();
        this.params = new ArrayList<CriteriaDesc>();
        this.criteria = new ArrayList<CriteriaDesc>();
        this.otherProps = new HashMap<String, String>();
        this.tablePath = ""; //$NON-NLS-1$
    }
    
    
    public String getTableXPath() {
        return this.tablePath;   
    }
    
    public String getLocation() {
        return this.location;   
    }
    
    public List getRequestedColumns() {
        return this.columns;   
    }
    
    
    public int getColumnCount() {
     return this.columnCount;   
    }
    
    
    public List<CriteriaDesc> getParameters() {
        return this.params;
    }
    
    
    public List<CriteriaDesc> getCriteria() {
        return this.criteria;
    }
    

    public Map<String, String> getOtherProperties() {
        return this.otherProps;
    }

    public void setTableXPath(String path)  {
        if (path == null || path.trim().length() == 0) {
        	this.tablePath = null;   
        } else {
        	this.tablePath = path;
        }
    }
    
    public void setLocation(String location)  {
        this.location = location;   
    }
    
    public void setRequestedColumns(List value) {
        this.columns = value;   
    }
    
    
    public void setColumnCount(int count) {
    	this.columnCount = count;   
    }
    
    
    public void setParameters(List<CriteriaDesc> parameters) {
        this.params = parameters;
    }
    
    
    public void setCriteria(List<CriteriaDesc> criteria) {
        this.criteria = criteria;
    }
    

    public void setOtherProperties(Map<String, String> props) {
    	if (props == null) {
    		this.otherProps = new HashMap<String, String>();
    	} else {
    		this.otherProps = props;	
    	}        
    }

	// It is not enforced that there is only one ResponseId in the parameters, but it is
	// a valid assumption and the modelgenerators will never create a model that has more 
	// than one.  We could enforce this with a real metamodel.
	public CriteriaDesc getResponseIDCriterion() {
		CriteriaDesc result = null;
		Iterator iter = this.params.iterator();
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
		if(this.namespaceToPrefix != null) {
			return this.namespaceToPrefix;
		} 
		getPrefixToNamespacesMap();
		return this.namespaceToPrefix;
	}
	
	public Map<String, String> getPrefixToNamespacesMap() throws ConnectorException {
		if (this.prefixToNamespace != null) {
			return this.prefixToNamespace;
		} 
		
		this.prefixToNamespace = new HashMap<String, String>();
		this.namespaceToPrefix = new HashMap<String, String>();
		String namespacePrefixes = getOtherProperties().get(Constants.NAMESPACE_PREFIX_PROPERTY_NAME);
		if (namespacePrefixes == null || namespacePrefixes.trim().length() == 0) {
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
				this.prefixToNamespace.put(prefix, uri);
				this.namespaceToPrefix.put(uri, prefix);
			}
		} catch (JDOMException e) {
			String rawMsg = Messages.getString("Executor.jaxen.error.on.namespace.pairs"); //$NON-NLS-1$
			Object[] objs = new Object[2];
			objs[0] = prefix;
			objs[1] = uri;
			String msg = MessageFormat.format(rawMsg, objs);
			throw new ConnectorException(e, msg);
		} catch (IOException e) {
			throw new ConnectorException(e, e.getMessage());
		}
		return this.prefixToNamespace;
	}
}
