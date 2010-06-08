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

package org.teiid.translator.ws;

import java.sql.SQLXML;
import java.util.Collections;
import java.util.List;

import javax.resource.cci.ConnectionFactory;
import javax.xml.transform.Source;
import javax.xml.ws.Dispatch;

import org.teiid.core.BundleUtil;
import org.teiid.language.Call;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TypeFacility;

@Translator(name="ws")
public class XMLExecutionFactory extends ExecutionFactory<ConnectionFactory, Dispatch<Source>> {
	
	public static BundleUtil UTIL = BundleUtil.getBundleUtil(XMLExecutionFactory.class);
		
	private String saxFilterProviderClass;
	private String encoding = "ISO-8859-1"; //$NON-NLS-1$
	private boolean logRequestResponseDocs = false;

	@TranslatorProperty(description="Encoding of the XML documents", display="Encoding Scheme")
	public String getCharacterEncodingScheme() {
		return this.encoding;
	}
	
	public void setCharacterEncodingScheme(String encoding) {
		this.encoding = encoding;
	}
	
	@TranslatorProperty(description="Must be extension of org.teiid.translator.xml.SAXFilterProvider class", display="SAX Filter Provider Class")
	public String getSaxFilterProviderClass() {
		return this.saxFilterProviderClass;
	}

	public void setSaxFilterProviderClass(String saxFilterProviderClass) {
		this.saxFilterProviderClass = saxFilterProviderClass;
	}

	// Can we get rid of this?
	private String inputStreamFilterClass;

	public String getInputStreamFilterClass() {
		return this.inputStreamFilterClass;
	}

	public void setInputStreamFilterClass(String inputStreamFilterClass) {
		this.inputStreamFilterClass = inputStreamFilterClass;
	}

	@TranslatorProperty(description="Log the XML request/response documents", display="Log Request/Response Documents")
	public boolean isLogRequestResponseDocs() {
		return logRequestResponseDocs && LogManager.isMessageToBeRecorded(LogConstants.CTX_CONNECTOR, MessageLevel.DETAIL);
	}

	public void setLogRequestResponseDocs(boolean logRequestResponseDocs) {
		this.logRequestResponseDocs = logRequestResponseDocs;
	}

    @Override
    public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata, Dispatch<Source> connection)
    		throws TranslatorException {
		return new XMLProcedureExecution(command, metadata, executionContext, this, connection);
    }
    
	public SAXFilterProvider getSaxFilterProvider() throws TranslatorException {
		if (getSaxFilterProviderClass() == null) {
			return null;
		}
		return getInstance(SAXFilterProvider.class, getSaxFilterProviderClass(), null, null);
	}  	
	
    public SQLXML convertToXMLType(Source value) {
    	/*XMLReader reader = XMLReaderFactory.createXMLReader();
		
		if (getSaxFilterProvider() != null) {
			XMLFilter[] filters = getSaxFilterProvider().getExtendedFilters();
			for(int i = 0; i < filters.length; i++) {
				XMLFilter filter = filters[i];
				filter.setParent(reader);
				reader = filter;
			}
		}
		
		SAXSource saxSource = new SAXSource(reader, inputSource);*/
		
    	return (SQLXML)getTypeFacility().convertToRuntimeType(value);
    } 	
	
	@Override
    public final List getSupportedFunctions() {
        return Collections.EMPTY_LIST;
    }
	
	@Override
	public void getMetadata(MetadataFactory metadataFactory,
			Dispatch<Source> conn) throws TranslatorException {
		Procedure p = metadataFactory.addProcedure("invoke"); //$NON-NLS-1$ 
		metadataFactory.addProcedureParameter("request", TypeFacility.RUNTIME_NAMES.OBJECT, Type.In, p); //$NON-NLS-1$
		ProcedureParameter param = metadataFactory.addProcedureParameter("endpoint", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
		param.setNullType(NullType.Nullable);
		param = metadataFactory.addProcedureParameter("soapaction", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
		param.setNullType(NullType.Nullable);
	}

}
