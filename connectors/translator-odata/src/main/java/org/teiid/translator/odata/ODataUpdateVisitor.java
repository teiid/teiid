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
package org.teiid.translator.odata;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.odata4j.core.OEntities;
import org.odata4j.core.OEntity;
import org.odata4j.core.OProperties;
import org.odata4j.core.OProperty;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.format.Entry;
import org.odata4j.format.FormatWriter;
import org.odata4j.format.FormatWriterFactory;
import org.teiid.language.*;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.TranslatorException;

public class ODataUpdateVisitor extends ODataSQLVisitor {
	protected ODataExecutionFactory executionFactory;
	protected RuntimeMetadata metadata;
	protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
	private String method = "POST"; //$NON-NLS-1$
	private String entityName;
	private EdmDataServices edmDataSerices;
	private String payload;
	private String uri;
	
	public ODataUpdateVisitor(ODataExecutionFactory executionFactory,
			RuntimeMetadata metadata, EdmDataServices edmDataSerices) {
		super(executionFactory, metadata);
		this.edmDataSerices = edmDataSerices;
	}
	
	@Override
    public void visit(Insert obj) {
		this.method = "POST"; //$NON-NLS-1$
		this.entityName = obj.getTable().getMetadataObject().getName();
		this.uri = this.entityName;
		
		final List<OProperty<?>> props = new ArrayList<OProperty<?>>();
		int elementCount = obj.getColumns().size();
		for (int i = 0; i < elementCount; i++) {
			Column column = obj.getColumns().get(i).getMetadataObject();
			List<Expression> values = ((ExpressionValueSource)obj.getValueSource()).getValues();
			OProperty<?> property = OProperties.simple(column.getName(), ((Literal)values.get(0)).getValue());
			props.add(property);
		}
		this.payload = buildPayload(this.entityName, props);	
	}	
	
	@Override
    public void visit(Update obj) {
		this.method = "PATCH"; //$NON-NLS-1$
		this.entityName = obj.getTable().getMetadataObject().getName();
		visitNode(obj.getTable());
		
		// only pk are allowed, no other criteria not allowed
		obj.setWhere(buildEntityKey(obj.getWhere()));
        
        // this will build with entity keys
        this.uri = getEnitityURL();
        
        if (this.uri.indexOf('(') == -1) {
        	this.exceptions.add(new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17011, this.filter.toString())));
        }
        
        if (this.filter.length() > 0) {
        	this.exceptions.add(new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17009, this.filter.toString())));
        }
		
		final List<OProperty<?>> props = new ArrayList<OProperty<?>>();
		int elementCount = obj.getChanges().size();
		for (int i = 0; i < elementCount; i++) {
			Column column = obj.getChanges().get(i).getSymbol().getMetadataObject();
			Literal value = (Literal)obj.getChanges().get(i).getValue();
			OProperty<?> property = OProperties.simple(column.getName(), value.getValue());
			props.add(property);
		}

		this.payload = buildPayload(this.entityName, props);
                
	}

	private String buildPayload(String entitySet, final List<OProperty<?>> props) {
		final EdmEntitySet ees = this.edmDataSerices.getEdmEntitySet(entitySet);
		
	    Entry entry =  new Entry() {
	        public String getUri() {
	          return null;
	        }
	        public OEntity getEntity() {
	          return OEntities.createRequest(ees, props, null);
	        }
	      };		
		
		StringWriter sw = new StringWriter();
		FormatWriter<Entry> fw = FormatWriterFactory.getFormatWriter(Entry.class, null, "ATOM", null); //$NON-NLS-1$
		fw.write(null, sw, entry);
		return sw.toString();
	}	
	
	@Override
    public void visit(Delete obj) {
		this.method = "DELETE"; //$NON-NLS-1$
		this.entityName = obj.getTable().getMetadataObject().getName();
		visitNode(obj.getTable());
		
		// only pk are allowed, no other criteria not allowed
        obj.setWhere(buildEntityKey(obj.getWhere()));
        
        // this will build with entity keys
        this.uri = getEnitityURL();
        if (this.uri.indexOf('(') == -1) {
        	this.exceptions.add(new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17011, this.filter.toString())));
        }
        
        if (this.filter.length() > 0) {
        	this.exceptions.add(new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17009, this.filter.toString())));
        }
	}
	
	public String getEntityName() {
		return this.entityName;
	}
	
	@Override
	public String buildURL() {
		return this.uri;
	}	
	
	public String getMethod() {
		return this.method;
	}
	
	public String getPayload() {
		return this.payload;
	}
}
