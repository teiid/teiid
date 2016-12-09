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
package org.teiid.translator.odata.sap;

import java.util.HashMap;

import org.odata4j.core.NamespacedAnnotation;
import org.odata4j.core.PrefixedNamespace;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmProperty;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.KeyRecord.Type;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.odata.ODataMetadataProcessor;
import org.teiid.translator.odata.ODataTypeManager;

public class SAPMetadataProcessor extends ODataMetadataProcessor {
	private static final String SAPURI = "http://www.sap.com/Protocols/SAPData"; //$NON-NLS-1$
	private HashMap<Table, KeyRecord> accessPatterns = new HashMap();
	
	@Override
	protected Table buildTable(MetadataFactory mf, EdmEntitySet entitySet) {
		boolean creatable = true;
		boolean updatable = true;
		boolean deletable = true;
		boolean pageable = true;
		boolean topable = true;
		
		Table t = mf.addTable(entitySet.getName());
		
		Iterable<? extends NamespacedAnnotation> annotations = entitySet.getAnnotations();
		for (NamespacedAnnotation annotation:annotations) {
			PrefixedNamespace namespace = annotation.getNamespace();
			if (namespace.getUri().equals(SAPURI)) {
				String name = annotation.getName();
				if (name.equalsIgnoreCase("label")) { //$NON-NLS-1$
					t.setAnnotation((String)annotation.getValue());	
				}
				else if (name.equalsIgnoreCase("creatable")) { //$NON-NLS-1$
					creatable = Boolean.parseBoolean((String)annotation.getValue());	
				}
				else if (name.equalsIgnoreCase("updatable")) { //$NON-NLS-1$
					updatable = Boolean.parseBoolean((String)annotation.getValue());
				}
				else if (name.equalsIgnoreCase("pageable")) { //$NON-NLS-1$
					pageable = Boolean.parseBoolean((String)annotation.getValue());
				}
				else if (name.equalsIgnoreCase("topable")) { //$NON-NLS-1$
					topable = Boolean.parseBoolean((String)annotation.getValue());
				}
				else if (name.equalsIgnoreCase("deletable")) { //$NON-NLS-1$
					deletable = Boolean.parseBoolean((String)annotation.getValue());
				}
			}
		}
		
		t.setSupportsUpdate(creatable && updatable && deletable);
		if (!topable || !pageable) {
			// TODO: currently Teiid can not do this in fine grained manner; 
			// will be turned on by default; but user needs to turn off using the 
			// capabilities if any table does not support this feature
		}
		return t;
	}
	
	String getProperty(EdmEntitySet entitySet, String key) {
		Iterable<? extends NamespacedAnnotation> annotations = entitySet.getAnnotations();
		for (NamespacedAnnotation annotation:annotations) {
			PrefixedNamespace namespace = annotation.getNamespace();
			if (namespace.getUri().equals(SAPURI)) {
				if (annotation.getName().equalsIgnoreCase(key)) {
					return (String)annotation.getValue();
				}
			}
		}
		return null;
	}

	@Override
	protected Column buildColumn(MetadataFactory mf, Table table, EdmProperty ep, EdmEntitySet entitySet, String prefix) {
		boolean creatable = true;
		boolean updatable = true;
		boolean filterable = true;
		boolean required_in_filter = false;
		
		String columnName = ep.getName();
		if (prefix != null) {
			columnName = prefix+"_"+columnName; //$NON-NLS-1$
		}
		Column c = mf.addColumn(columnName, ODataTypeManager.teiidType(ep.getType().getFullyQualifiedTypeName()), table);
		c.setNameInSource(ep.getName());
		
		Iterable<? extends NamespacedAnnotation> annotations = ep.getAnnotations();
		for (NamespacedAnnotation annotation:annotations) {
			PrefixedNamespace namespace = annotation.getNamespace();
			if (namespace.getUri().equals(SAPURI)) {
				String name = annotation.getName();
				if (name.equalsIgnoreCase("label")) { //$NON-NLS-1$
					c.setAnnotation((String)annotation.getValue());	
				}
				else if (name.equalsIgnoreCase("creatable")) { //$NON-NLS-1$
					creatable = Boolean.parseBoolean((String)annotation.getValue());	
				}
				if (name.equalsIgnoreCase("visible")) { //$NON-NLS-1$
					c.setSelectable(Boolean.parseBoolean((String)annotation.getValue()));
				}
				if (name.equalsIgnoreCase("updatable")) { //$NON-NLS-1$
					updatable = Boolean.parseBoolean((String)annotation.getValue());
				}
				if (name.equalsIgnoreCase("sortable")) { //$NON-NLS-1$
					if (!Boolean.parseBoolean((String)annotation.getValue())){
						c.setSearchType(SearchType.Unsearchable); 
					}
				}
				if (name.equalsIgnoreCase("filterable")) { //$NON-NLS-1$
					filterable = Boolean.parseBoolean((String)annotation.getValue());
				}
				if (name.equalsIgnoreCase("required-in-filter")) { //$NON-NLS-1$
					required_in_filter = Boolean.parseBoolean((String)annotation.getValue());
				}
				if (name.equalsIgnoreCase("filter-restriction")) { //$NON-NLS-1$
					//TODO:
				}
			}
		}
		
		c.setUpdatable(creatable && updatable);
		if (!filterable) {
			c.setSearchType(SearchType.Unsearchable);
		}

		if (required_in_filter) {
			if (this.accessPatterns.get(table) == null) {
				KeyRecord record = new KeyRecord(Type.AccessPattern);
				record.addColumn(c);
				this.accessPatterns.put(table, record);
			}
			else {
				this.accessPatterns.get(table).addColumn(c);
			}
		}
		
		// entity set defined to as must have filter
		boolean requiresFilter = Boolean.parseBoolean(getProperty(entitySet, "requires-filter")); //$NON-NLS-1$
		if (requiresFilter && filterable && !required_in_filter) {
			KeyRecord record = new KeyRecord(Type.AccessPattern);
			record.addColumn(c);
			table.getAccessPatterns().add(record);
		}
		return c;
	}

	@Override
	protected Table addEntitySetAsTable(MetadataFactory mf, EdmEntitySet entitySet) throws TranslatorException {
		Table table = super.addEntitySetAsTable(mf, entitySet);
		KeyRecord accessPattern = this.accessPatterns.get(table);
		if (accessPattern != null) {
			table.getAccessPatterns().add(accessPattern);
		}
		return table;
	}	
}
