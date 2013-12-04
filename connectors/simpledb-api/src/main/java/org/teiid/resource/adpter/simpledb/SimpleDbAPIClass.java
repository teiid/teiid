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

package org.teiid.resource.adpter.simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.*;

/**
 * Class encapsulating AmazonSimpleDBClient class with some added value
 * 
 * @author rhopp
 * 
 */

public class SimpleDbAPIClass {

	private AmazonSimpleDBClient client;

	public SimpleDbAPIClass(String key1, String key2) {
		client = new AmazonSimpleDBClient(new BasicAWSCredentials(key1, key2));
	}

	/**
	 * Lists all domains of database
	 * @return
	 */
	
	public List<String> getDomains() {
		return client.listDomains().getDomainNames();
	}

	/**
	 * 
	 * @param domainName 
	 * @return Set of attribute names for given domain
	 */
	
	public Set<String> getAttributeNames(String domainName) {
		DomainMetadataRequest domainMetadataRequest = new DomainMetadataRequest(domainName);
		DomainMetadataResult metadataResult = client.domainMetadata(domainMetadataRequest);
		int attributesCount = metadataResult.getAttributeNameCount();
		SelectResult selectResult = client.select(new SelectRequest("SELECT * FROM " + domainName)); //$NON-NLS-1$
		return getAttributeNamesFromSelectResult(selectResult, attributesCount);

	}

	/**
	 * Removes item with given ItemName from domain
	 * @param domainName
	 * @param itemName
	 */
	
	public void performDelete(String domainName, String itemName) {
		DeleteAttributesRequest req = new DeleteAttributesRequest(domainName, itemName);
		client.deleteAttributes(req);
	}

	/**
	 * Performs select expression. This expression must be in format which is understandable to SimpleDB database
	 * @param selectExpression
	 * @param columns
	 * @return Iterator of List<String> results 
	 */
	
	public Iterator<List<String>> performSelect(String selectExpression, final List<String> columns) {
		SelectRequest selectRequest = new SelectRequest(selectExpression);
		SelectResult result = client.select(selectRequest);
		final Iterator<Item> items = result.getItems().iterator();
		return new Iterator<List<String>>() {

			@Override
			public boolean hasNext() {
				return items.hasNext();
			}

			@Override
			public List<String> next() {
				if (!items.hasNext()) {
					throw new NoSuchElementException();
				}
				return transformItemIntoStringList(items.next(), columns);
				
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};
	}

	/**
	 *  Performs update on given domain and items
	 * @param domainName
	 * @param items
	 */
	
	public void performUpdate(String domainName, Map<String, Map<String, String>> items) {
		List<ReplaceableItem> itemsList = new ArrayList<ReplaceableItem>();
		for (Map.Entry<String, Map<String, String>> item : items.entrySet()) {
			ReplaceableItem it = new ReplaceableItem(item.getKey());
			List<ReplaceableAttribute> attributesList = new ArrayList<ReplaceableAttribute>();
			for (Map.Entry<String, String> attribute : item.getValue().entrySet()) {
				attributesList.add(new ReplaceableAttribute(attribute.getKey(), attribute.getValue(), true));
			}
			it.setAttributes(attributesList);
			itemsList.add(it);
		}
		BatchPutAttributesRequest req = new BatchPutAttributesRequest(domainName, itemsList);
		client.batchPutAttributes(req);
	}

	/**
	 *  Inserts item into given domain.
	 * @param domainName
	 * @param itemName
	 * @param columnsMap
	 * @return
	 */
	
	public int performInsert(String domainName, String itemName, Map<String, String> columnsMap) {
		List<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>();
		for (Map.Entry<String, String> column : columnsMap.entrySet()) {
			if (!column.getKey().equals("itemName()")) { //$NON-NLS-1$
				if (column.getValue().matches("^\\[.+\\]$")) { //is multivalued attribute //$NON-NLS-1$
					List<ReplaceableAttribute> multivaluedAttributes= createMultivaluedAttribute(column);
					attributes.addAll(multivaluedAttributes);
				} else { //is just single valued attribute
					attributes.add(new ReplaceableAttribute(column.getKey(), column.getValue(), false));
				}
			}
		}
		PutAttributesRequest putAttributesRequest = new PutAttributesRequest(domainName, itemName, attributes);
		client.putAttributes(putAttributesRequest);
		return 1;
	}

	private List<ReplaceableAttribute> createMultivaluedAttribute(Map.Entry<String, String> column) {
		List<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>();
		String trimmedAttributes = column.getValue().substring(1, column.getValue().length() - 1);
		Pattern p = Pattern.compile("[^\\\\];"); //$NON-NLS-1$
		Matcher match = p.matcher(trimmedAttributes);
		int lastMatch = 0;
		while (match.find()) {
			ReplaceableAttribute attribute = new ReplaceableAttribute();
			attribute.setName(column.getKey());
			String value = trimmedAttributes.substring(lastMatch, match.start() + 1);
			attribute.setValue(value.replaceAll("\\;", ";")); //$NON-NLS-1$ //$NON-NLS-2$
			lastMatch = match.end();
			attribute.setReplace(false);
			attributes.add(attribute);
		}
		ReplaceableAttribute attribute = new ReplaceableAttribute();
		attribute.setName(column.getKey());
		String value = trimmedAttributes.substring(lastMatch, trimmedAttributes.length());
		attribute.setValue(value.replaceAll("\\\\;", ";")); //$NON-NLS-1$ //$NON-NLS-2$
		attribute.setReplace(false);
		attributes.add(attribute);
		return attributes;
	}

	private Set<String> getAttributeNamesFromSelectResult(SelectResult selectResult, int attributesCount) {
		Set<String> attributes = new LinkedHashSet<String>();
		Iterator<Item> itemsIterator = selectResult.getItems().iterator();
		while (attributes.size() < attributesCount) {
			Item item = itemsIterator.next();
			for (Attribute attribute : item.getAttributes()) {
				attributes.add(attribute.getName());
			}
		}
		return attributes;
	}
	
	private List<String> transformItemIntoStringList(Item item, List<String> columns) {
		List<String> row = new ArrayList<String>(columns.size());
		Map<String, List<Attribute>> attributeMap = createAttributeMap(item.getAttributes());
		for (String column : columns) {
			if (column.equals("itemName()")) { //$NON-NLS-1$
				row.add(item.getName());
			} else {
				List<Attribute> list = attributeMap.get(column);
				if (list != null) {
					row.add(getMultivalueAttributeString(list));
				} else {
					row.add("null"); //$NON-NLS-1$					
				}
			}
		}
		return row;
	}

	private String getMultivalueAttributeString(List<Attribute> list) {
		if (list.size() == 1) {
			return list.get(0).getValue();
		}
		StringBuilder sb = new StringBuilder();
		sb.append("["); //$NON-NLS-1$
		String escapedValue = list.get(0).getValue().replace(";", "\\;"); //$NON-NLS-1$ //$NON-NLS-2$
		sb.append(escapedValue);
		for (int i = 1; i < list.size(); i++) {
			escapedValue = list.get(i).getValue().replace(";", "\\;"); //$NON-NLS-1$ //$NON-NLS-2$
			sb.append(";"); //$NON-NLS-1$
			sb.append(escapedValue);
		}
		sb.append("]"); //$NON-NLS-1$
		return sb.toString();
	}

	private Map<String, List<Attribute>> createAttributeMap(List<Attribute> attributes) {
		Map<String, List<Attribute>> map = new HashMap<String, List<Attribute>>();
		for (Attribute attribute : attributes) {
			if (map.get(attribute.getName()) == null) {
				List<Attribute> list = new ArrayList<Attribute>();
				list.add(attribute);
				map.put(attribute.getName(), list);
			} else {
				map.get(attribute.getName()).add(attribute);
			}
		}
		return map;
	}
}
