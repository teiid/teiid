/*
 * Copyright 2012-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.simpledb.api;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.*;
import com.amazonaws.services.simpledb.util.SimpleDBUtils;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.Column;
import org.teiid.translator.TranslatorException;

import java.util.*;

public class SimpleDBConnectionImpl implements SimpleDBConnection {
    private AmazonSimpleDB amazonSimpleDBClient;

    public SimpleDBConnectionImpl(AmazonSimpleDB amazonSimpleDBClient) {
        this.amazonSimpleDBClient = amazonSimpleDBClient;
    }

    @Override
    public void createDomain(String domainName) throws TranslatorException {
        try {
            this.amazonSimpleDBClient.createDomain(new CreateDomainRequest(domainName));
        } catch (AmazonServiceException e) {
            throw new TranslatorException(e);
        } catch (AmazonClientException e) {
            throw new TranslatorException(e);
        }
    }

    @Override
    public void deleteDomain(String domainName) throws TranslatorException {
        try {
            this.amazonSimpleDBClient.deleteDomain(new DeleteDomainRequest(domainName));
        } catch (AmazonServiceException e) {
            throw new TranslatorException(e);
        } catch (AmazonClientException e) {
            throw new TranslatorException(e);
        }
    }

    @Override
    public List<String> getDomains() throws TranslatorException {
        return amazonSimpleDBClient.listDomains().getDomainNames();
    }

    @Override
    public Set<SimpleDBAttribute> getAttributeNames(String domainName) throws TranslatorException {
        DomainMetadataRequest domainMetadataRequest = new DomainMetadataRequest(domainName);
        DomainMetadataResult metadataResult = amazonSimpleDBClient.domainMetadata(domainMetadataRequest);
        int attributesCount = metadataResult.getAttributeNameCount();
        SelectResult selectResult = amazonSimpleDBClient.select(new SelectRequest("SELECT * FROM " + SimpleDBUtils.quoteName(domainName))); //$NON-NLS-1$
        return getAttributeNamesFromSelectResult(selectResult, attributesCount);
    }

    @Override
    public int performInsert(String domainName, List<Column> columns, Iterator<? extends List<?>> valueList) throws TranslatorException {
        try {
            int count = 0;
            List<ReplaceableItem> insertItems = new ArrayList<ReplaceableItem>();
            while(valueList.hasNext()) {
                List<?> values = valueList.next();
                List<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>();
                String itemName = null;
                for (int i = 0; i < columns.size(); i++) {
                    Column column = columns.get(i);
                    if (SQLStringVisitor.getRecordName(column).equals(SimpleDBConnection.ITEM_NAME)) {
                        itemName = (String)values.get(i);
                    }
                    else {
                        addAttribute(SQLStringVisitor.getRecordName(column), SimpleDBDataTypeManager.convertToSimpleDBType(values.get(i), column.getJavaType()), attributes);
                    }
                }
                if (itemName == null) {
                    throw new TranslatorException("ItemName() column value is not specified, it can not be null. Please provide a value.");
                }
                insertItems.add(new ReplaceableItem(itemName, attributes));
                count++;
                if (count%25 == 0) {
                    executeBatch(domainName, insertItems);
                    insertItems.clear();
                }
            }
            // http://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SDB_API_BatchPutAttributes.html
            // TODO: 25 limit we may need to batch; but if batch atomicity is gone
            executeBatch(domainName, insertItems);
            return count;
        } catch (AmazonServiceException e) {
            throw new TranslatorException(e);
        } catch (AmazonClientException e) {
            throw new TranslatorException(e);
        }
    }

    @Override
    public SelectResult performSelect(String selectExpression, String nextToken) throws TranslatorException {
        try {
            SelectRequest selectRequest = new SelectRequest(selectExpression);
            if (nextToken != null) {
                selectRequest.setNextToken(nextToken);
            }
            selectRequest.setConsistentRead(true);
            return amazonSimpleDBClient.select(selectRequest);
        } catch (AmazonServiceException e) {
            throw new TranslatorException(e);
        } catch (AmazonClientException e) {
            throw new TranslatorException(e);
        }
    }

    @Override
    public int performUpdate(String domainName, Map<String, Object> updateAttributes, String selectExpression) throws TranslatorException {
        try {
            List<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>();
            for (Map.Entry<String, Object> column : updateAttributes.entrySet()) {
                addAttribute(column.getKey(), column.getValue(), attributes);
            }

            List<ReplaceableItem> updateItems = new ArrayList<ReplaceableItem>();
            int count = 0;
            String nextToken = null;
            do {
                SelectResult result = performSelect(selectExpression, nextToken);
                nextToken = result.getNextToken();
                Iterator<Item> iter = result.getItems().iterator();
                while (iter.hasNext()) {
                    Item item = iter.next();
                    updateItems.add(new ReplaceableItem(item.getName(), attributes));
                    count++;
                    if (count%25 == 0) {
                        executeBatch(domainName, updateItems);
                        updateItems.clear();
                    }
                }
                executeBatch(domainName, updateItems);
            } while (nextToken != null);
            return count;
        } catch (AmazonServiceException e) {
            throw new TranslatorException(e);
        } catch (AmazonClientException e) {
            throw new TranslatorException(e);
        }
    }

    @Override
    public int performDelete(String domainName, String selectExpression) throws TranslatorException {
        try {
            List<DeletableItem> deleteItems = new ArrayList<DeletableItem>();
            int count = 0;
            String nextToken = null;
            do {
                SelectResult result = performSelect(selectExpression, nextToken);
                nextToken = result.getNextToken();
                Iterator<Item> iter = result.getItems().iterator();
                while (iter.hasNext()) {
                    Item item = iter.next();
                    deleteItems.add(new DeletableItem(item.getName(), null));
                    count++;
                    if (count%25 == 0) {
                        BatchDeleteAttributesRequest request = new BatchDeleteAttributesRequest(domainName, deleteItems);
                        this.amazonSimpleDBClient.batchDeleteAttributes(request);
                        deleteItems.clear();
                    }
                }
                // http://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SDB_API_BatchDeleteAttributes.html
                // 25 limit we may need to batch; but if batch atomicity is gone
                if (!deleteItems.isEmpty()) {
                    BatchDeleteAttributesRequest request = new BatchDeleteAttributesRequest(domainName, deleteItems);
                    this.amazonSimpleDBClient.batchDeleteAttributes(request);
                }
            } while (nextToken != null);
            return count;
        } catch (AmazonServiceException e) {
            throw new TranslatorException(e);
        } catch (AmazonClientException e) {
            throw new TranslatorException(e);
        }
    }

    @Override
    public void close() {

    }

    private void executeBatch(String domainName, List<ReplaceableItem> insertItems) {
        if (!insertItems.isEmpty()) {
            BatchPutAttributesRequest request = new BatchPutAttributesRequest(domainName, insertItems);
            this.amazonSimpleDBClient.batchPutAttributes(request);
        }
    }

    void addAttribute(String name, Object value, List<ReplaceableAttribute> attributes) {
        if (value != null && value.getClass().isArray()) {
            String[] values = (String[])value;
            for (int i = 0; i < values.length; i++) {
                addAttribute(name, values[i], attributes);
            }
        }
        else {
            ReplaceableAttribute attribute = new ReplaceableAttribute();
            attribute.setName(name);
            attribute.setReplace(true);
            attribute.setValue((String)value);
            attributes.add(attribute);
        }
    }

    private Set<SimpleDBAttribute> getAttributeNamesFromSelectResult(SelectResult selectResult, int attributesCount) {
        Set<SimpleDBAttribute> attributes = new LinkedHashSet<SimpleDBAttribute>();
        Iterator<Item> itemsIterator = selectResult.getItems().iterator();
        while (attributes.size() < attributesCount) {
            Item item = itemsIterator.next();
            Map<String, List<String>> valueMap = createAttributeMap(item.getAttributes());
            for (String attributeName : valueMap.keySet()) {
                List<String> values = valueMap.get(attributeName);
                attributes.add(new SimpleDBAttribute(attributeName, values.size() > 1));
            }
        }
        return attributes;
    }

    private Map<String, List<String>> createAttributeMap(List<Attribute> attributes) {
        Map<String, List<String>> map = new LinkedHashMap<String, List<String>>();
        for (Attribute attribute : attributes) {
            if (map.get(attribute.getName()) == null) {
                List<String> list = new ArrayList<String>();
                list.add(attribute.getValue());
                map.put(attribute.getName(), list);
            } else {
                map.get(attribute.getName()).add(attribute.getValue());
            }
        }
        return map;
    }
}
