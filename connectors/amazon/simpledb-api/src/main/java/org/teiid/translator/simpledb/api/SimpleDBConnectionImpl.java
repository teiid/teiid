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

import com.amazonaws.services.simpledb.model.SelectResult;
import org.teiid.metadata.Column;
import org.teiid.translator.TranslatorException;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SimpleDBConnectionImpl implements SimpleDBConnection {
    @Override
    public void createDomain(String domainName) throws TranslatorException {

    }

    @Override
    public void deleteDomain(String domainName) throws TranslatorException {

    }

    @Override
    public List<String> getDomains() throws TranslatorException {
        return null;
    }

    @Override
    public Set<SimpleDBAttribute> getAttributeNames(String domainName) throws TranslatorException {
        return null;
    }

    @Override
    public int performInsert(String domainName, List<Column> columns, Iterator<? extends List<?>> values) throws TranslatorException {
        return 0;
    }

    @Override
    public SelectResult performSelect(String selectExpression, String nextToken) throws TranslatorException {
        return null;
    }

    @Override
    public int performUpdate(String domainName, Map<String, Object> updateAttributes, String selectExpression) throws TranslatorException {
        return 0;
    }

    @Override
    public int performDelete(String domainName, String selectExpression) throws TranslatorException {
        return 0;
    }

    @Override
    public void close() throws Exception {

    }
}
