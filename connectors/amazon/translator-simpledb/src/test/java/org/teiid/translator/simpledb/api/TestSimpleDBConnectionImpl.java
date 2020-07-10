/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.simpledb.api;

import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.*;
import com.amazonaws.services.simpledb.util.SimpleDBUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.translator.TranslatorException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class TestSimpleDBConnectionImpl {
    private SimpleDBConnectionImpl simpleDBConnection;
    private AmazonSimpleDBClient simpleDBClient = Mockito.mock(AmazonSimpleDBClient.class);

    @Before
    public void setUp() {
        simpleDBConnection = new SimpleDBConnectionImpl(simpleDBClient);
    }


    @Test
    public void testCreateDomain() throws TranslatorException {
        String domainName = "abc";
        simpleDBConnection.createDomain(domainName);
        verify(simpleDBClient, times(1)).createDomain(new CreateDomainRequest(domainName));
    }

    @Test
    public void testDeleteDomain() throws TranslatorException {
        String domainName = "abc";
        simpleDBConnection.deleteDomain(domainName);
        verify(simpleDBClient, times(1)).deleteDomain(new DeleteDomainRequest(domainName));
    }

    @Test
    public void testGetDomains() throws TranslatorException {
        ListDomainsResult listDomainsResult = Mockito.mock(ListDomainsResult.class);
        List<String> domainName = Arrays.asList(new String[]{"abc", "def"});
        when(listDomainsResult.getDomainNames()).thenReturn(domainName);
        when(simpleDBClient.listDomains()).thenReturn(listDomainsResult);

        assertEquals(domainName, simpleDBConnection.getDomains());
    }

    @Test
    public void testGetAttributeNames() throws TranslatorException {
        String domainName = "abc";
        DomainMetadataResult domainMetadataResult = Mockito.mock(DomainMetadataResult.class);
        when(domainMetadataResult.getAttributeNameCount()).thenReturn(0);
        when(simpleDBClient.domainMetadata(any(DomainMetadataRequest.class))).thenReturn(domainMetadataResult);

        SelectResult selectResult = Mockito.mock(SelectResult.class);
        when(simpleDBClient.select(new SelectRequest("SELECT * FROM " + SimpleDBUtils.quoteName(domainName))))
                .thenReturn(selectResult);

        simpleDBConnection.getAttributeNames(domainName);
    }

    @Test
    public void testAddNullAttribute() throws Exception {
        List<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>();
        simpleDBConnection.addAttribute("x", null, attributes);
        assertNull(attributes.get(0).getValue());
    }
}