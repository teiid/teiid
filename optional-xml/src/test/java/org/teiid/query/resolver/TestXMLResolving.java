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

package org.teiid.query.resolver;

import static org.junit.Assert.*;
import static org.teiid.query.resolver.TestResolver.*;

import org.junit.Test;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.XMLQuery;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestXMLResolving {

    @Test public void testXmlTableWithParam() {
        helpResolve("select * from xmltable('/a' passing ?) as x", RealMetadataFactory.example1Cached());
    }

    @Test public void testXmlQueryWithParam() {
        Query q = (Query)helpResolve("select xmlquery('/a' passing ?)", RealMetadataFactory.example1Cached());
        XMLQuery ex = (XMLQuery) SymbolMap.getExpression(q.getSelect().getSymbols().get(0));
        assertEquals(DataTypeManager.DefaultDataClasses.XML, ex.getPassing().get(0).getExpression().getType());
    }

}
