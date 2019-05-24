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

package org.teiid.query.sql.visitor;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.LinkedList;

import org.junit.Test;
import org.teiid.query.resolver.TestResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestCorrelatedReferenceCollectorVisitor {

    @Test public void testDeepNesting() throws Exception {
        String sql = "select * from bqt1.smalla where exists (select intnum from bqt1.smalla x where smalla.intnum = x.intnum and exists (select intnum from bqt1.smalla where exists (select intnum from bqt1.smalla x where smalla.intnum = x.intnum)))";
        Command command = TestResolver.helpResolve(sql, RealMetadataFactory.exampleBQTCached());
        command = QueryRewriter.rewrite(command, RealMetadataFactory.exampleBQTCached(), null);
        command = ((ExistsCriteria)((Query)command).getCriteria()).getCommand();
        LinkedList<Reference> correlatedReferences = new LinkedList<Reference>();
        GroupSymbol gs = new GroupSymbol("bqt1.smalla");
        ResolverUtil.resolveGroup(gs, RealMetadataFactory.exampleBQTCached());
        CorrelatedReferenceCollectorVisitor.collectReferences(command, Arrays.asList(gs), correlatedReferences, RealMetadataFactory.exampleBQTCached());
        assertEquals(1, correlatedReferences.size());
    }

    @Test public void testSubqueryWithoutFromCorrelation() throws Exception {
        String sql = "select (select case when booleanvalue then 'a' else 'b' end) from bqt1.smalla";
        Command command = TestResolver.helpResolve(sql, RealMetadataFactory.exampleBQTCached());
        command = QueryRewriter.rewrite(command, RealMetadataFactory.exampleBQTCached(), null);
        command = ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(command).iterator().next().getCommand();
        LinkedList<Reference> correlatedReferences = new LinkedList<Reference>();
        GroupSymbol gs = new GroupSymbol("bqt1.smalla");
        ResolverUtil.resolveGroup(gs, RealMetadataFactory.exampleBQTCached());
        CorrelatedReferenceCollectorVisitor.collectReferences(command, Arrays.asList(gs), correlatedReferences, RealMetadataFactory.exampleBQTCached());
        assertEquals(1, correlatedReferences.size());
    }

}
