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
		CorrelatedReferenceCollectorVisitor.collectReferences(command, Arrays.asList(gs), correlatedReferences);
		assertEquals(1, correlatedReferences.size());
	}

}
