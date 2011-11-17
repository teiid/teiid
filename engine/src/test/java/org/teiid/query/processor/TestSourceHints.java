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

package org.teiid.query.processor;

import static org.junit.Assert.*;
import static org.teiid.query.processor.TestProcessor.*;

import java.util.List;

import org.junit.Test;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TestSourceHints {

	@Test public void testUserQueryHint() {
		String sql = "SELECT /*+ sh:'foo' bar:'leading' */ e1 from pm1.g1 order by e1 limit 1"; //$NON-NLS-1$
		
		ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        List<?>[] expected = new List[] {};
        helpProcess(plan, manager("foo", "leading"), expected);
	}
	
	@Test public void testHintInView() {
    	MetadataStore metadataStore = new MetadataStore();
        Schema p1 = RealMetadataFactory.createPhysicalModel("p1", metadataStore); //$NON-NLS-1$
        Table t1 = RealMetadataFactory.createPhysicalGroup("t", p1); //$NON-NLS-1$
        RealMetadataFactory.createElements(t1, new String[] {"a", "b" }, new String[] { "string", "string" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        
        Schema v1 = RealMetadataFactory.createVirtualModel("v1", metadataStore); //$NON-NLS-1$
        QueryNode n1 = new QueryNode("SELECT /*+ sh:'x' */ a as c, b FROM p1.t"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vt1 = RealMetadataFactory.createVirtualGroup("t1", v1, n1); //$NON-NLS-1$
        RealMetadataFactory.createElements(vt1, new String[] {"c", "b" }, new String[] { "string", "string" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$

        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(metadataStore, "metadata");
        
        //top level applies
        HardcodedDataManager manager = manager("foo", "leading");
		
		String sql = "SELECT /*+ sh:'foo' bar:'leading' */ c from t1 order by c limit 1"; //$NON-NLS-1$
		ProcessorPlan plan = helpGetPlan(sql, metadata);
        
        List<?>[] expected = new List[] {};
        helpProcess(plan, manager, expected);
        
        //use the underlying hint
        manager = manager("x", null);
		sql = "SELECT c from t1 order by c limit 1"; //$NON-NLS-1$
		plan = helpGetPlan(sql, metadata);
        helpProcess(plan, manager, expected);
        
        //use no hints
        manager = manager(null, null);
		sql = "SELECT c from t1 union all select c from t1"; //$NON-NLS-1$
		plan = helpGetPlan(sql, metadata);
        helpProcess(plan, manager, expected);
	}

	private HardcodedDataManager manager(final String general, final String hint) {
		HardcodedDataManager manager = new HardcodedDataManager() {
			@Override
			public TupleSource registerRequest(CommandContext context,
					Command command, String modelName,
					String connectorBindingId, int nodeID, int limit)
					throws TeiidComponentException {
				if (general == null && hint == null) {
					assertNull(context.getSourceHint());
				} else {
					assertEquals(general, context.getSourceHint().getGeneralHint()); //$NON-NLS-1$
					assertEquals(hint, context.getSourceHint().getSourceHint("bar")); //$NON-NLS-1$
				}
				return CollectionTupleSource.createNullTupleSource();
			}
		};
		return manager;
	}
	
}
