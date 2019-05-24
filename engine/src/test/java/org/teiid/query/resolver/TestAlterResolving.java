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
import org.teiid.query.sql.lang.AlterProcedure;
import org.teiid.query.sql.lang.AlterTrigger;
import org.teiid.query.sql.lang.AlterView;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.proc.CommandStatement;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestAlterResolving {

    @Test public void testAlterView() {
        AlterView alterView = (AlterView) helpResolve("alter view SmallA_2589 as select 2", RealMetadataFactory.exampleBQTCached());
        assertNotNull(alterView.getTarget().getMetadataID());
    }

    @Test public void testAlterProcedure() {
        AlterProcedure alterProc = (AlterProcedure) helpResolve("alter procedure MMSP5 as begin select param1; end", RealMetadataFactory.exampleBQTCached());
        assertNotNull(alterProc.getTarget().getMetadataID());
        Query q = (Query)((CommandStatement)alterProc.getDefinition().getBlock().getStatements().get(0)).getCommand();
        assertTrue(((ElementSymbol)q.getSelect().getSymbol(0)).isExternalReference());
    }

    @Test public void testAlterTriggerInsert() {
        AlterTrigger alterTrigger = (AlterTrigger) helpResolve("alter trigger on SmallA_2589 instead of insert as for each row begin atomic select new.intkey; end", RealMetadataFactory.exampleBQTCached());
        assertNotNull(alterTrigger.getTarget().getMetadataID());
    }

    @Test public void testAlterTriggerInsert_Invalid() {
        helpResolveException("alter trigger on SmallA_2589 instead of insert as for each row begin atomic select old.intkey; end", RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testAlterView_Invalid() {
        helpResolveException("alter view bqt1.SmallA as select 2", RealMetadataFactory.exampleBQTCached());
    }

}
