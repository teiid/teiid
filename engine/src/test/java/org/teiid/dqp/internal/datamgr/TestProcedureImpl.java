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

package org.teiid.dqp.internal.datamgr;

import junit.framework.TestCase;

import org.teiid.language.Call;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.StoredProcedure;


public class TestProcedureImpl extends TestCase {

    /**
     * Constructor for TestExecuteImpl.
     * @param name
     */
    public TestProcedureImpl(String name) {
        super(name);
    }

    public static Call example() throws Exception {
        String sql = "EXEC pm1.sq3('x', 1)"; //$NON-NLS-1$
        Command command = new QueryParser().parseCommand(sql);
        QueryResolver.resolveCommand(command, TstLanguageBridgeFactory.metadata);
        return TstLanguageBridgeFactory.factory.translate((StoredProcedure)command);
    }

    public void testGetProcedureName() throws Exception {
        assertEquals("sq3", example().getProcedureName()); //$NON-NLS-1$
    }

    public void testGetParameters() throws Exception {
        Call exec = example();
        assertNotNull(exec.getArguments());
        assertEquals(2, exec.getArguments().size());
    }

}
