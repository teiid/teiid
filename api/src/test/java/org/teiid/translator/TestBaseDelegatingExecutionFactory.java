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

package org.teiid.translator;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Method;

import org.junit.Test;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;

public class TestBaseDelegatingExecutionFactory {

    @Test public void testMethodOverrides() {
        Method[] methods = ExecutionFactory.class.getDeclaredMethods();
        Method[] proxyMethods = BaseDelegatingExecutionFactory.class.getDeclaredMethods();
        //excluding the setter methods the counts should be equal
        assertEquals(methods.length+103, proxyMethods.length);
    }

    @Test public void testExecution() throws TranslatorException {
        BaseDelegatingExecutionFactory<Void, Void> ef = new BaseDelegatingExecutionFactory<Void, Void>() {
            @Override
            public ResultSetExecution createResultSetExecution(
                    QueryExpression command, ExecutionContext executionContext,
                    RuntimeMetadata metadata, Void connection)
                    throws TranslatorException {
                return null;
            }
        };
        ef.setDelegate(new ExecutionFactory<Void, Void>() {
            @Override
            public Execution createExecution(Command command,
                    ExecutionContext executionContext,
                    RuntimeMetadata metadata, Void connection)
                    throws TranslatorException {
                throw new AssertionError();
            }
        });
        ef.createExecution(new Select(null, false, null, null, null, null, null), null, null, null);
    }

}
