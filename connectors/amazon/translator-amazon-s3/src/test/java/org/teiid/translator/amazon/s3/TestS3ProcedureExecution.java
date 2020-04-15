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

package org.teiid.translator.amazon.s3;

import static org.junit.Assert.*;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.ws.WSConnection;

@SuppressWarnings("nls")
public class TestS3ProcedureExecution {

    @Test public void testEndpoint() throws TranslatorException {
        WSConnection conn = Mockito.mock(WSConnection.class);
        S3ProcedureExecution s3ProcedureExecution = new S3ProcedureExecution(null, null, null, null, conn);
        assertEquals("https://s3.z.amazonaws.com/y/x", s3ProcedureExecution.determineEndpoint("x", "y", "z"));

        Mockito.stub(conn.getEndPoint()).toReturn("http://localhost:9000");
        s3ProcedureExecution = new S3ProcedureExecution(null, null, null, null, conn);
        assertEquals("http://localhost:9000/y/x", s3ProcedureExecution.determineEndpoint("x", "y", "z"));
    }

}
