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

package org.teiid.translator.file;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.file.JavaVirtualFile;
import org.teiid.file.VirtualFileConnection;
import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Call;
import org.teiid.language.Literal;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TypeFacility;

@SuppressWarnings("nls")
public class TestFileExecutionFactory {

    @Test public void testGetTextFiles() throws Exception {
        FileExecutionFactory fef = new FileExecutionFactory();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "text", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        fef.getMetadata(mf, null);
        Procedure p = mf.getSchema().getProcedure("getTextFiles");
        VirtualFileConnection fc = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(fc.getFiles("*.txt")).toReturn(JavaVirtualFile.getFiles("*.txt", new File(UnitTestUtil.getTestDataPath(), "*.txt")));
        Call call = fef.getLanguageFactory().createCall("getTextFiles", Arrays.asList(new Argument(Direction.IN, new Literal("*.txt", TypeFacility.RUNTIME_TYPES.STRING), TypeFacility.RUNTIME_TYPES.STRING, null)), p);
        ProcedureExecution pe = fef.createProcedureExecution(call, null, null, fc);
        pe.execute();
        int count = 0;
        while (true) {
            List<?> val = pe.next();
            if (val == null) {
                break;
            }
            assertEquals(5, val.size());
            assertTrue(val.get(3) instanceof Timestamp);
            assertEquals(Long.valueOf(0), val.get(4));
            count++;
        }
        assertEquals(2, count);


        call = fef.getLanguageFactory().createCall("getTextFiles", Arrays.asList(new Argument(Direction.IN, new Literal("*1*", TypeFacility.RUNTIME_TYPES.STRING), TypeFacility.RUNTIME_TYPES.STRING, null)), p);
        pe = fef.createProcedureExecution(call, null, null, fc);
        Mockito.stub(fc.getFiles("*1*")).toReturn(JavaVirtualFile.getFiles("*1*", new File(UnitTestUtil.getTestDataPath(), "*1*")));
        pe.execute();
        count = 0;
        while (true) {
            if (pe.next() == null) {
                break;
            }
            count++;
        }
        assertEquals(1, count);
    }

}
