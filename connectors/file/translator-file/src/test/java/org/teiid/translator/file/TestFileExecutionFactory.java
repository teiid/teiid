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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.teiid.core.types.BlobType;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.file.JavaVirtualFile;
import org.teiid.file.VirtualFile;
import org.teiid.file.VirtualFileConnection;
import org.teiid.file.VirtualFileConnection.FileMetadata;
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
        Mockito.stub(fc.getFiles("*.txt")).toReturn(new VirtualFile[]{new JavaVirtualFile(UnitTestUtil.getTestDataFile("file.txt")), new JavaVirtualFile(UnitTestUtil.getTestDataFile("file1.txt"))});
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
    }

    @Test public void testSaveFileLength() throws Exception {
        FileExecutionFactory fef = new FileExecutionFactory();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "text", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        fef.getMetadata(mf, null);
        Procedure p = mf.getSchema().getProcedure(FileExecutionFactory.SAVEFILE);
        VirtualFileConnection fc = Mockito.mock(VirtualFileConnection.class);
        int length = 100;
        BlobType blob = new BlobType(new byte[length]);
        Call call = fef.getLanguageFactory().createCall(FileExecutionFactory.SAVEFILE, Arrays.asList(
                new Argument(Direction.IN, new Literal("path", TypeFacility.RUNTIME_TYPES.STRING), TypeFacility.RUNTIME_TYPES.STRING, null),
                new Argument(Direction.IN, new Literal(blob, TypeFacility.RUNTIME_TYPES.BLOB), TypeFacility.RUNTIME_TYPES.BLOB, null)), p);
        ProcedureExecution pe = fef.createProcedureExecution(call, null, null, fc);
        pe.execute();
        ArgumentCaptor<FileMetadata> argument = ArgumentCaptor.forClass(FileMetadata.class);
        Mockito.verify(fc).add(Mockito.any(InputStream.class), Mockito.eq("path"), argument.capture());
        FileMetadata fm = argument.getValue();
        assertEquals(Long.valueOf(length), fm.size());
        assertNull(pe.next());
    }

}
