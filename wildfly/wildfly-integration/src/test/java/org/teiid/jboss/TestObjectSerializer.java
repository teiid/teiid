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

package org.teiid.jboss;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jboss.ObjectSerializer;


@SuppressWarnings("nls")
public class TestObjectSerializer {

    @Test public void testLoadSafe() throws Exception {
        ObjectSerializer os = new ObjectSerializer(System.getProperty("java.io.tmpdir"));
        File f = UnitTestUtil.getTestScratchFile("foo");
        os.saveAttachment(f, new Long(2), false);
        assertNotNull(os.loadAttachment(f, Long.class));
        assertNull(os.loadSafe(f, Integer.class));
    }

}
