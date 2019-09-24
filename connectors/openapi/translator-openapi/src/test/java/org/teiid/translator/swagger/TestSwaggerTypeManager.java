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
package org.teiid.translator.swagger;

import static org.junit.Assert.assertEquals;

import java.sql.Timestamp;
import java.util.TimeZone;

import org.junit.Test;
public class TestSwaggerTypeManager {
    @Test
    public void testTimeStamp() throws Exception {
        TimeZone tz = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
            assertEquals("2016-04-08 10:14:23.000000006",
                    SwaggerTypeManager.formTimestamp("2016-04-08T10:14:23.6Z").toString());
            assertEquals("2016-04-08 04:14:23.0",
                    SwaggerTypeManager.formTimestamp("2016-04-08T10:14:23+06:00").toString());
            assertEquals("2016-04-08T04:14:23.0Z",
                    SwaggerTypeManager.timestampToString(new Timestamp(1460088863000L)));
        } finally {
           TimeZone.setDefault(tz);
        }
    }
}
