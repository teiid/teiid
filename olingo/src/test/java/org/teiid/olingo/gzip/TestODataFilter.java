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

package org.teiid.olingo.gzip;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.olingo.web.ODataFilter;

import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@SuppressWarnings("nls")
public class TestODataFilter {

    @Test public void testVdbVersion() throws Exception {
        ODataFilter filter = new ODataFilter();
        FilterConfig config = Mockito.mock(FilterConfig.class);
        ServletContext mock = Mockito.mock(ServletContext.class);
        Mockito.when(mock.getInitParameterNames()).thenReturn(Collections.emptyEnumeration());
        Mockito.when(config.getServletContext()).thenReturn(mock);
        Mockito.when(config.getInitParameterNames()).thenReturn(Collections.emptyEnumeration());

        //default to 1
        filter.init(config);
        assertEquals("1", filter.getDefaultVdbVersion());

        //override
        Mockito.when(config.getInitParameter("explicit-vdb-version")).thenReturn("false");
        filter.init(config);
        assertNull(filter.getDefaultVdbVersion());
    }

}
