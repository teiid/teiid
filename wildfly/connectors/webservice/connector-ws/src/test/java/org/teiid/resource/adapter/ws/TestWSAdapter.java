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

package org.teiid.resource.adapter.ws;

import javax.resource.ResourceException;
import javax.xml.transform.stream.StreamSource;
import javax.xml.ws.Service.Mode;
import javax.xml.ws.WebServiceException;
import javax.xml.ws.http.HTTPBinding;

import org.junit.Test;
import org.teiid.translator.ws.WSConnection;

public class TestWSAdapter {

    @Test(expected=WebServiceException.class) public void testMissingEndpoint() throws ResourceException {
        WSManagedConnectionFactory wsmcf = new WSManagedConnectionFactory();

        WSConnection conn = (WSConnection)wsmcf.createConnectionFactory().getConnection();
        conn.createDispatch(HTTPBinding.HTTP_BINDING, null, StreamSource.class, Mode.PAYLOAD);
    }

    @Test(expected=WebServiceException.class) public void testMissingEndpoint1() throws ResourceException {
        WSManagedConnectionFactory wsmcf = new WSManagedConnectionFactory();

        WSConnection conn = (WSConnection)wsmcf.createConnectionFactory().getConnection();
        conn.createDispatch(HTTPBinding.HTTP_BINDING, "/x", StreamSource.class, Mode.PAYLOAD); //$NON-NLS-1$
    }

}
