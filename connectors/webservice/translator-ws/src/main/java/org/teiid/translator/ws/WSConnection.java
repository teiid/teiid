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

package org.teiid.translator.ws;

import java.io.IOException;
import java.net.URL;

import javax.xml.namespace.QName;
import javax.xml.ws.Dispatch;
import javax.xml.ws.Service;

import org.teiid.resource.api.Connection;

/**
 * Simple interface for web services
 */
public interface WSConnection extends Connection {

    public static final String STATUS_CODE = "status-code"; //$NON-NLS-1$

    <T> Dispatch<T> createDispatch(String binding, String endpoint, Class<T> type, Service.Mode mode);

    <T> Dispatch<T> createDispatch(Class<T> type, Service.Mode mode) throws IOException;

    URL getWsdl();

    QName getServiceQName();

    QName getPortQName();

    String getStatusMessage(int status);

    String getEndPoint();

}
