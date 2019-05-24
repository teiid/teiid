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

package org.teiid.resource.adapter.simpledb;

import javax.resource.ResourceException;

import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

public class SimpleDBManagedConnectionFactory extends BasicManagedConnectionFactory {

    private static final long serialVersionUID = -1346340853914009086L;

    private String accessKey;
    private String secretAccessKey;

    @Override
    @SuppressWarnings("serial")
    public BasicConnectionFactory<SimpleDBConnectionImpl> createConnectionFactory()
            throws ResourceException {
        return new BasicConnectionFactory<SimpleDBConnectionImpl>() {

            @Override
            public SimpleDBConnectionImpl getConnection()
                    throws ResourceException {
                return new SimpleDBConnectionImpl(accessKey, secretAccessKey);
            }

        };
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public void setSecretAccessKey(String secretAccessKey) {
        this.secretAccessKey = secretAccessKey;
    }

}
