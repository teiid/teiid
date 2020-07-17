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

import java.util.Objects;

import javax.resource.ResourceException;

import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;
import org.teiid.resource.spi.ResourceConnection;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.simpledb.api.BaseSimpleDBConfiguration;
import org.teiid.translator.simpledb.api.SimpleDBConnectionFactory;
import org.teiid.translator.simpledb.api.SimpleDBConnectionImpl;

import com.amazonaws.services.simpledb.AmazonSimpleDB;

public class SimpleDBManagedConnectionFactory extends BasicManagedConnectionFactory implements BaseSimpleDBConfiguration {

    private static final long serialVersionUID = -1346340853914009086L;

    private static class SimpleDBResourceConnection extends SimpleDBConnectionImpl implements ResourceConnection {

        public SimpleDBResourceConnection(AmazonSimpleDB amazonSimpleDBClient) {
            super(amazonSimpleDBClient);
        }
    }

    private String accessKey;
    private String secretAccessKey;

    @Override
    public BasicConnectionFactory<ResourceConnection> createConnectionFactory() throws ResourceException {
        try {
            SimpleDBConnectionFactory simpleDBConnectionFactory = new SimpleDBConnectionFactory(SimpleDBManagedConnectionFactory.this);
            return new BasicConnectionFactory<ResourceConnection>() {

                @Override
                public ResourceConnection getConnection() throws ResourceException {
                    return new SimpleDBResourceConnection(simpleDBConnectionFactory.getSimpleDBClient());
                }
            };
        } catch (TranslatorException e) {
            throw new ResourceException(e);
        }
    }

    @Override
    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    @Override
    public String getSecretKey() {
        return getSecretAccessKey();
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public void setSecretAccessKey(String secretAccessKey) {
        this.secretAccessKey = secretAccessKey;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(accessKey);
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(!(obj instanceof SimpleDBManagedConnectionFactory)) {
            return false;
        }
        SimpleDBManagedConnectionFactory other = (SimpleDBManagedConnectionFactory) obj;
        return Objects.equals(accessKey, other.accessKey)
                && Objects.equals(secretAccessKey, other.secretAccessKey);
    }
}
