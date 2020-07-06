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
package org.teiid.resource.adapter.hdfs;

import java.util.Objects;

import javax.resource.ResourceException;

import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.teiid.core.BundleUtil;
import org.teiid.hdfs.HdfsConfiguration;
import org.teiid.hdfs.HdfsConnection;
import org.teiid.hdfs.HdfsConnectionFactory;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;
import org.teiid.resource.spi.ResourceConnection;
import org.teiid.translator.TranslatorException;

public class HdfsManagedConnectionFactory extends BasicManagedConnectionFactory implements HdfsConfiguration {

    private static final long serialVersionUID = -687763504336137294L;

    public static final BundleUtil UTIL = BundleUtil.getBundleUtil(HdfsManagedConnectionFactory.class);

    private static class HdfsResourceConnection extends HdfsConnection implements ResourceConnection {

        public HdfsResourceConnection(HdfsConnectionFactory connectionFactory) throws TranslatorException {
            super(connectionFactory);
        }

    }

    private String fsUri;
    private String resourcePath;

    @Override
    public BasicConnectionFactory<ResourceConnection> createConnectionFactory()
            throws ResourceException {
        HdfsConnectionFactory hdfsConnectionFactory = new HdfsConnectionFactory(this);

        return new BasicConnectionFactory<ResourceConnection>() {

            @Override
            public ResourceConnection getConnection() throws ResourceException {
                //this may need to be done more generally
                //at least here we need the classloader that may create the filesystem to match
                //the resource adapter - not the translator
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                try {
                    Thread.currentThread().setContextClassLoader(DistributedFileSystem.class.getClassLoader());
                    return new HdfsResourceConnection(hdfsConnectionFactory);
                } catch (TranslatorException e) {
                    throw new ResourceException(e);
                } finally {
                    Thread.currentThread().setContextClassLoader(classLoader);
                }
            }
        };
    }

    @Override
    public String getFsUri() {
        return fsUri;
    }

    @Override
    public String getResourcePath() {
        return resourcePath;
    }

    public void setFsUri(String fsUri) {
        this.fsUri = fsUri;
    }

    public void setResourcePath(String resourcePath) {
        this.resourcePath = resourcePath;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fsUri, resourcePath);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof HdfsManagedConnectionFactory)) {
            return false;
        }
        HdfsManagedConnectionFactory other = (HdfsManagedConnectionFactory) obj;
        return Objects.equals(fsUri, other.fsUri) && Objects.equals(resourcePath, other.resourcePath);
    }

}
