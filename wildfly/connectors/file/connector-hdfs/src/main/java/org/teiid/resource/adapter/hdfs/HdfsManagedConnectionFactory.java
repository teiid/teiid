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

import javax.resource.ResourceException;

import org.apache.curator.shaded.com.google.common.base.Objects;
import org.teiid.core.BundleUtil;
import org.teiid.hdfs.HdfsConfiguration;
import org.teiid.hdfs.HdfsConnection;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;
import org.teiid.resource.spi.ResourceConnection;
import org.teiid.translator.TranslatorException;

public class HdfsManagedConnectionFactory extends BasicManagedConnectionFactory implements HdfsConfiguration {

    private static final long serialVersionUID = -687763504336137294L;

    public static final BundleUtil UTIL = BundleUtil.getBundleUtil(HdfsManagedConnectionFactory.class);

    private static class HdfsResourceConnection extends HdfsConnection implements ResourceConnection {

        public HdfsResourceConnection(HdfsConfiguration config)
                throws TranslatorException {
            super(config);
        }

    }

    private String fsUri;
    private String resourcePath;

    @Override
    public BasicConnectionFactory<ResourceConnection> createConnectionFactory()
            throws ResourceException {
        return new BasicConnectionFactory<ResourceConnection>() {

            @Override
            public ResourceConnection getConnection() throws ResourceException {
                try {
                    return new HdfsResourceConnection(HdfsManagedConnectionFactory.this);
                } catch (TranslatorException e) {
                    throw new ResourceException(e);
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
        return Objects.hashCode(fsUri, resourcePath);
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
        return Objects.equal(fsUri, other.fsUri) && Objects.equal(resourcePath, other.resourcePath);
    }

}
