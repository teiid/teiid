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
package org.teiid.resource.adapter.solr;


import org.teiid.resource.spi.BasicConnectionFactory;

public class SolrConnectionFactory extends BasicConnectionFactory<SolrConnectionImpl>{
    private static final long serialVersionUID = 7636834759365334558L;
    private SolrManagedConnectionFactory config;

    public SolrConnectionFactory(SolrManagedConnectionFactory config) {
        this.config = config;
    }

    @Override
    public SolrConnectionImpl getConnection() {
        return new SolrConnectionImpl(config);
    }
}
