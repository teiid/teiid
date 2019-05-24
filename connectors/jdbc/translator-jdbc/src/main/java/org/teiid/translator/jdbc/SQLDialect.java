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

package org.teiid.translator.jdbc;

import org.hibernate.dialect.Dialect;
import org.hibernate.hql.spi.id.AbstractMultiTableBulkIdStrategyImpl;

/**
 * A pruned version of a Hibernate {@link Dialect} for use by Teiid
 */
public interface SQLDialect {

    public AbstractMultiTableBulkIdStrategyImpl getDefaultMultiTableBulkIdStrategy();

    //TODO: there's a chance that the type is not supported by the source
    //which will throw a HibernateException - this is likely a modeling error
    //rather than something we need to generally consider
    public String getTypeName(int code, long length, int precision, int scale);


}
