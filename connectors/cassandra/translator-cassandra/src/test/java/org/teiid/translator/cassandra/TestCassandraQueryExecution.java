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

package org.teiid.translator.cassandra;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

public class TestCassandraQueryExecution {

    @Test public void testGetRowWithNull() {
        CassandraQueryExecution cqe = new CassandraQueryExecution(null, null, null);
        Row row = Mockito.mock(Row.class);
        Mockito.stub(row.isNull(0)).toReturn(true);
        ColumnDefinitions cd = Mockito.mock(ColumnDefinitions.class);
        Mockito.stub(row.getColumnDefinitions()).toReturn(cd);
        Mockito.stub(cd.size()).toReturn(1);
        Mockito.stub(cd.getType(0)).toReturn(DataType.cint());
        List<?> val = cqe.getRow(row);
        assertNull(val.get(0));
    }

}
