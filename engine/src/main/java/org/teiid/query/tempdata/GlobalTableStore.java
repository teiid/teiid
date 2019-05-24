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

package org.teiid.query.tempdata;

import java.io.Serializable;
import java.util.List;

import org.teiid.Replicated;
import org.teiid.Replicated.ReplicationMode;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.tempdata.GlobalTableStoreImpl.MatTableInfo;

public interface GlobalTableStore {

    TempMetadataID getGlobalTempTableMetadataId(Object groupID) throws TeiidComponentException, TeiidProcessingException;

    TempMetadataID getGlobalTempTableMetadataId(String matTableName);

    TempMetadataID getCodeTableMetadataId(String codeTableName,
            String returnElementName, String keyElementName,
            String matTableName) throws TeiidComponentException,
            QueryMetadataException;

    MatTableInfo getMatTableInfo(String matTableName);

    TempTable getTempTable(String matTableName);

    Serializable getAddress();

    List<?> updateMatViewRow(String matTableName, List<?> tuple, boolean delete) throws TeiidComponentException;

    TempTable createMatTable(String tableName, GroupSymbol group)
    throws TeiidComponentException, QueryMetadataException, TeiidProcessingException;

    @Replicated
    void failedLoad(String matTableName);

    @Replicated(asynch=false, timeout=5000)
    boolean needsLoading(String matTableName, Serializable loadingAddress,
            boolean firstPass, boolean refresh, boolean invalidate);

    @Replicated(replicateState=ReplicationMode.PUSH)
    void loaded(String matTableName, TempTable table);

}
