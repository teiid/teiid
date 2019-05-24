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

package org.teiid.query.sql.lang;

import java.util.HashSet;
import java.util.Set;

import org.teiid.query.validator.UpdateValidator.UpdateInfo;


public abstract class ProcedureContainer extends Command implements TargetedCommand {

    private int updateCount = -1;
    private UpdateInfo updateInfo;
    private Set<String> tags;

    protected void copyMetadataState(ProcedureContainer copy) {
        super.copyMetadataState(copy);
        copy.setUpdateInfo(this.getUpdateInfo());
        copy.updateCount = updateCount;
        if (tags != null) {
            copy.tags = new HashSet<String>(tags);
        }
    }

    /**
     * @return Returns the updateCount.
     */
    public int getUpdateCount() {
        return this.updateCount;
    }


    /**
     * @param updateCount The updateCount to set.
     */
    public void setUpdateCount(int updateCount) {
        if (updateCount < 0) {
            return;
        }
        if (updateCount > 2) {
            updateCount = 2;
        }
        this.updateCount = updateCount;
    }

    public UpdateInfo getUpdateInfo() {
        return updateInfo;
    }

    public void setUpdateInfo(UpdateInfo updateInfo) {
        this.updateInfo = updateInfo;
    }

    public boolean hasTag(String name) {
        return tags != null && tags.contains(name);
    }

    public void addTag(String name) {
        if (tags == null) {
            tags = new HashSet<String>();
        }
        tags.add(name);
    }

}
