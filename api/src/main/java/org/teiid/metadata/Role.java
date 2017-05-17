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
package org.teiid.metadata;

import java.util.ArrayList;
import java.util.List;

public class Role extends AbstractMetadataRecord {
    private static final long serialVersionUID = 1379125260214964302L;
    private List<String> jaasRoles;
    private boolean anyAuthenticated;
    
    public Role(String name) {
        super.setName(name);
    }
    
    public List<String> getJassRoles() {
        if (this.jaasRoles != null) {
            return new ArrayList<>(this.jaasRoles);
        }
        return jaasRoles;
    }

    public void setJaasRoles(List<String> jaasRoles) {
        this.jaasRoles = new ArrayList<String>(jaasRoles);
    }

    public boolean isAnyAuthenticated() {
        return this.anyAuthenticated;
    }
    
    public void setAnyAuthenticated(boolean b) {
        this.anyAuthenticated = b;
    }    
}
