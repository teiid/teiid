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

/**
 * Represents a grant / revoke.  We do not yet store grants, just permissions,
 * as there is no identity to the grantor.
 */
public class Grant extends AbstractMetadataRecord {
    private static final long serialVersionUID = 3728259393244582775L;

    //private String grantor;
    private String role;
    private Permission permission;

    public Grant() {
    }

    public Grant(String role, Permission permission) {
        this.role = role;
        this.permission = permission;
    }

    public Permission getPermission() {
        return permission;
    }

    public void setPermission(Permission permission) {
        this.permission = permission;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }
}
