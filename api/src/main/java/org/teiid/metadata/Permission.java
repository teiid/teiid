package org.teiid.metadata;

import java.util.EnumSet;
import java.util.List;

import org.teiid.metadata.Database.ResourceType;

public class Permission extends AbstractMetadataRecord {
    private static final long serialVersionUID = -1445856249500105877L;

    public enum Privilege {
        SELECT, INSERT, UPDATE, DELETE, EXECUTE,
        ALTER, DROP,
        USAGE,
        ALL_PRIVILEGES("ALL PRIVILEGES"), //$NON-NLS-1$
        TEMPORARY_TABLE("TEMPORARY TABLE"), //$NON-NLS-1$
        CREATE;

        private final String toString;

        Privilege(String toString) {
            this.toString = toString;
        }

        Privilege() {
            this.toString = name();
        }

        public String toString() {
            return toString;
        }
    }
    private Database.ResourceType resourceType= null;
    private String resource = null;
    private String mask = null;
    private Integer maskOrder;
    private String condition = null;
    private Boolean isConstraint;
    private EnumSet<Permission.Privilege> privileges = EnumSet.noneOf(Permission.Privilege.class);
    private EnumSet<Permission.Privilege> revokePrivileges = EnumSet.noneOf(Permission.Privilege.class);

    /**
     * The {@link org.teiid.adminapi.DataPolicy.ResourceType}, never null.  Will default
     * to DATABASE
     * @return
     */
    public Database.ResourceType getResourceType() {
        if (resourceType == null) {
            return ResourceType.DATABASE;
        }
        return resourceType;
    }

    public void setResourceType(Database.ResourceType on) {
        this.resourceType = on;
    }

    public String getResourceName() {
        return resource;
    }

    public void setResourceName(String resource) {
        this.resource = resource;
    }

    public String getMask() {
        return mask;
    }

    public void setMask(String mask) {
        this.mask = mask;
    }

    public Integer getMaskOrder() {
        return maskOrder;
    }

    public void setMaskOrder(Integer maskOrder) {
        this.maskOrder = maskOrder;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition, Boolean isConstraint) {
        this.condition = condition;
        this.isConstraint = isConstraint;
    }

    public Boolean isConditionAConstraint() {
        return isConstraint;
    }

    public EnumSet<Permission.Privilege> getPrivileges() {
        return privileges;
    }

    public EnumSet<Permission.Privilege> getRevokePrivileges() {
        return revokePrivileges;
    }

    public Boolean hasPrivilege(Permission.Privilege allow) {
        if (this.privileges.contains(allow)) {
            return true;
        }
        if (this.revokePrivileges.contains(allow)) {
            return false;
        }
        return null;
    }

    public void setPrivileges(List<Permission.Privilege> types) {
        if (types == null ||types.isEmpty()) {
            return;
        }
        this.privileges = EnumSet.copyOf(types);
    }

    public void setRevokePrivileges(List<Permission.Privilege> types) {
        if (types == null ||types.isEmpty()) {
            return;
        }
        this.revokePrivileges = EnumSet.copyOf(types);
    }

    public void appendPrivileges(EnumSet<Permission.Privilege> types) {
        if (types == null ||types.isEmpty()) {
            return;
        }
        for (Permission.Privilege a:types) {
            this.privileges.add(a);
            this.revokePrivileges.remove(a);
        }
    }

    public void removePrivileges(EnumSet<Permission.Privilege> types) {
        if (types == null ||types.isEmpty()) {
            return;
        }
        for (Permission.Privilege a:types) {
            if (!this.privileges.remove(a)) {
                this.revokePrivileges.add(a);
            }
        }
    }

    private void setAllows(Boolean allow, Permission.Privilege privilege) {
        if(allow!= null) {
            if (allow) {
                this.revokePrivileges.remove(privilege);
                this.privileges.add(privilege);
            } else {
                if (!this.privileges.remove(privilege)) {
                    this.revokePrivileges.add(privilege);
                }
            }
        }
    }

    public void setAllowSelect(Boolean allow) {
        setAllows(allow, Privilege.SELECT);
    }

    public void setAllowAlter(Boolean allow) {
        setAllows(allow, Privilege.ALTER);
    }

    public void setAllowInsert(Boolean allow) {
        setAllows(allow, Privilege.INSERT);
    }

    public void setAllowDelete(Boolean allow) {
        setAllows(allow, Privilege.DELETE);
    }

    public void setAllowExecute(Boolean allow) {
        setAllows(allow, Privilege.EXECUTE);
    }

    public void setAllowUpdate(Boolean allow) {
        setAllows(allow, Privilege.UPDATE);
    }

    public void setAllowDrop(Boolean allow) {
        setAllows(allow, Privilege.DROP);
    }

    public void setAllowUsage(Boolean allow) {
        setAllows(allow, Privilege.USAGE);
    }
    public void setAllowAllPrivileges(Boolean allow) {
        setAllows(allow, Privilege.ALL_PRIVILEGES);
    }
    public void setAllowTemporyTables(Boolean allow) {
        setAllows(allow, Privilege.TEMPORARY_TABLE);
    }

}