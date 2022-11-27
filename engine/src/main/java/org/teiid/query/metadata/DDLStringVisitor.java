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
package org.teiid.query.metadata;

import static org.teiid.language.SQLConstants.NonReserved.*;
import static org.teiid.language.SQLConstants.Reserved.*;
import static org.teiid.language.SQLConstants.Tokens.*;
import static org.teiid.query.metadata.DDLConstants.*;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.teiid.adminapi.Admin.SchemaObjectType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SQLConstants;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.metadata.*;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Database.ResourceType;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.Permission.Privilege;
import org.teiid.metadata.Policy.Operation;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class DDLStringVisitor {
    private static final String TAB = "\t"; //$NON-NLS-1$
    private static final String NEWLINE = "\n";//$NON-NLS-1$
    public static final String GENERATED = "TEIID_GENERATED"; //$NON-NLS-1$

    private static final HashSet<String> PRECISION_DATATYPES = new HashSet<String>(
            Arrays.asList(DataTypeManager.DefaultDataTypes.BIG_DECIMAL));

    protected StringBuilder buffer = new StringBuilder();
    private boolean includeTables = true;
    private boolean includeProcedures = true;
    private boolean includeFunctions = true;
    private Pattern filter;

    public static String getDDLString(Schema schema, EnumSet<SchemaObjectType> types, String regexPattern) {
        DDLStringVisitor visitor = new DDLStringVisitor(types, regexPattern);
        visitor.visit(schema);
        return visitor.toString();
    }

    public static String getDDLString(Database database) {
        DDLStringVisitor visitor = new DDLStringVisitor(null, null);
        visitor.visit(database);
        return visitor.toString();
    }

    public DDLStringVisitor(EnumSet<SchemaObjectType> types, String regexPattern) {
        if (types != null) {
            this.includeTables = types.contains(SchemaObjectType.TABLES);
            this.includeProcedures = types.contains(SchemaObjectType.PROCEDURES);
            this.includeFunctions = types.contains(SchemaObjectType.FUNCTIONS);
        }
        if (regexPattern != null) {
            this.filter = Pattern.compile(regexPattern);
        }
    }

    public void visit(Database database) {
        append(NEWLINE);
        append("/*").append(NEWLINE);
        append("###########################################").append(NEWLINE);
        append("# START DATABASE ").append(database.getName()).append(NEWLINE);
        append("###########################################").append(NEWLINE);
        append("*/").append(NEWLINE);

        append(CREATE).append(SPACE).append(DATABASE).append(SPACE)
                .append(SQLStringVisitor.escapeSinglePart(database.getName())).append(SPACE).append(VERSION)
                .append(SPACE).append(new Constant(database.getVersion()));
        appendOptions(database);
        append(SEMICOLON);
        append(NEWLINE);
        append(USE).append(SPACE).append(DATABASE).append(SPACE);
        append(SQLStringVisitor.escapeSinglePart(database.getName())).append(SPACE);
        append(VERSION).append(SPACE).append(new Constant(database.getVersion()));
        append(SEMICOLON);
        append(NEWLINE);

        boolean outputDt = false;
        for (Datatype dt : database.getMetadataStore().getDatatypes().values()) {
            if (dt.getType() == Datatype.Type.Domain) {
                outputDt = true;
                break;
            }
        }

        if (outputDt) {
            append(NEWLINE);
            append("--############ Domains ############");
            append(NEWLINE);

            for (Datatype dt : database.getMetadataStore().getDatatypes().values()) {
                if (dt.isBuiltin()) {
                    continue;
                }
                visit(dt);
                append(NEWLINE);
                append(NEWLINE);
            }
        }

        if (!database.getDataWrappers().isEmpty()){
            append(NEWLINE);
            append("--############ Translators ############");
            append(NEWLINE);
            for (DataWrapper dw : database.getDataWrappers()) {
                visit(dw);
            }
        }

        if (!database.getServers().isEmpty()){
            append(NEWLINE);
            append("--############ Servers ############");
            append(NEWLINE);
            for (Server server : database.getServers()) {
                visit(server);
                append(NEWLINE);
                append(NEWLINE);
            }
        }

        if (!database.getSchemas().isEmpty()) {
            append(NEWLINE);
            append("--############ Schemas ############");
            append(NEWLINE);

            for (Schema schema : database.getSchemas()) {
                append(CREATE);
                if (!schema.isPhysical()) {
                    append(SPACE).append(VIRTUAL);
                }
                append(SPACE).append(SCHEMA).append(SPACE).append(SQLStringVisitor.escapeSinglePart(schema.getName()));
                if (!schema.getServers().isEmpty()) {
                    append(SPACE).append(SERVER);
                    boolean first = true;
                    for (Server s:schema.getServers()) {
                        if (first) {
                            first = false;
                        } else {
                            append(COMMA);
                        }
                        append(SPACE).append(SQLStringVisitor.escapeSinglePart(s.getName()));
                    }
                }

                appendOptions(schema);
                append(SEMICOLON);
                append(NEWLINE);
                append(NEWLINE);
                createdSchmea(schema);
            }
        }

        if (!database.getRoles().isEmpty()){
            append(NEWLINE);
            append("--############ Roles ############");
            append(NEWLINE);
            for (Role role:database.getRoles()) {
                visit(role);
                append(NEWLINE);
                append(NEWLINE);
            }
        }

        for (Schema schema : database.getSchemas()) {
            append(NEWLINE);
            append("--############ Schema:").append(schema.getName()).append(" ############");
            append(NEWLINE);
            append(SET).append(SPACE).append(SCHEMA).append(SPACE);
            append(SQLStringVisitor.escapeSinglePart(schema.getName())).append(SEMICOLON);
            append(NEWLINE);
            append(NEWLINE);
            visit(schema);
        }

        if (!database.getRoles().isEmpty()){
            boolean hasPolicies = false;
            append(NEWLINE);
            append("--############ Grants ############");
            append(NEWLINE);
            for (Role role:database.getRoles()) {
                visitGrants(role);
                append(NEWLINE);
                if (!role.getPolicies().isEmpty()) {
                    hasPolicies = true;
                }
            }

            if (hasPolicies) {
                append(NEWLINE);
                append("--############ Policies ############");
                append(NEWLINE);
                for (Role role:database.getRoles()) {
                    visitPolicies(role);
                    append(NEWLINE);
                }
            }
        }

        append(NEWLINE);
        append("/*").append(NEWLINE);
        append("###########################################").append(NEWLINE);
        append("# END DATABASE ").append(database.getName()).append(NEWLINE);
        append("###########################################").append(NEWLINE);
        append("*/").append(NEWLINE);
        append(NEWLINE);
    }

    protected void createdSchmea(Schema schema) {

    }

    private void visit(Datatype dt) {
        append(CREATE).append(SPACE).append(DOMAIN).append(SPACE);
        append(SQLStringVisitor.escapeSinglePart(dt.getName())).append(SPACE).append(AS).append(SPACE);
        String runtimeTypeName = dt.getBasetypeName();
        append(runtimeTypeName);
        Datatype base = SystemMetadata.getInstance().getRuntimeTypeMap().get(runtimeTypeName);
        if (DataTypeManager.hasLength(runtimeTypeName)) {
            if (dt.getLength() != base.getLength()) {
                append(LPAREN).append(dt.getLength()).append(RPAREN);
            }
        } else if (PRECISION_DATATYPES.contains(runtimeTypeName)
                && (dt.getPrecision() != base.getPrecision() || dt.getScale() != base.getScale())) {
            append(LPAREN).append(dt.getPrecision());
            if (dt.getScale() != 0) {
                append(COMMA).append(dt.getScale());
            }
            append(RPAREN);
        }
        if (dt.getNullType() == NullType.No_Nulls) {
            append(SPACE).append(NOT_NULL);
        }
        append(SEMICOLON);
    }

    private void visitPolicies(Role r) {
        for (Map<String, Policy> policies : r.getPolicies().values()) {
            for (Policy p : policies.values()) {
                visitPolicy(r, p);
            }
        }
    }

    private void visitPolicy(Role r, Policy p) {
        append(CREATE).append(SPACE).append(POLICY).append(SPACE);
        append(SQLStringVisitor.escapeSinglePart(p.getName())).append(SPACE);
        append(ON).append(SPACE);
        if (p.getResourceType() == ResourceType.PROCEDURE) {
            append(PROCEDURE).append(SPACE);
            append(new GroupSymbol(p.getResourceName())).append(SPACE);
            if (p.getOperations().contains(Operation.ALL)) {
                append(FOR).append(SPACE).append(ALL);
            }
        } else {
            append(new GroupSymbol(p.getResourceName())).append(SPACE);
            if (p.getOperations().contains(Operation.ALL)) {
                append(FOR).append(SPACE).append(ALL).append(SPACE);
            } else if (!p.getOperations().isEmpty()) {
                append(FOR).append(SPACE);
                boolean first = true;
                for (Operation oper : p.getOperations()) {
                    if (!first) {
                        append(COMMA).append(SPACE);
                    }
                    append(oper.name());
                    first = false;
                }
                append(SPACE);
            }
        }
        append(TO).append(SPACE).append(r.getName()).append(SPACE);
        append(USING).append(SPACE).append(LPAREN).append(p.getCondition()).append(RPAREN);
        append(SEMICOLON).append(NEWLINE);
    }

    private void visitGrants(Role r) {
        for (Permission permission : r.getGrants().values()) {
            if (permission.getResourceType() == ResourceType.DATABASE && permission.getResourceName() == null) {
                for (Privilege p : permission.getPrivileges()) {
                    appendGrant(r, permission, EnumSet.of(p), false);
                }
                for (Privilege p : permission.getRevokePrivileges()) {
                    appendGrant(r, permission, EnumSet.of(p), true);
                }
                continue;
            }
            if (!permission.getPrivileges().isEmpty() || permission.getMask() != null) {
                appendGrant(r, permission, permission.getPrivileges(), false);
            }
            if (permission.getMask() == null && permission.getCondition() != null) {
                Policy p = new Policy();
                p.setCondition(permission.getCondition());
                p.setName("grant_policy_" + permission.getResourceName()); //$NON-NLS-1$
                if (Boolean.FALSE.equals(permission.isConditionAConstraint())) {
                    p.getOperations().add(Operation.SELECT);
                    p.getOperations().add(Operation.DELETE);
                } else {
                    p.getOperations().add(Operation.ALL);
                }
                p.setResourceName(permission.getResourceName());
                p.setResourceType(permission.getResourceType());
                visitPolicy(r, p);
            }
            if (!permission.getRevokePrivileges().isEmpty()) {
                appendGrant(r, permission, permission.getRevokePrivileges(), true);
            }
        }
    }

    private void appendGrant(Role role, Permission permission, EnumSet<Privilege> privileges, boolean revoke) {
        append(revoke?REVOKE:GRANT);
        boolean first = true;
        for (Privilege allowance:privileges) {
            if (first) {
                first = false;
                append(SPACE);
            } else {
                append(COMMA);
            }
            append(allowance);
        }
        if (permission.getResourceType() != ResourceType.DATABASE) {
            append(SPACE).append(ON).append(SPACE).append(permission.getResourceType());
        }

        if (permission.getResourceName() != null) {
            append(SPACE).append(SQLStringVisitor.escapeSinglePart(permission.getResourceName()));
        }

        if (!revoke && permission.getMask() != null) {
            append(SPACE).append(MASK);
            if (permission.getMaskOrder() != null && permission.getMaskOrder() != -1) {
                append(SPACE).append(ORDER).append(SPACE).append(permission.getMaskOrder());
            }
            append(SPACE).append(new Constant(permission.getMask()));
            if (permission.getCondition() != null) {
                append(SPACE).append(CONDITION).append(SPACE).append(permission.getCondition());
            }
        }

        append(SPACE).append(revoke?FROM:TO).append(SPACE).append(role.getName());
        append(SEMICOLON).append(NEWLINE);
    }

    private void visit(Role role) {
        append(CREATE).append(SPACE).append(ROLE).append(SPACE)
                .append(SQLStringVisitor.escapeSinglePart(role.getName()));
        if (role.isAnyAuthenticated()) {
            append(SPACE).append(WITH).append(SPACE).append(ANY).append(SPACE).append(AUTHENTICATED);
        } else if (role.getMappedRoles() != null && !role.getMappedRoles().isEmpty()) {
            append(SPACE).append(WITH).append(SPACE).append(FOREIGN).append(SPACE).append(ROLE);
            for (String str:role.getMappedRoles()) {
                append(SPACE).append(SQLStringVisitor.escapeSinglePart(str));
            }
        }

        append(SEMICOLON);
    }

    private void visit(DataWrapper dw) {
        if (dw.getType() != null) {
            append(CREATE).append(SPACE).append(FOREIGN).append(SPACE).append(DATA).append(SPACE).append(WRAPPER)
                    .append(SPACE);
            append(SQLStringVisitor.escapeSinglePart(dw.getName()));
            if (dw.getType() != null) {
                append(SPACE).append(TYPE).append(SPACE).append(SQLStringVisitor.escapeSinglePart(dw.getType()));
            }
            appendOptions(dw);
            append(SEMICOLON);
            append(NEWLINE);
            append(NEWLINE);
        }
    }

    private void visit(Server server) {
        append(CREATE).append(SPACE).append(SERVER).append(SPACE)
                .append(SQLStringVisitor.escapeSinglePart(server.getName()));
        if (server.getType() != null) {
            append(SPACE).append(TYPE).append(SPACE).append(new Constant(server.getType()));
        }
        if (server.getVersion() != null) {
            append(SPACE).append(VERSION).append(SPACE).append(new Constant(server.getVersion()));
        }
        append(SPACE).append(FOREIGN).append(SPACE).append(DATA).append(SPACE).append(WRAPPER).append(SPACE);
        append(SQLStringVisitor.escapeSinglePart(server.getDataWrapper()));
        appendOptions(server);
        append(SEMICOLON);
    }

    protected void visit(Schema schema) {
        boolean first = true;
        for (AbstractMetadataRecord record : schema.getResolvingOrder()) {
            String generated = record.getProperty(GENERATED, false);
            if (generated != null && Boolean.valueOf(generated)) {
                continue;
            }

            if (record instanceof Table) {
                if (!this.includeTables) {
                    continue;
                }
            } else if (record instanceof Procedure) {
                if (!this.includeProcedures) {
                    continue;
                }
            } else if (record instanceof FunctionMethod) {
                if (!this.includeFunctions) {
                    continue;
                }
            }
            if (!shouldInclude(record)) {
                continue;
            }
            if (first) {
                first = false;
            }
            else {
                append(NEWLINE);
                append(NEWLINE);
            }
            if (record instanceof Table) {
                visit((Table)record);
            } else if (record instanceof Procedure) {
                visit((Procedure)record);
            } else if (record instanceof FunctionMethod) {
                visit((FunctionMethod)record);
            }
        }
    }

    private boolean shouldInclude(AbstractMetadataRecord record) {
        return this.filter == null || filter.matcher(record.getName()).matches();
    }

    private void visit(Table table) {
        append(CREATE).append(SPACE);
        if (table.isPhysical()) {
            append(FOREIGN_TABLE);
        }
        else {
            if (table.getTableType() == Table.Type.TemporaryTable) {
                append(GLOBAL).append(SPACE).append(TEMPORARY).append(SPACE).append(TABLE);
            } else {
                append(VIEW);
            }
        }
        append(SPACE);
        String name = addTableBody(table);

        if (table.getTableType() != Table.Type.TemporaryTable) {
            if (table.isVirtual()) {
                append(NEWLINE).append(SQLConstants.Reserved.AS).append(NEWLINE).append(table.getSelectTransformation());
            }
            append(SQLConstants.Tokens.SEMICOLON);

            if (table.isInsertPlanEnabled()) {
                buildTrigger(name, null, INSERT, table.getInsertPlan(), false);
            }

            if (table.isUpdatePlanEnabled()) {
                buildTrigger(name, null, UPDATE, table.getUpdatePlan(), false);
            }

            if (table.isDeletePlanEnabled()) {
                buildTrigger(name, null, DELETE, table.getDeletePlan(), false);
            }

            for (Trigger tr : table.getTriggers().values()) {
                String generated = tr.getProperty(GENERATED, false);
                if (generated == null || !Boolean.valueOf(generated)) {
                    buildTrigger(name, tr.getName(), tr.getEvent().name(), tr.getPlan(), tr.isAfter());
                }
            }
        } else {
            append(SQLConstants.Tokens.SEMICOLON);
        }
    }

    public String addTableBody(Table table) {
        String name = SQLStringVisitor.escapeSinglePart(table.getName());
        append(name);

        if (table.getColumns() != null) {
            append(SPACE);
            append(LPAREN);
            boolean first = true;
            for (Column c:table.getColumns()) {
                if (first) {
                    first = false;
                }
                else {
                    append(COMMA);
                }
                visit(table, c);
            }
            buildContraints(table);
            append(NEWLINE);
            append(RPAREN);
        }

        // options
        String options = buildTableOptions(table);
        if (!options.isEmpty()) {
            append(SPACE).append(OPTIONS).append(SPACE).append(LPAREN).append(options).append(RPAREN);
        }
        return name;
    }

    protected DDLStringVisitor append(Object o) {
        buffer.append(o);
        return this;
    }

    private void buildTrigger(String name, String trigger_name, String type, String plan, boolean isAfter) {
        append(NEWLINE);
        append(NEWLINE);
        append(SQLConstants.Reserved.CREATE).append(SPACE).append(TRIGGER).append(SPACE);
        if (trigger_name != null) {
            append(SQLStringVisitor.escapeSinglePart(trigger_name)).append(SPACE);
        }
        append(SQLConstants.Reserved.ON).append(SPACE).append(name).append(SPACE);
        if (isAfter) {
            append(AFTER);
        } else {
            append(INSTEAD_OF);
        }
        append(SPACE).append(type).append(SPACE).append(SQLConstants.Reserved.AS).append(NEWLINE);
        append(plan);
        append(SQLConstants.Tokens.SEMICOLON);
    }

    private String buildTableOptions(Table table) {
        StringBuilder options = new StringBuilder();
        addCommonOptions(options, table);

        if (table.isMaterialized()) {
            addOption(options, MATERIALIZED, table.isMaterialized());
            if (table.getMaterializedTable() != null) {
                addOption(options, MATERIALIZED_TABLE, table.getMaterializedTable().getName());
            }
        }
        if (table.supportsUpdate()) {
            addOption(options, UPDATABLE, table.supportsUpdate());
        }
        if (table.getCardinality() != -1) {
            if (table.getCardinality() != table.getCardinalityAsFloat()) {
                addOption(options, CARDINALITY, (long)table.getCardinalityAsFloat());
            } else {
                addOption(options, CARDINALITY, table.getCardinality());
            }
        }
        if (!table.getProperties().isEmpty()) {
            for (String key:table.getProperties().keySet()) {
                addOption(options, key, table.getProperty(key, false));
            }
        }
        return options.toString();
    }

    private void addCommonOptions(StringBuilder sb, AbstractMetadataRecord record) {
        if (record.isUUIDSet() && record.getUUID() != null && !record.getUUID().startsWith("tid:")) { //$NON-NLS-1$
            addOption(sb, UUID, record.getUUID());
        }
        if (record.getAnnotation() != null) {
            addOption(sb, ANNOTATION, record.getAnnotation());
        }
        if (record.getNameInSource() != null) {
            addOption(sb, NAMEINSOURCE, record.getNameInSource());
        }
    }

    private void buildContraints(Table table) {
        addConstraints(table.getAccessPatterns(), "AP", ACCESSPATTERN); //$NON-NLS-1$

        KeyRecord pk = table.getPrimaryKey();
        if (pk != null) {
            addConstraint("PK", PRIMARY_KEY, pk, true); //$NON-NLS-1$
        }

        addConstraints(table.getUniqueKeys(), UNIQUE, UNIQUE);
        addConstraints(table.getIndexes(), INDEX, INDEX);
        addConstraints(table.getFunctionBasedIndexes(), INDEX, INDEX);

        for (int i = 0; i < table.getForeignKeys().size(); i++) {
            ForeignKey key = table.getForeignKeys().get(i);
            if (key.getReferenceKey() != null && !shouldInclude(key.getReferenceKey().getParent())) {
                continue;
            }
            addConstraint("FK" + i, FOREIGN_KEY, key, false); //$NON-NLS-1$
            append(SPACE).append(REFERENCES);
            if (key.getReferenceKey() != null) {
                if (key.getReferenceKey().getParent().getParent().equals(key.getParent().getParent())) {
                    append(SPACE).append(new GroupSymbol(key.getReferenceKey().getParent().getName()));
                } else {
                    append(SPACE).append(new GroupSymbol(key.getReferenceKey().getParent().getFullName()));
                }
            } else if (key.getReferenceTableName() != null) {
                append(SPACE).append(new GroupSymbol(key.getReferenceTableName()));
            }
            append(SPACE);
            addNames(key.getReferenceColumns());
            appendOptions(key);
        }
    }

    private void addConstraints(List<KeyRecord> constraints, String defaultName, String type) {
        for (int i = 0; i < constraints.size(); i++) {
            KeyRecord constraint = constraints.get(i);
            addConstraint(defaultName + i, type, constraint, true);
        }
    }

    private void addConstraint(String defaultName, String type,
            KeyRecord constraint, boolean addOptions) {
        append(COMMA).append(NEWLINE).append(TAB);
        boolean nameMatches = defaultName.equals(constraint.getName());
        if (!nameMatches) {
            append(CONSTRAINT).append(SPACE).append(SQLStringVisitor.escapeSinglePart(constraint.getName())).append(SPACE);
        }
        append(type);
        addColumns(constraint.getColumns(), false);
        if (addOptions) {
            appendOptions(constraint);
        }
    }

    private void addColumns(List<Column> columns, boolean includeType) {
        append(LPAREN);
        boolean first = true;
        for (Column c:columns) {
            if (first) {
                first = false;
            }
            else {
                append(COMMA).append(SPACE);
            }
            if (includeType) {
                appendColumn(c, true, includeType);
                appendColumnOptions(c);
            } else if (c.getParent() instanceof KeyRecord) {
                //function based column
                append(c.getNameInSource());
            } else {
                append(SQLStringVisitor.escapeSinglePart(c.getName()));
            }
        }
        append(RPAREN);
    }

    private void addNames(List<String> columns) {
        if (columns != null) {
            append(LPAREN);
            boolean first = true;
            for (String c:columns) {
                if (first) {
                    first = false;
                }
                else {
                    append(COMMA).append(SPACE);
                }
                append(SQLStringVisitor.escapeSinglePart(c));
            }
            append(RPAREN);
        }
    }

    private void visit(Table table, Column column) {
        append(NEWLINE).append(TAB);
        if (table.getTableType() == Table.Type.TemporaryTable && column.isAutoIncremented() && column.getNullType() == NullType.No_Nulls && column.getJavaType() == DataTypeManager.DefaultDataClasses.INTEGER) {
            append(SQLStringVisitor.escapeSinglePart(column.getName()));
            append(SPACE);
            append(SERIAL);
        } else {
            appendColumn(column, true, !table.isVirtual() || !Boolean.valueOf(column.getProperty(MetadataValidator.UNTYPED, false)));

            if (column.isAutoIncremented()) {
                append(SPACE).append(AUTO_INCREMENT);
            }
        }

        appendDefault(column);

        // options
        appendColumnOptions(column);
    }

    private void appendDefault(BaseColumn column) {
        if (column.getDefaultValue() != null) {
            append(SPACE).append(DEFAULT).append(SPACE);
            if (BaseColumn.EXPRESSION_DEFAULT.equalsIgnoreCase(column.getProperty(BaseColumn.DEFAULT_HANDLING, false))) {
                append(column.getDefaultValue());
            } else {
                append(new Constant(column.getDefaultValue()));
            }
        }
    }

    private void appendColumn(BaseColumn column, boolean includeName, boolean includeType) {
        if (includeName) {
            append(SQLStringVisitor.escapeSinglePart(column.getName()));
        }
        if (includeType) {
            Datatype datatype = column.getDatatype();
            String runtimeTypeName = column.getRuntimeType();
            boolean domain = false;
            if (datatype != null) {
                runtimeTypeName = datatype.getRuntimeTypeName();
                domain = datatype.getType() == Datatype.Type.Domain;
            }
            if (includeName) {
                append(SPACE);
            }
            if (domain) {
                append(datatype.getName());
            } else {
                append(runtimeTypeName);
                if (DataTypeManager.hasLength(runtimeTypeName)) {
                    if (column.getLength() != 0 && (datatype == null || column.getLength() != datatype.getLength())) {
                        append(LPAREN).append(column.getLength()).append(RPAREN);
                    }
                } else if (PRECISION_DATATYPES.contains(runtimeTypeName)
                        && !column.isDefaultPrecisionScale()) {
                    append(LPAREN).append(column.getPrecision());
                    if (column.getScale() != 0) {
                        append(COMMA).append(column.getScale());
                    }
                    append(RPAREN);
                } else if (runtimeTypeName.equals(DataTypeManager.DefaultDataTypes.TIMESTAMP)
                        && !column.isDefaultPrecisionScale()) {
                    append(LPAREN);
                    append(column.getScale());
                    append(RPAREN);
                }
            }
            if (datatype != null) {
                for (int dims = column.getArrayDimensions(); dims > 0; dims--) {
                    append(Tokens.LSBRACE).append(Tokens.RSBRACE);
                }
            }
            if (column.getNullType() == NullType.No_Nulls && (!domain || datatype.getNullType() != NullType.No_Nulls)) {
                append(SPACE).append(NOT_NULL);
            }
        }
    }

    private void appendColumnOptions(BaseColumn column) {
        StringBuilder options = new StringBuilder();
        addCommonOptions(options, column);

        if (!column.getDatatype().isBuiltin() && column.getDatatype().getType() != Datatype.Type.Domain) {
            //an enterprise type
            addOption(options, UDT, column.getDatatype().getName() + "("+column.getLength()+ ", " +column.getPrecision()+", " + column.getScale()+ ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        }

        if (column.getDatatype().getRadix() != 0 && column.getRadix() != column.getDatatype().getRadix()) {
            addOption(options, RADIX, column.getRadix());
        }

        buildColumnOptions(column, options);

        if (options.length() != 0) {
            append(SPACE).append(OPTIONS).append(SPACE).append(LPAREN).append(options).append(RPAREN);
        }
    }

    private void buildColumnOptions(BaseColumn baseColumn,
            StringBuilder options) {
        if (baseColumn instanceof Column) {
            Column column = (Column)baseColumn;
            if (!column.isSelectable()) {
                addOption(options, SELECTABLE, column.isSelectable());
            }

            // if table is already updatable, then columns are implicitly updatable.
            if (!column.isUpdatable() && column.getParent() instanceof Table && ((Table)column.getParent()).supportsUpdate()) {
                addOption(options, UPDATABLE, column.isUpdatable());
            }

            if (column.isCurrency()) {
                addOption(options, CURRENCY, column.isCurrency());
            }

            // only record if not default
            if (!column.isCaseSensitive() && column.getDatatype().isCaseSensitive()) {
                addOption(options, CASE_SENSITIVE, column.isCaseSensitive());
            }

            if (!column.isSigned() && column.getDatatype().isSigned()) {
                addOption(options, SIGNED, column.isSigned());
            }
            if (column.isFixedLength()) {
                addOption(options, FIXED_LENGTH, column.isFixedLength());
            }
            // length and octet length should be same. so this should be never be true.
            //TODO - this is not quite valid since we are dealing with length representing chars in UTF-16, then there should be twice the bytes
            if (column.getCharOctetLength() != 0 && column.getLength() != column.getCharOctetLength()) {
                addOption(options, CHAR_OCTET_LENGTH, column.getCharOctetLength());
            }

            // by default the search type is default data type search, so avoid it.
            if (column.getSearchType() != null && (!column.getSearchType().equals(column.getDatatype().getSearchType()) || column.isSearchTypeSet())) {
                addOption(options, SEARCHABLE, column.getSearchType().name());
            }

            if (column.getMinimumValue() != null) {
                addOption(options, MIN_VALUE, column.getMinimumValue());
            }

            if (column.getMaximumValue() != null) {
                addOption(options, MAX_VALUE, column.getMaximumValue());
            }

            if (column.getNullValues() != -1) {
                addOption(options, NULL_VALUE_COUNT, column.getNullValues());
            }

            if (column.getDistinctValues() != -1) {
                addOption(options, DISTINCT_VALUES, column.getDistinctValues());
            }
        }

        if (baseColumn.getNativeType() != null) {
            addOption(options, NATIVE_TYPE, baseColumn.getNativeType());
        }

        buildOptions(baseColumn, options);
    }

    private void appendOptions(AbstractMetadataRecord record) {
        StringBuilder options = new StringBuilder();
        addCommonOptions(options, record);
        buildOptions(record, options);
        if (options.length() != 0) {
            append(SPACE).append(OPTIONS).append(SPACE).append(LPAREN).append(options).append(RPAREN);
        }
    }

    private void buildOptions(AbstractMetadataRecord record, StringBuilder options) {
        if (!record.getProperties().isEmpty()) {
            for (Map.Entry<String, String> entry:record.getProperties().entrySet()) {
                if (record instanceof Database && entry.getKey().equals("full-ddl")) {
                    continue;
                }
                if (entry.getKey().equals(MetadataValidator.UNTYPED)) {
                    continue;
                }
                addOption(options, entry.getKey(), entry.getValue());
            }
        }
    }

    private void addOption(StringBuilder sb, String key, Object value) {
        if (sb.length() != 0) {
            sb.append(COMMA).append(SPACE);
        }
        if (value != null) {
            value = new Constant(value);
        } else {
            value = Constant.NULL_CONSTANT;
        }
        sb.append(SQLStringVisitor.escapeSinglePart(key)).append(SPACE).append(value);
    }

    private void visit(Procedure procedure) {
        append(CREATE).append(SPACE);
        if (procedure.isVirtual()) {
            append(VIRTUAL);
        }
        else {
            append(FOREIGN);
        }
        append(SPACE).append(procedure.isFunction()?FUNCTION:PROCEDURE).append(SPACE).append(SQLStringVisitor.escapeSinglePart(procedure.getName()));
        append(LPAREN);

        boolean first = true;
        for (ProcedureParameter pp:procedure.getParameters()) {
            if (first) {
                first = false;
            }
            else {
                append(COMMA).append(SPACE);
            }
            visit(pp);
        }
        append(RPAREN);

        if (procedure.getResultSet() != null) {
            append(SPACE).append(RETURNS);
            appendOptions(procedure.getResultSet());
            append(SPACE).append(TABLE).append(SPACE);
            addColumns(procedure.getResultSet().getColumns(), true);
        }
        /* The parser treats the RETURN clause as optional for a procedure if using the RESULT param
          for (ProcedureParameter pp: procedure.getParameters()) {
            if (pp.getType().equals(Type.ReturnValue)) {
                append(SPACE).append(RETURNS).append(SPACE);
                appendColumn(buffer, pp, false, true);
                break;
            }
        }*/

        //options
        String options = buildProcedureOptions(procedure);
        if (!options.isEmpty()) {
            append(NEWLINE).append(OPTIONS).append(SPACE).append(LPAREN).append(options).append(RPAREN);
        }
        //block
        if (procedure.isVirtual()) {
            append(NEWLINE).append(SQLConstants.Reserved.AS).append(NEWLINE);
            String plan = procedure.getQueryPlan();
            append(plan);
        }

        append(SEMICOLON);
    }

    private String buildProcedureOptions(Procedure procedure) {
        StringBuilder options = new StringBuilder();
        addCommonOptions(options, procedure);

        if (procedure.getUpdateCount() != Procedure.AUTO_UPDATECOUNT) {
            addOption(options, UPDATECOUNT, procedure.getUpdateCount());
        }

        if (!procedure.getProperties().isEmpty()) {
            for (String key:procedure.getProperties().keySet()) {
                addOption(options, key, procedure.getProperty(key, false));
            }
        }

        return options.toString();
    }

    private void visit(ProcedureParameter param) {
        Type type = param.getType();
        String typeStr = null;
        switch (type) {
        case InOut:
            typeStr = INOUT;
            break;
        case ReturnValue:
        case Out:
            typeStr = OUT;
            break;
        case In:
            if (param.isVarArg()) {
                typeStr = NonReserved.VARIADIC;
            } else {
                typeStr = IN;
            }
            break;
        }
        append(typeStr).append(SPACE);
        appendColumn(param, true, true);
        if (type == Type.ReturnValue) {
            append(SPACE).append(NonReserved.RESULT);
        }
        appendDefault(param);
        appendColumnOptions(param);
    }

    private void visit(FunctionMethod function) {
        append(CREATE).append(SPACE);
        if (function.getPushdown().equals(FunctionMethod.PushDown.MUST_PUSHDOWN)) {
            append(FOREIGN);
        }
        else {
            append(VIRTUAL);
        }
        append(SPACE).append(FUNCTION).append(SPACE).append(SQLStringVisitor.escapeSinglePart(function.getName()));
        append(LPAREN);

        boolean first = true;
        for (FunctionParameter fp:function.getInputParameters()) {
            if (first) {
                first = false;
            }
            else {
                append(COMMA).append(SPACE);
            }
            visit(fp);
        }
        append(RPAREN);

        append(SPACE).append(RETURNS);
        appendOptions(function.getOutputParameter());
        append(SPACE);
        append(function.getOutputParameter().getType());

        //options
        String options = buildFunctionOptions(function);
        if (!options.isEmpty()) {
            append(NEWLINE).append(OPTIONS).append(SPACE).append(LPAREN).append(options).append(RPAREN);
        }

        /*if (function.getDefinition() != null) {
            append(NEWLINE).append(SQLConstants.Reserved.AS).append(NEWLINE);
            append(function.getDefinition());
        }*/

        append(SQLConstants.Tokens.SEMICOLON);
    }

    private String buildFunctionOptions(FunctionMethod function) {
        StringBuilder options = new StringBuilder();
        addCommonOptions(options, function);

        if (function.getCategory() != null) {
            addOption(options, CATEGORY, function.getCategory());
        }

        if (!function.getDeterminism().equals(Determinism.DETERMINISTIC)) {
            addOption(options, DETERMINISM, function.getDeterminism().name());
        }

        if (function.getInvocationClass() != null) {
            addOption(options, JAVA_CLASS, function.getInvocationClass());
        }

        if (function.getInvocationMethod() != null) {
            addOption(options, JAVA_METHOD, function.getInvocationMethod());
        }

        if (!function.getProperties().isEmpty()) {
            for (String key:function.getProperties().keySet()) {
                addOption(options, key, function.getProperty(key, false));
            }
        }

        return options.toString();
    }

    private void visit(FunctionParameter param) {
        if (param.isVarArg()) {
            append(NonReserved.VARIADIC).append(SPACE);
        }
        appendColumn(param, true, true);
    }

    @Override
    public String toString() {
        return buffer.toString();
    }

    public static String getDomainDDLString(Database database) {
        DDLStringVisitor visitor = new DDLStringVisitor(null, null);
        for (Datatype dt : database.getMetadataStore().getDatatypes().values()) {
            if (dt.getType() != Datatype.Type.Domain) {
                continue;
            }
            visitor.visit(dt);
            visitor.append(SPACE);
        }
        return visitor.toString();
    }
}
