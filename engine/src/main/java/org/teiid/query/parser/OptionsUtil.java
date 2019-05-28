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
package org.teiid.query.parser;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.core.util.StringUtil;
import org.teiid.metadata.*;
import org.teiid.metadata.Column.SearchType;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.DDLConstants;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.parser.SQLParserUtil.ParsedDataType;

public class OptionsUtil {
    static final Pattern udtPattern = Pattern.compile("(\\w+)\\s*\\(\\s*(\\d+),\\s*(\\d+),\\s*(\\d+)\\)"); //$NON-NLS-1$

    private static void setProcedureOptions(Procedure proc) {
        Map<String, String> props = proc.getProperties();
        setCommonProperties(proc, props);

        String value = props.remove("UPDATECOUNT"); //$NON-NLS-1$
        if (value != null) {
            proc.setUpdateCount(Integer.parseInt(value));
        }
    }

    public static void removeOption(AbstractMetadataRecord record, String key) {
        if (record instanceof Table) {
            removeTableOption(key, (Table)record);
        }
        if (record instanceof Procedure) {
            removeProcedureOption(key, (Procedure)record);
        }
        if (record instanceof BaseColumn) {
            removeColumnOption(key, (BaseColumn)record);
        }
        if (record instanceof Schema) {
            removeSchemaOption(key, (Schema)record);
        }
        record.getProperties().remove(key);
    }

    public static void setOptions(AbstractMetadataRecord record) {
        if (record instanceof Table) {
            setTableOptions((Table)record);
        }
        if (record instanceof Procedure) {
            setProcedureOptions((Procedure)record);
        }
        if (record instanceof BaseColumn) {
            setColumnOptions((BaseColumn)record);
        }
        if (record instanceof Schema) {
            setSchemaOptions((Schema)record);
        }
        if (record instanceof Database ||
                record instanceof DataWrapper ||
                record instanceof Server) {
            Map<String, String> props = record.getProperties();
            setCommonProperties(record, props);
        }
    }

    private static void setSchemaOptions(Schema schema) {
        Map<String, String> props = schema.getProperties();
        setCommonProperties(schema, props);
    }

    private static void removeSchemaOption(String key, Schema schema) {
        if (schema.getProperty(key, false) != null) {
            schema.setProperty(key, null);
        }
        removeCommonProperty(key, schema);
    }

    private static void removeColumnOption(String key, BaseColumn c)  throws MetadataException {
        if (c.getProperty(key, false) != null) {
            c.setProperty(key, null);
        }
        removeCommonProperty(key, c);

        if (key.equals(DDLConstants.RADIX)) {
            c.setRadix(0);
        } else if (key.equals(DDLConstants.NATIVE_TYPE)) {
            c.setNativeType(null);
        } else if (c instanceof Column) {
            removeColumnOption(key, (Column)c);
        }
    }

    private static void removeColumnOption(String key, Column c) {
        if (key.equals(DDLConstants.CASE_SENSITIVE)) {
            c.setCaseSensitive(false);
        } else if (key.equals(DDLConstants.SELECTABLE)) {
            c.setSelectable(true);
        } else if (key.equals(DDLConstants.UPDATABLE)) {
            c.setUpdatable(false);
        } else if (key.equals(DDLConstants.SIGNED)) {
            c.setSigned(false);
        } else if (key.equals(DDLConstants.CURRENCY)) {
            c.setSigned(false);
        } else if (key.equals(DDLConstants.FIXED_LENGTH)) {
            c.setFixedLength(false);
        } else if (key.equals(DDLConstants.SEARCHABLE)) {
            c.setSearchType(null);
        } else if (key.equals(DDLConstants.MIN_VALUE)) {
            c.setMinimumValue(null);
        } else if (key.equals(DDLConstants.MAX_VALUE)) {
            c.setMaximumValue(null);
        } else if (key.equals(DDLConstants.CHAR_OCTET_LENGTH)) {
            c.setCharOctetLength(0);
        } else if (key.equals(DDLConstants.NULL_VALUE_COUNT)) {
            c.setNullValues(-1);
        } else if (key.equals(DDLConstants.DISTINCT_VALUES)) {
            c.setDistinctValues(-1);
        } else if (key.equals(DDLConstants.UDT)) {
            c.setDatatype(null, false, c.getArrayDimensions());
            c.setLength(0);
            c.setPrecision(0);
            c.setScale(0);
        }
    }
   private static void removeProcedureOption(String key, Procedure proc) {
        if (proc.getProperty(key, false) != null) {
            proc.setProperty(key, null);
        }
        removeCommonProperty(key, proc);

        if (key.equals("UPDATECOUNT")) { //$NON-NLS-1$
            proc.setUpdateCount(1);
        }
    }

    public static void setCommonProperties(AbstractMetadataRecord c, Map<String, String> props) {
        String v = props.remove(DDLConstants.UUID);
        if (v != null) {
            c.setUUID(v);
        }

        v = props.remove(DDLConstants.ANNOTATION);
        if (v != null) {
            c.setAnnotation(v);
        }

        v = props.remove(DDLConstants.NAMEINSOURCE);
        if (v != null) {
            c.setNameInSource(v);
        }
    }

    private static void removeCommonProperty(String key, AbstractMetadataRecord c) {
        if (key.equals(DDLConstants.UUID)) {
            c.setUUID(null);
        } else if (key.equals(DDLConstants.ANNOTATION)) {
            c.setAnnotation(null);
        } else if (key.equals(DDLConstants.NAMEINSOURCE)) {
            c.setNameInSource(null);
        }
    }

    private static void setTableOptions(Table table) {
        Map<String, String> props = table.getProperties();
        setCommonProperties(table, props);

        String value = props.remove(DDLConstants.MATERIALIZED);
        if (value != null) {
            table.setMaterialized(isTrue(value));
        }

        value = props.remove(DDLConstants.MATERIALIZED_TABLE);
        if (value != null) {
            Table mattable = new Table();
            mattable.setName(value);
            table.setMaterializedTable(mattable);
        }

        value = props.remove(DDLConstants.UPDATABLE);
        if (value != null) {
            table.setSupportsUpdate(isTrue(value));
        }

        value = props.remove(DDLConstants.CARDINALITY);
        if (value != null) {
            table.setCardinality(Long.valueOf(value));
        }
    }

    private static void removeTableOption(String key, Table table) {
        if (table.getProperty(key, false) != null) {
            table.setProperty(key, null);
        }
        removeCommonProperty(key, table);

        if (key.equals(DDLConstants.MATERIALIZED)) {
            table.setMaterialized(false);
        }

        if (key.equals(DDLConstants.MATERIALIZED_TABLE)) {
            table.setMaterializedTable(null);
        }

        if (key.equals(DDLConstants.UPDATABLE)) {
            table.setSupportsUpdate(false);
        }

        if (key.equals(DDLConstants.CARDINALITY)) {
            table.setCardinality(-1);
        }
    }

    private static boolean isTrue(final String text) {
        return Boolean.valueOf(text);
    }

    private static void setColumnOptions(BaseColumn c)  throws MetadataException {
        Map<String, String> props = c.getProperties();
        setCommonProperties(c, props);

        String v = props.remove(DDLConstants.RADIX);
        if (v != null) {
            c.setRadix(Integer.parseInt(v));
        }

        v = props.remove(DDLConstants.NATIVE_TYPE);
        if (v != null) {
            c.setNativeType(v);
        }

        if (c instanceof Column) {
            setColumnOptions((Column)c, props);
        }
    }

    private static void setColumnOptions(Column c, Map<String, String> props) throws MetadataException {
        String v = props.remove(DDLConstants.CASE_SENSITIVE);
        if (v != null) {
            c.setCaseSensitive(isTrue(v));
        }

        v = props.remove(DDLConstants.SELECTABLE);
        if (v != null) {
            c.setSelectable(isTrue(v));
        }

        v = props.remove(DDLConstants.UPDATABLE);
        if (v != null) {
            c.setUpdatable(isTrue(v));
        }

        v = props.remove(DDLConstants.SIGNED);
        if (v != null) {
            c.setSigned(isTrue(v));
        }

        v = props.remove(DDLConstants.CURRENCY);
        if (v != null) {
            c.setSigned(isTrue(v));
        }

        v = props.remove(DDLConstants.FIXED_LENGTH);
        if (v != null) {
            c.setFixedLength(isTrue(v));
        }

        v = props.remove(DDLConstants.SEARCHABLE);
        if (v != null) {
            c.setSearchType(StringUtil.caseInsensitiveValueOf(SearchType.class, v));
        }

        v = props.remove(DDLConstants.MIN_VALUE);
        if (v != null) {
            c.setMinimumValue(v);
        }

        v = props.remove(DDLConstants.MAX_VALUE);
        if (v != null) {
            c.setMaximumValue(v);
        }

        v = props.remove(DDLConstants.CHAR_OCTET_LENGTH);
        if (v != null) {
            c.setCharOctetLength(Integer.parseInt(v));
        }

        v = props.remove(DDLConstants.NULL_VALUE_COUNT);
        if (v != null) {
            c.setNullValues(Integer.parseInt(v));
        }

        v = props.remove(DDLConstants.DISTINCT_VALUES);
        if (v != null) {
            c.setDistinctValues(Integer.parseInt(v));
        }

        v = props.remove(DDLConstants.UDT);
        if (v != null) {
            Matcher matcher = udtPattern.matcher(v);
            List<Datatype> datatypes = SystemMetadata.getInstance().getDataTypes();
            Datatype match = null;
            if (matcher.matches()) {
                for (Datatype dt : datatypes) {
                    if (dt.getName().equalsIgnoreCase(matcher.group(1))) {
                        match = dt;
                        break;
                    }
                }
            }
            if (match != null) {
                c.setDatatype(match, false, c.getArrayDimensions());
                c.setLength(Integer.parseInt(matcher.group(2)));
                ParsedDataType pdt = new ParsedDataType(matcher.group(1), Integer.parseInt(matcher.group(3)), Integer.parseInt(matcher.group(4)), true);
                c.setScale(Integer.parseInt(matcher.group(4)));
                SQLParserUtil.setTypeInfo(pdt, c);
            }
            else {
                throw new MetadataException(QueryPlugin.Util.getString("udt_format_wrong", c.getName())); //$NON-NLS-1$
            }
        }
    }
}
