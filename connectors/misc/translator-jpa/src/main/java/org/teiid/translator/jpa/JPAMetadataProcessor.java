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
package org.teiid.translator.jpa;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.ManagedType;
import javax.persistence.metamodel.Metamodel;
import javax.persistence.metamodel.SingularAttribute;
import javax.persistence.metamodel.Type.PersistenceType;

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.metadata.Column;
import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;

/**
 * TODO:
 *  - support of abstract entities is an issue, should we represent base and extended types, just extended types?
 *
 */
@SuppressWarnings("nls")
public class JPAMetadataProcessor implements MetadataProcessor<EntityManager> {

    @ExtensionMetadataProperty(applicable=Column.class, datatype=String.class, display="Foreign Table Name", description="Applicable on Foreign Key columns")
    public static final String KEY_ASSOSIATED_WITH_FOREIGN_TABLE = MetadataFactory.JPA_PREFIX+"assosiated_with_table";
    @ExtensionMetadataProperty(applicable=Column.class, datatype=String.class, display="Relation Property", description="Applicable on Foreign Key columns")
    public static final String RELATION_PROPERTY = MetadataFactory.JPA_PREFIX+"relation_property";
    @ExtensionMetadataProperty(applicable=Column.class, datatype=String.class, display="Relation Key", description="Applicable on Foreign Key columns")
    public static final String RELATION_KEY = MetadataFactory.JPA_PREFIX+"relation_key";

    @ExtensionMetadataProperty(applicable=Table.class, datatype=String.class, display="Entity Class", description="Java Entity Class that represents this table", required=true)
    public static final String ENTITYCLASS= MetadataFactory.JPA_PREFIX+"entity_class";

    public void process(MetadataFactory mf, EntityManager entityManager) throws TranslatorException {
        Metamodel model = entityManager.getMetamodel();

        /*
         * Hibernate sometimes creates entities which don't have a javaType.
         * Envers _aud entities fall into this category. Perhaps something more
         * useful could be done with these, but for now filter them out so we
         * don't die on NullPointerException later on.
         */
        Metamodel filteredModel = new Metamodel() {
            @Override
            public <X> EntityType<X> entity(Class<X> cls) {
                return model.entity(cls);
            }

            @Override
            public <X> ManagedType<X> managedType(Class<X> cls) {
                return model.managedType(cls);
            }

            @Override
            public <X> EmbeddableType<X> embeddable(Class<X> cls) {
                return model.embeddable(cls);
            }

            @Override
            public Set<ManagedType<?>> getManagedTypes() {
                return model.getManagedTypes().stream()
                        .filter(e -> e.getJavaType() != null)
                        .collect(Collectors.toSet());
            }

            @Override
            public Set<EntityType<?>> getEntities() {
                return model.getEntities().stream()
                        .filter(e -> e.getJavaType() != null)
                        .sorted(Comparator.comparing(EntityType::getName))
                        .collect(Collectors.toSet());
            }

            @Override
            public Set<EmbeddableType<?>> getEmbeddables() {
                return model.getEmbeddables();
            }
        };

        Set<EntityType<?>> entities = filteredModel.getEntities();

        for (EntityType<?> entity:entities) {
            addEntity(mf, filteredModel, entity);
        }
    }

    private Table addEntity(MetadataFactory mf, Metamodel model, EntityType<?> entity) throws TranslatorException {
        Table table = mf.getSchema().getTable(entity.getName());
        if (table == null) {
            table = mf.addTable(entity.getName());
            table.setSupportsUpdate(true);
            table.setProperty(ENTITYCLASS, entity.getJavaType().getCanonicalName());
            addPrimaryKey(mf, model, entity, table);
            addSingularAttributes(mf, model, entity, table, Collections.EMPTY_LIST);
        }
        return table;
    }

    private boolean columnExists(String name, Table table) {
        return table.getColumnByName(name) != null;
    }

    private Column addColumn(MetadataFactory mf, String name, String type, Table entityTable) throws TranslatorException {
        if (!columnExists(name, entityTable)) {
            Column c = mf.addColumn(name, type, entityTable);
            c.setUpdatable(true);
            return c;
        }
        return entityTable.getColumnByName(name);
    }

    private void addForeignKey(MetadataFactory mf, String name, List<String> columnNames, String referenceTable, Table table) throws TranslatorException {
        ForeignKey fk = mf.addForeignKey("FK_"+name, columnNames, referenceTable, table);
        for (String column:columnNames) {
            Column c = table.getColumnByName(column);
            c.setProperty(KEY_ASSOSIATED_WITH_FOREIGN_TABLE, mf.getName()+Tokens.DOT+referenceTable);
        }
        fk.setNameInSource(name);
    }

    private void addSingularAttributes(MetadataFactory mf, Metamodel model,
            ManagedType<?> entity, Table entityTable, List<String> path)
            throws TranslatorException {
        List<Attribute<?,?>> attributes = new ArrayList<>(entity.getAttributes());
        Collections.sort(attributes, Comparator.comparing(Attribute::getName));

        for (Attribute<?, ?> attr:attributes) {
            if (!attr.isCollection()) {
                List<String> attrPath = new LinkedList<>(path);
                attrPath.add(attr.getName());

                boolean simpleType = isSimpleType(attr.getJavaType());
                if (simpleType) {
                    Column column = addColumn(mf, String.join("_", attrPath),
                            TypeFacility.getDataTypeName(getJavaDataType(attr.getJavaType())), entityTable);
                    if (((SingularAttribute)attr).isOptional()) {
                        column.setDefaultValue(null);
                    }
                    column.setNameInSource(String.join(".", attrPath));
                }
                else if (attr.getJavaType().isEnum()) {
                    Column column = addColumn(mf, String.join("_", attrPath),
                            DataTypeManager.DefaultDataTypes.STRING, entityTable);
                    if (((SingularAttribute)attr).isOptional()) {
                        column.setDefaultValue(null);
                    }
                    column.setNativeType(attr.getJavaType().getName());
                    column.setNameInSource(String.join(".", attrPath));
                }
                else {
                    boolean classFound = false;
                    // If this attribute is a @Embeddable then add its columns as
                    // this tables columns
                    for (EmbeddableType<?> embeddable:model.getEmbeddables()) {
                        if (embeddable.getJavaType().equals(attr.getJavaType())) {
                            addSingularAttributes(mf, model, embeddable, entityTable, attrPath);
                            classFound = true;
                            break;
                        }
                    }

                    if (!classFound) {
                        // if the attribute is @Entity then add entity's PK as column on this
                        // table, then add that column as FK
                        for (EntityType et:model.getEntities()) {
                            if (et.getJavaType().equals(attr.getJavaType())) {
                                Table attributeTable = addEntity(mf, model, et);
                                KeyRecord pk = attributeTable.getPrimaryKey();
                                if (pk != null) { // TODO: entities must have PK, so this check is not needed.
                                    ArrayList<String> keys = new ArrayList<String>();
                                    for (Column column:pk.getColumns()) {
                                        String fk = attr.getName() + "_" + column.getName();
                                        Column c = addColumn(mf, fk, column.getDatatype().getRuntimeTypeName(), entityTable);
                                        c.setProperty(RELATION_PROPERTY, attr.getName());
                                        c.setProperty(RELATION_KEY, column.getName());
                                        c.setNameInSource(column.getNameInSource());
                                        keys.add(fk);
                                    }
                                    if (!foreignKeyExists(keys, entityTable)) {
                                        addForeignKey(mf, attr.getName(), keys, attributeTable.getName(), entityTable);
                                    }
                                }
                                else {
                                    throw new TranslatorException(JPAPlugin.Util.gs(JPAPlugin.Event.TEIID14001, attributeTable.getName()));
                                }
                                classFound = true;
                                break;
                            }
                        }
                    }

                    if (!classFound) {
                        throw new TranslatorException(JPAPlugin.Util.gs(JPAPlugin.Event.TEIID14002, attr.getName()));
                    }
                }
            }
        }
    }

    private boolean isSimpleType(Class type) {
        return type.isPrimitive() || type.equals(String.class)
                || type.equals(BigDecimal.class) || type.equals(Date.class)
                || type.equals(BigInteger.class)
                || TypeFacility.getRuntimeType(type) != Object.class;
    }

    private boolean foreignKeyExists(List<String> keys, Table forignTable) {
        boolean fkExists = false;
        for (ForeignKey fk:forignTable.getForeignKeys()) {
            boolean allKeysFound = true;
            for (String key:keys) {
                boolean keyfound = false;
                for (Column col:fk.getColumns()) {
                    if (col.getName().equals(key)) {
                        keyfound = true;
                        break;
                    }
                }
                if (!keyfound) {
                    allKeysFound = false;
                    break;
                }
            }

            if (allKeysFound) {
                fkExists = true;
                break;
            }
        }
        return fkExists;
    }

    private void addPrimaryKey(MetadataFactory mf, Metamodel model, EntityType<?> entity, Table entityTable) throws TranslatorException {
        // figure out the PK
        if (entity.hasSingleIdAttribute()) {
            if (entity.getIdType().getPersistenceType().equals(PersistenceType.BASIC)) {
                SingularAttribute<?, ?> pkattr = entity.getId(entity.getIdType().getJavaType());
                addColumn(mf, pkattr.getName(), TypeFacility.getDataTypeName(getJavaDataType(pkattr.getJavaType())), entityTable);
                mf.addPrimaryKey("PK_"+entity.getName(), Arrays.asList(pkattr.getName()), entityTable); //$NON-NLS-1$
            }
            else if (entity.getIdType().getPersistenceType().equals(PersistenceType.EMBEDDABLE)) {
                SingularAttribute<?, ?> pkattr = entity.getId(entity.getIdType().getJavaType());
                for (EmbeddableType<?> embeddable:model.getEmbeddables()) {
                    if (embeddable.getJavaType().equals(pkattr.getJavaType())) {
                        addSingularAttributes(mf, model, embeddable, entityTable, Collections.singletonList(pkattr.getName()));
                        ArrayList<String> keys = new ArrayList<String>();
                        for (Attribute<?, ?> attr:embeddable.getAttributes()) {
                            if (isSimpleType(attr.getJavaType())
                                    || attr.getJavaType().isEnum()) {
                                keys.add(String.join("_", pkattr.getName(), attr.getName()));
                            } else {
                                throw new TranslatorException(JPAPlugin.Util.gs(JPAPlugin.Event.TEIID14003, entityTable.getName()));
                            }
                        }
                        mf.addPrimaryKey("PK_"+pkattr.getName(), keys, entityTable);
                        break;
                    }
                }
            }
        }
        else {
            // Composite PK. If the PK is specified with @IdClass then read its attributes,
            // if those attributes are not found, add them as columns then as composite PK
            ArrayList<String> keys = new ArrayList<String>();
            for (Object obj:entity.getIdClassAttributes()) {
                SingularAttribute<?, ?> attr = (SingularAttribute)obj;
                addColumn(mf, attr.getName(), TypeFacility.getDataTypeName(getJavaDataType(attr.getJavaType())), entityTable);
                keys.add(attr.getName());
            }
            mf.addPrimaryKey("PK_"+entity.getName(), keys, entityTable);
        }
    }

    private Class getJavaDataType(Class type) {
        return TypeFacility.getRuntimeType(type);
    }

}
