/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.translator.odata4;

import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.olingo.client.api.edm.xml.XMLMetadata;
import org.apache.olingo.client.core.serialization.ClientODataDeserializerImpl;
import org.apache.olingo.commons.api.edm.provider.CsdlAction;
import org.apache.olingo.commons.api.edm.provider.CsdlActionImport;
import org.apache.olingo.commons.api.edm.provider.CsdlComplexType;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainer;
import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.api.edm.provider.CsdlFunction;
import org.apache.olingo.commons.api.edm.provider.CsdlFunctionImport;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationPropertyBinding;
import org.apache.olingo.commons.api.edm.provider.CsdlOperation;
import org.apache.olingo.commons.api.edm.provider.CsdlParameter;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlPropertyRef;
import org.apache.olingo.commons.api.edm.provider.CsdlReferentialConstraint;
import org.apache.olingo.commons.api.edm.provider.CsdlReturnType;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.apache.olingo.commons.api.edm.provider.CsdlSingleton;
import org.apache.olingo.commons.api.format.ODataFormat;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.WSConnection;
import org.teiid.translator.ws.BinaryWSProcedureExecution;

public class ODataMetadataProcessor implements MetadataProcessor<WSConnection> {
    
    enum Association {ONE, MANY};
    private static final String PARENT_TABLE = "PARENT_TABLE"; //$NON-NLS-1$
    private static final String ASSOSIATION = "ASSOSIATION"; //$NON-NLS-1$
    private static final String CONSTRAINT = "CONSTRAINT"; //$NON-NLS-1$
    
    @ExtensionMetadataProperty(applicable=Table.class, datatype=String.class, display="Link Tables", 
            description="Used to define navigation relationship in many to many case")    
	public static final String LINK_TABLES = MetadataFactory.ODATA_URI+"LinkTables"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable=Procedure.class, datatype=String.class, display="Http Method", 
            description="Http method used for procedure invocation", required=true)
    public static final String HTTP_METHOD = MetadataFactory.ODATA_URI+"HttpMethod"; //$NON-NLS-1$
    
    @ExtensionMetadataProperty(applicable=Column.class, datatype=Boolean.class, display="Join Column", 
            description="On Link tables this property defines the join column")    
	public static final String JOIN_COLUMN = MetadataFactory.ODATA_URI+"JoinColumn"; //$NON-NLS-1$
    
    @ExtensionMetadataProperty(applicable = { Table.class, Procedure.class }, datatype = String.class, 
            display = "Entity Type Name", description = "Name of the Entity Type in EDM", required = true)    
	public static final String ENTITY_TYPE = MetadataFactory.ODATA_URI+"EntityType"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable=Column.class, datatype=String.class, display="Complex Type Name", 
            description="Name of the Complex Type in EDM")
    public static final String COMPLEX_TYPE = MetadataFactory.ODATA_URI+"ComplexType"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable=Column.class, datatype=String.class, display="Column Group",
            description="Name of the Column Group")
	public static final String COLUMN_GROUP = MetadataFactory.ODATA_URI+"ColumnGroup"; //$NON-NLS-1$

	private String schemaNamespace;
	private ODataExecutionFactory ef;

	public void setExecutionfactory(ODataExecutionFactory ef) {
        this.ef = ef;
    }
	
    private XMLMetadata getSchema(WSConnection conn) throws TranslatorException {
        try {
            BaseQueryExecution execution = new BaseQueryExecution(ef, null, null, conn);
            BinaryWSProcedureExecution call = execution.executeDirect("GET", "$metadata", null, execution.getDefaultHeaders()); //$NON-NLS-1$ //$NON-NLS-2$
            if (call.getResponseCode() != HttpStatusCode.OK.getStatusCode()) {
                throw execution.buildError(call);
            }

            Blob out = (Blob)call.getOutputParameterValues().get(0);
            ClientODataDeserializerImpl deserializer = new ClientODataDeserializerImpl(false, ODataFormat.APPLICATION_XML);
            XMLMetadata metadata = deserializer.toMetadata(out.getBinaryStream());
            return metadata;
        } catch (SQLException e) {
            throw new TranslatorException(e);
        } catch (Exception e) {
            throw new TranslatorException(e);
        }
	}
    
	public void process(MetadataFactory mf, WSConnection conn) throws TranslatorException {
	    XMLMetadata metadata = getSchema(conn);
	    getMetadata(mf, metadata);
	}
	
	public void getMetadata(MetadataFactory mf, XMLMetadata metadata) throws TranslatorException {
	    CsdlSchema csdlSchema = getDefaultSchema(metadata);
	    CsdlEntityContainer container = csdlSchema.getEntityContainer();

		// add entity sets as tables
		for (CsdlEntitySet entitySet:container.getEntitySets()) {
			addTable(mf, entitySet.getName(), entitySet.getType(), metadata);
		}
		
        // add entity sets as tables
        for (CsdlSingleton singleton:container.getSingletons()) {
            addTable(mf, singleton.getName(), singleton.getType(), metadata);
        }		

		// build relations ships among tables
		for (CsdlEntitySet entitySet:container.getEntitySets()) {
			addNavigationProperties(mf, entitySet.getName(), entitySet, metadata);
		}

		// add functions
		for (CsdlFunctionImport function:container.getFunctionImports()) {
			addFunctionImportAsProcedure(mf, function, metadata);
		}
		
        // add actions
        for (CsdlActionImport action:container.getActionImports()) {
            addActionImportAsProcedure(mf, action, metadata);
        }
        
        for (Table table:mf.getSchema().getTables().values()) {
            String parentTable = table.getProperty(PARENT_TABLE, false);
            if (parentTable != null) {
                addForeignKey(mf, table, mf.getSchema().getTable(parentTable));
            }
        }
        
	}

    private CsdlSchema getDefaultSchema(XMLMetadata metadata) throws TranslatorException {
        CsdlSchema csdlSchema = null;
        if (this.schemaNamespace != null) {
            csdlSchema = metadata.getSchema(this.schemaNamespace);
        } 
        else {
            if (!metadata.getSchemas().isEmpty()) {
                csdlSchema = metadata.getSchemas().get(0);
            }
        }
        if(csdlSchema == null) {
            throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17019));
        }
        return csdlSchema;
    }

    private Table buildTable(MetadataFactory mf, String name) {
		Table table = mf.addTable(name);
		table.setSupportsUpdate(true);
		return table;
	}

	private boolean isSimple(String type) {
	    return type.startsWith("Edm");
	}
	
	private boolean isComplexType(XMLMetadata metadata, String type) throws TranslatorException {
	    return getComplexType(metadata, type) != null;
	}
	
    private boolean isEntityType(XMLMetadata metadata, String type) throws TranslatorException {
        return getEntityType(metadata, type) != null;
    }
	
    private Table addTable(MetadataFactory mf, String tableName, String entityType, XMLMetadata metadata) 
            throws TranslatorException {
        Table table = buildTable(mf, tableName);
        table.setProperty(ENTITY_TYPE, entityType);
        
        CsdlEntityType type = getEntityType(metadata, entityType);
        addEntityTypeProperties(mf, metadata, table, type);
        return table;
    }	
	
    private void addEntityTypeProperties(MetadataFactory mf,
            XMLMetadata metadata, Table table, CsdlEntityType entityType) throws TranslatorException {
        // add columns; add complex types as child tables with 1-1 or 1-many relation
		for (CsdlProperty property:entityType.getProperties()) {
			addProperty(mf, metadata, table, property);
		}
		
		// add properties from base type; if any to flatten the model
		String baseType = entityType.getBaseType();
		while(baseType != null) {
		    CsdlEntityType baseEntityType = getEntityType(metadata, baseType);
	        for (CsdlProperty property:baseEntityType.getProperties()) {
	            addProperty(mf, metadata, table, property);
	        }
	        baseType = baseEntityType.getBaseType();
		}

		// add PK
		addPrimaryKey(mf, table, entityType.getKey()); //$NON-NLS-1$
    }
	
    private void addProperty(MetadataFactory mf, XMLMetadata metadata,
            Table table, CsdlProperty property) throws TranslatorException {
        if (isSimple(property.getType())) {
            addPropertyAsColumn(mf, table, property);
        }
        else {
            CsdlComplexType childType = (CsdlComplexType)getComplexType(metadata, property.getType());
            addComplexPropertyAsTable(mf, childType, metadata, table, property.isCollection());
        }
    }
    
    private void addComplexPropertyAsTable(MetadataFactory mf, CsdlComplexType complexType, 
            XMLMetadata metadata, Table parentTable, boolean collection) throws TranslatorException {
	    
        Table childTable = buildTable(mf, parentTable.getName()+"."+complexType.getName());
        childTable.setProperty(COMPLEX_TYPE, complexType.getName()); // complex type
        childTable.setProperty(PARENT_TABLE, parentTable.getFullName());
        childTable.setProperty(ASSOSIATION, collection?Association.MANY.name():Association.ONE.name());
        
	    for (CsdlProperty property:complexType.getProperties()) {
	        addProperty(mf, metadata, childTable, property);
        }
	    
        // add properties from base type; if any to flatten the model
        String baseType = complexType.getBaseType();
        while(baseType != null) {
            CsdlComplexType baseComplexType = getComplexType(metadata, baseType);
            for (CsdlProperty property:baseComplexType.getProperties()) {
                addProperty(mf, metadata, childTable, property);
            }
            baseType = baseComplexType.getBaseType();
        }
    }

    void addPrimaryKey(MetadataFactory mf, Table table, List<CsdlPropertyRef> keys) throws TranslatorException {
	    List<String> pkNames = new ArrayList<String>();
	    for (CsdlPropertyRef ref: keys) {
	        pkNames.add(ref.getName());
	        if (ref.getAlias() != null) {
                throw new TranslatorException(ODataPlugin.Util.gs(
                        ODataPlugin.Event.TEIID17018, table.getName(),ref.getName()));
	        }
	    }
	}
    
    private CsdlNavigationPropertyBinding getNavigationPropertyBinding(CsdlEntitySet entitySet, String name) {
        List<CsdlNavigationPropertyBinding> bindings = entitySet.getNavigationPropertyBindings();
        for (CsdlNavigationPropertyBinding binding:bindings) {
            if (binding.getPath().equals(name)) {
                return binding;
            }
        }
        return null;
    }
    
    private CsdlEntityType getEntityType(XMLMetadata metadata, String name) throws TranslatorException {
        if (name.contains(".")) {
            int idx = name.lastIndexOf('.');
            CsdlSchema schema = metadata.getSchema(name.substring(0, idx));
            if (schema == null) {
                throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17021, name));
            }
            return schema.getEntityType(name.substring(idx+1));
        }
        return getDefaultSchema(metadata).getEntityType(name);
    }
    
    private List<CsdlFunction> getFunctions(XMLMetadata metadata, String name) throws TranslatorException {
        if (name.contains(".")) {
            int idx = name.lastIndexOf('.');
            CsdlSchema schema = metadata.getSchema(name.substring(0, idx));
            if (schema == null) {
                throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17021, name));
            }
            return schema.getFunctions(name.substring(idx+1));
        }
        return getDefaultSchema(metadata).getFunctions(name);
    }    

    private List<CsdlAction> getActions (XMLMetadata metadata, String name) throws TranslatorException {
        if (name.contains(".")) {
            int idx = name.lastIndexOf('.');
            CsdlSchema schema = metadata.getSchema(name.substring(0, idx));
            if (schema == null) {
                throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17021, name));
            }
            return schema.getActions(name.substring(idx+1));
        }
        return getDefaultSchema(metadata).getActions(name);
    }    
    
    private CsdlComplexType getComplexType(XMLMetadata metadata, String name) throws TranslatorException {
        if (name.contains(".")) {
            int idx = name.lastIndexOf('.');
            CsdlSchema schema = metadata.getSchema(name.substring(0, idx));
            if (schema == null) {
                throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17021, name));
            }
            return schema.getComplexType(name.substring(idx+1));
        }
        return getDefaultSchema(metadata).getComplexType(name);
    }    

	void addNavigationProperties(MetadataFactory mf, String tableName, CsdlEntitySet entitySet, XMLMetadata metadata) 
	        throws TranslatorException {
	    
		Table fromTable = mf.getSchema().getTable(tableName);
		CsdlEntityType fromEntityType = getEntityType(metadata, entitySet.getType());

		Table toTable = null; 
		for(CsdlNavigationProperty property:fromEntityType.getNavigationProperties()) {
		    CsdlNavigationPropertyBinding binding = getNavigationPropertyBinding(entitySet, property.getName());
		    if (binding != null) {
		        String target = binding.getTarget();
		        if (target.contains("/")) {
		            throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17020, target));
		        }
		        else {
		            toTable = mf.getSchema().getTable(target);
		        }
		    }
		    else {
		        // this means there is no EntitySet defined for this EntityType, thus there will be
		        // no table for this. Need to create a one.
		        toTable = addTable(mf, property.getName(), property.getType(), metadata);
		        toTable.setProperty(PARENT_TABLE, fromTable.getFullName());
		    }
		    
			// support for self-joins
			if (same(fromTable, toTable)) {
			    StringBuilder name = new StringBuilder().append(fromTable.getName())
			            .append(".").append(property.getName());
				toTable = addTable(mf, name.toString(), fromEntityType.getName(), metadata);
				toTable.setProperty(PARENT_TABLE, fromTable.getFullName());							
			}

			toTable.setProperty(ASSOSIATION, property.isCollection()?Association.MANY.name():Association.ONE.name());
			int i = 0;
			for (CsdlReferentialConstraint constraint : property.getReferentialConstraints()) {
			    toTable.setProperty(CONSTRAINT+i, constraint.getReferencedProperty()+","+constraint.getProperty());
			}
		}
	}
	
	private KeyRecord getPK(MetadataFactory mf, Table table) throws TranslatorException {
	    KeyRecord record = table.getPrimaryKey();
	    if (record == null) {
	        String parentTable = table.getProperty(PARENT_TABLE, false);
	        if (parentTable == null) {
	            throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17024, table.getName()));
	        }
	        return getPK(mf, mf.getSchema().getTable(parentTable));
	    }
	    return record;
	}
	
    private void addForeignKey(MetadataFactory mf, Table childTable, Table table) throws TranslatorException {
        Association association = Association.valueOf(childTable.getProperty(ASSOSIATION, false));
        childTable.setProperty(ASSOSIATION, null);
        if (association == Association.ONE) {
            KeyRecord record = getPK(mf, table);
            if (record != null) {
                ArrayList<String> pkColumns = new ArrayList<String>(); 
                for (Column column:record.getColumns()) {
                    Column c = mf.getSchema().getTable(childTable.getName()).getColumnByName(column.getName());
                    if (c == null) {
                        c = mf.addColumn(column.getName(), column.getRuntimeType(), childTable);
                    }
                    pkColumns.add(c.getName());
                }
                mf.addPrimaryKey("PK0", pkColumns, childTable); //$NON-NLS-1$
                mf.addForiegnKey("FK0", pkColumns, table.getName(), childTable); //$NON-NLS-1$
            }
        }
        else {
            KeyRecord record = getPK(mf,table);
            if (record != null) {
                ArrayList<String> pkColumns = new ArrayList<String>(); 
                for (Column column:record.getColumns()) {
                    Column c = mf.getSchema().getTable(childTable.getName()).getColumnByName(table.getName()+"_"+column.getName()); //$NON-NLS-1$
                    if (c == null) {
                        c = mf.addColumn(table.getName()+"_"+column.getName(), column.getRuntimeType(), childTable); //$NON-NLS-1$
                    }
                    pkColumns.add(c.getName());
                }
                mf.addForiegnKey("FK0", pkColumns, table.getName(), childTable); //$NON-NLS-1$                
            }            
        }
    }	

	boolean same(Table x, Table y) {
		return (x.getFullName().equalsIgnoreCase(y.getFullName()));
	}

	boolean keyMatches(List<String> names, KeyRecord record) {
		if (names.size() != record.getColumns().size()) {
			return false;
		}
		Set<String> keyNames = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
		for (Column c: record.getColumns()) {
			keyNames.add(c.getName());
		}
		for (int i = 0; i < names.size(); i++) {
			if (!keyNames.contains(names.get(i))) {
				return false;
			}
		}
		return true;
	}

    private Column addPropertyAsColumn(MetadataFactory mf, Table table, CsdlProperty property) {
		
        Column c = buildColumn(mf, table, property);
		
		c.setNullType(property.isNullable()?NullType.Nullable:NullType.No_Nulls);
		
		if (property.getMaxLength() != null) {
			c.setLength(property.getMaxLength());
		}
		c.setPrecision(property.getPrecision());
		c.setScale(property.getScale());
		
		if (property.getDefaultValue() != null) {
		    c.setDefaultValue(property.getDefaultValue());
		}
		
		if (property.getMimeType() != null) {
		    c.setProperty("MIME-TYPE", property.getMimeType());
		}
		
		if (property.getSrid() != null) {
		    c.setProperty("SRID", property.getSrid().toString());
		}
		return c;
	}
    
    private ProcedureParameter addPrameterAsColumn(MetadataFactory mf, Procedure procedure, CsdlParameter parameter) {
        ProcedureParameter p = mf.addProcedureParameter(
                parameter.getName(),
                ODataTypeManager.teiidType(parameter.getType(),parameter.isCollection()), 
                ProcedureParameter.Type.In,
                procedure);
        
        p.setNullType(parameter.isNullable()?NullType.Nullable:NullType.No_Nulls);
        
        if (parameter.getMaxLength() != null) {
            p.setLength(parameter.getMaxLength());
        }
        p.setPrecision(parameter.getPrecision());
        p.setScale(parameter.getScale());
        
        if (parameter.getSrid() != null) {
            p.setProperty("SRID", parameter.getSrid().toString());
        }
        return p;
    }    

    private Column buildColumn(MetadataFactory mf, Table table, CsdlProperty property) {
		String columnName = property.getName();
		Column c = mf.addColumn(columnName, ODataTypeManager.teiidType(property.getType(), 
		        property.isCollection()), table);
		c.setUpdatable(true);
		return c;
	}

    private void addParameter(MetadataFactory mf, XMLMetadata metadata,
            Procedure procedure, CsdlParameter parameter) throws TranslatorException {
        if (isSimple(parameter.getType())) {
            addPrameterAsColumn(mf, procedure, parameter);
        }
        else {
            throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17022, parameter.getName()));
        }
    }
    
    void addFunctionImportAsProcedure(MetadataFactory mf, CsdlFunctionImport functionImport, XMLMetadata metadata)
            throws TranslatorException {
		List<CsdlFunction> functions = getFunctions(metadata, functionImport.getFunction());
		for (CsdlFunction function : functions) {
	        Procedure procedure = mf.addProcedure(function.getName());
	        procedure.setProperty(HTTP_METHOD, "GET");
	        addOperation(mf, metadata, function, procedure);
		}
	}

    private void addProcedureTableReturn(MetadataFactory mf, Procedure procedure, CsdlComplexType type)
            throws TranslatorException {
        for (CsdlProperty property:type.getProperties()) {
            if (isSimple(property.getType())) {
                mf.addProcedureResultSetColumn(
                        property.getName(),
                        ODataTypeManager.teiidType(property.getType(), property.isCollection()), procedure);
            }
            else {
                throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17023, procedure.getName()));
            }
        }
    }
    
    
    private void addProcedureTableReturn(MetadataFactory mf, Procedure procedure, CsdlEntityType type)
            throws TranslatorException {
        for (CsdlProperty property:type.getProperties()) {
            if (isSimple(property.getType())) {
                mf.addProcedureResultSetColumn(
                        property.getName(),
                        ODataTypeManager.teiidType(property.getType(), property.isCollection()), procedure);
            }
            else {
                throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17023, procedure.getName()));
            }
        }
    }
    private void addActionImportAsProcedure(MetadataFactory mf, CsdlActionImport actionImport, 
            XMLMetadata metadata) throws TranslatorException {
        List<CsdlAction> actions = getActions(metadata, actionImport.getAction());
        
        for (CsdlAction function : actions) {
            Procedure procedure = mf.addProcedure(function.getName());
            procedure.setProperty(HTTP_METHOD, "POST");
            
            addOperation(mf, metadata, function, procedure);
        }
        
    }

    private void addOperation(MetadataFactory mf, XMLMetadata metadata,
            CsdlOperation function, Procedure procedure)
            throws TranslatorException {
        for (CsdlParameter parameter : function.getParameters()) {
            addParameter(mf, metadata, procedure, parameter);
        }
        
        CsdlReturnType returnType = function.getReturnType();
        if (isSimple(returnType.getType())) {
            mf.addProcedureParameter("return", ODataTypeManager.teiidType(returnType.getType(), 
                    returnType.isCollection()), ProcedureParameter.Type.ReturnValue, procedure); //$NON-NLS-1$              
        } 
        else if (isComplexType(metadata, returnType.getType())) {
            procedure.setProperty(COMPLEX_TYPE, function.getReturnType().getType());
            addProcedureTableReturn(mf, procedure, getComplexType(metadata, returnType.getType()));
        }
        else if (isEntityType(metadata, returnType.getType())){
            procedure.setProperty(ENTITY_TYPE, function.getReturnType().getType());
            addProcedureTableReturn(mf, procedure, getEntityType(metadata, returnType.getType()));              
        }
        else {
            throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17005, function.getName(), returnType.getType()));
        }
    }

	public void setSchemaNamespace(String namespace) {
		this.schemaNamespace = namespace;
	}

	List<String> getColumnNames(List<Column> columns){
		ArrayList<String> names = new ArrayList<String>();
		for (Column c:columns) {
			names.add(c.getName());
		}
		return names;
	}

	@TranslatorProperty(display="Schema Namespace", category=PropertyType.IMPORT, description="Namespace of the schema to import")
    public String getSchemaNamespace() {
        return schemaNamespace;
    }
}
