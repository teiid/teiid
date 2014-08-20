package org.teiid.eclipselink.platform;

import java.util.Hashtable;

import org.eclipse.persistence.internal.databaseaccess.FieldTypeDefinition;
import org.eclipse.persistence.platform.database.DatabasePlatform;

public class TeiidServerPlatform extends DatabasePlatform{

	private static final long serialVersionUID = 6894570254643353289L;
	
	public TeiidServerPlatform() {
		super();
		this.pingSQL = "SELECT 1";		
	}

	
	protected Hashtable buildFieldTypes() {
		
		Hashtable fieldTypeMapping = super.buildFieldTypes();
		
		fieldTypeMapping.put(Boolean.class, new FieldTypeDefinition("BOOLEAN", false));
		fieldTypeMapping.put(Integer.class, new FieldTypeDefinition("INTEGER", false));
		
		
		fieldTypeMapping.put(java.sql.Date.class, new FieldTypeDefinition("DATE", false));
		fieldTypeMapping.put(java.sql.Timestamp.class, new FieldTypeDefinition("TIMESTAMP", false));
		fieldTypeMapping.put(java.sql.Time.class, new FieldTypeDefinition("TIME", false));
		fieldTypeMapping.put(java.util.Calendar.class, new FieldTypeDefinition("TIMESTAMP", false));
		fieldTypeMapping.put(java.util.Date.class, new FieldTypeDefinition("TIMESTAMP", false));
		
		fieldTypeMapping.put(Long.class, new FieldTypeDefinition("BIGINT", false));
		fieldTypeMapping.put(java.math.BigInteger.class, new FieldTypeDefinition("BIGINT", false));
		fieldTypeMapping.put(Short.class, new FieldTypeDefinition("SMALLINT"));
        fieldTypeMapping.put(Byte.class, new FieldTypeDefinition("SMALLINT"));
        fieldTypeMapping.put(Float.class, new FieldTypeDefinition("REAL"));
        fieldTypeMapping.put(Double.class, new FieldTypeDefinition("DOUBLE"));
		
		return fieldTypeMapping;
	}
	


	public static void main(String[] args) {
		new TeiidServerPlatform().buildFieldTypes();
	}

	/**
	 * Avoid alter/create Constraint/index
	 */
	public boolean supportsDeleteOnCascade() {
		return false;
	}

	public boolean supportsForeignKeyConstraints() {
		return false;
	}

	public boolean requiresUniqueConstraintCreationOnTableCreate() {
		return false;
	}
	
	public boolean supportsIndexes() {
		return false;
	}

	public boolean supportsTempTables() {
		return true;
	}

	public boolean supportsLocalTempTables() {
		return true;
	}

	public boolean supportsGlobalTempTables() {
		return false;
	}

	public void setPrintOuterJoinInWhereClause(boolean printOuterJoinInWhereClause) {
		super.setPrintOuterJoinInWhereClause(false);
	}

	public String getCreateViewString() {
		throw new RuntimeException("Teiid Server don't support create view in runtime");
	}

	
	
	

}
