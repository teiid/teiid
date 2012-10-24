package org.teiid.translator.object.metadata;

import java.lang.reflect.Method;

import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.metadata.Column.SearchType;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;

public class JavaBeanMetadataProcessor extends BaseMetadataProcessor {
	
	
	private boolean isUpdatable = false;

	
	protected String getTableName(Class<?> entity) {
		if (entity == null) {
			return "tableName";
		}
		String name = null;
		String className = entity.getName();
		int idx = className.lastIndexOf(".");
		if (idx > 0) {
			name = className.substring(idx + 1);
		} else {
			name = className;
		}
		return name;		
		
	}
	
	protected boolean isUpdateable(Class<?> entity) {
		return this.isUpdatable;
	}

	protected boolean isUpdateable(Class<?> entity, String columnName) {
		return this.isUpdatable;
	}
	
	protected String createViewTransformation(MetadataFactory mf, Class<?> entity, Table vtable, Table sourceTable) throws TranslatorException {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT ");
		
		StringBuilder sbObjTable = new StringBuilder();
		
		
		Method[] methods = entity.getDeclaredMethods();

		int cnt = 0;
		for (Method m : methods) {
			String methodName = m.getName();
			if (methodName.startsWith(GET)) {
				methodName=methodName.substring( methodName.indexOf(GET) + 3);
			} else if (methodName.startsWith(IS)) {
				methodName=methodName.substring( methodName.indexOf(IS) + 2);
			} else {
				continue;
			}
			boolean simpleType = isSimpleType(m.getReturnType());
			boolean returnType = simpleType;
			if (!simpleType) {
				returnType = isSupportedObjectType(m.getReturnType());
			}
			
			// if the object type is not supported to be returned as is, then don't include it,
			// this is an object that will need a chaining OBJECTTABLE specified
			if (! returnType) continue;
			
			if (cnt > 0) {
				sb.append(", ");
				sbObjTable.append(", ");
			}
			
			sb.append("o.");
			sb.append(methodName);
			
			sbObjTable.append(methodName);
			sbObjTable.append(" ");
			
			
			String simpleName = m.getReturnType().getSimpleName();
			
			Column column = addColumn(mf, entity, methodName, "", SearchType.Searchable, TypeFacility.getDataTypeName(getJavaDataType(m.getReturnType())), false, vtable);
		
			sbObjTable.append(column.getRuntimeType());
			sbObjTable.append(" ");
			sbObjTable.append("'teiid_row.");
			sbObjTable.append(methodName);
			sbObjTable.append("'");
			
			column.setNativeType(simpleName);

			if (simpleName.equalsIgnoreCase("string")) {
				column.setLength(4000);
			}
			if (!simpleType) {
					column.setSearchType(SearchType.Unsearchable);
			}
			++cnt;
		}
		
		Column sourceColumn = sourceTable.getColumns().get(0);
		
		sb.append(" FROM ");
		sb.append(sourceTable.getName());
		sb.append(" as T, OBJECTTABLE('x' PASSING T.");
		sb.append(sourceColumn.getName());
		sb.append(" AS x COLUMNS ");
		
		sb.append(sbObjTable.toString());
		
		sb.append(") as o;");
		
		return sb.toString();
		
	}

}
