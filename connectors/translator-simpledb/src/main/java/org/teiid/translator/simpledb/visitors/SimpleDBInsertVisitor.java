package org.teiid.translator.simpledb.visitors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.language.ColumnReference;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.visitor.HierarchyVisitor;
public class SimpleDBInsertVisitor extends HierarchyVisitor {
	
	private Map<String, String> columnsValuesMap;
	private List<String> columnNames;
	
	public SimpleDBInsertVisitor() {
		columnsValuesMap = new HashMap<String, String>();
		columnNames = new ArrayList<String>();
	}
	
	public Map<String, String> returnColumnsValuesMap() {
		return columnsValuesMap;
	}
	
	public static Map<String, String> getColumnsValuesMap(Insert insert) {
		SimpleDBInsertVisitor visitor = new SimpleDBInsertVisitor();
		visitor.visit(insert);
		return visitor.returnColumnsValuesMap();
	}
	
	public static String getDomainName(Insert insert){
		return insert.getTable().getName();
	}
	
	@Override
	public void visit(ColumnReference obj) {
		columnNames.add(obj.getName());
		super.visit(obj);
	}
	
	@Override
	public void visit(ExpressionValueSource obj) {
		List<Expression> values = obj.getValues();
		for (int i = 0; i< obj.getValues().size(); i++){
			if (values.get(i) instanceof Literal){
				Literal lit = (Literal) values.get(i);
				columnsValuesMap.put(columnNames.get(i), (String) lit.getValue());
			}else{
				throw new RuntimeException("Just literals are allowed in VALUES section so far");
			}
		}
		super.visit(obj);
	}
}
