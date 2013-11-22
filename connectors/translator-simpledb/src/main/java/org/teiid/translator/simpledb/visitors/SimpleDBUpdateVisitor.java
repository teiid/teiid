package org.teiid.translator.simpledb.visitors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.language.Comparison;
import org.teiid.language.Literal;
import org.teiid.language.SetClause;
import org.teiid.language.Update;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.resource.adpter.simpledb.SimpleDbAPIClass;

public class SimpleDBUpdateVisitor extends HierarchyVisitor{

	private SimpleDbAPIClass apiClass;
	private String tableName;
	private Map<String, String> attributes;
	private List<String> itemNames;
	
	public SimpleDBUpdateVisitor(Update update, SimpleDbAPIClass apiClass) {
		attributes = new HashMap<String, String>();
		itemNames = new ArrayList<String>();
		this.apiClass = apiClass;
		visit(update);
	}
	
	@Override
	public void visit(Update obj) {
		tableName = obj.getTable().getName();
		for(SetClause setClause : obj.getChanges()){
			visitNode(setClause);
		}
		if (obj.getWhere() != null){
			visitNode(obj.getWhere());
		}
		super.visit(obj);
	}
	
	@Override
	public void visit(SetClause obj) {
		if (obj.getValue() instanceof Literal){
			Literal l = (Literal) obj.getValue();
			attributes.put(obj.getSymbol().getName(), (String) l.getValue());
		}
		super.visit(obj);
	}
	
	@Override
	public void visit(Comparison obj) {
		ArrayList<String> columns = new ArrayList<String>();
		columns.add("itemName()");
		List<List<String>> response = apiClass.performSelect("SELECT itemName() FROM "+tableName+" WHERE "+SimpleDBSQLVisitor.getSQLString(obj), columns);
		for (List<String> list : response) {
			itemNames.add(list.get(0));
		}
		super.visit(obj);
	}

	public String getTableName() {
		return tableName;
	}

	public Map<String, String> getAttributes() {
		return attributes;
	}

	public List<String> getItemNames() {
		return itemNames;
	}
	
	
	
}
