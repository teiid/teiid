package org.teiid.translator.simpledb.visitors;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.Delete;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.resource.adpter.simpledb.SimpleDbAPIClass;

public class SimpleDBDeleteVisitor extends HierarchyVisitor {

	private boolean hasWhere = false;
	private String tableName;
	private boolean isSimpleDelete = false;
	private String itemName = "";
	private SimpleDbAPIClass apiClass;
	private List<String> itemNames;
	
	public SimpleDBDeleteVisitor(Delete delete, SimpleDbAPIClass apiClass) {
		this.apiClass = apiClass;
		visit(delete);
	}
	
	public String getTableName(){
		return tableName;
	}
	
	public boolean hasWhere(){
		return hasWhere;
	}
	
	public boolean isSimpleDelete(){
		return isSimpleDelete;
	}
	
	public String getItemName(){
		return itemName;
	}
	
	public List<String> getItemNames() {
		return itemNames;
	}

	public void setItemNames(List<String> itemNames) {
		this.itemNames = itemNames;
	}

	@Override
	public void visit(Delete obj) {
		visitNode(obj.getTable());
		if (obj.getWhere() != null){
			hasWhere = true;
			visitNode(obj.getWhere());
		}
	}
	
	@Override
	public void visit(NamedTable obj) {
		super.visit(obj);
		tableName = obj.getName();
	}
	
	@Override
	public void visit(Comparison obj) {
		//check whether it's simple delete (itemName() = <value>)
		if (obj.getLeftExpression() instanceof ColumnReference){
			if (((ColumnReference)obj.getLeftExpression()).getName().equals("itemName()") && obj.getOperator() == Operator.EQ){
				isSimpleDelete = true;
				if (obj.getRightExpression() instanceof Literal){
					itemName = (String) ((Literal)obj.getRightExpression()).getValue();
				}else{
					//Could here be something else than literal?
//					throw new TranslatorException("Wrong DELETE Format");
				}
				super.visit(obj);
				return;
			}
		}else if (obj.getRightExpression() instanceof ColumnReference){
			if (((ColumnReference)obj.getRightExpression()).getName().equals("itemName()") && obj.getOperator() == Operator.EQ){
				isSimpleDelete = true;
				if (obj.getLeftExpression() instanceof Literal){
					itemName = (String) ((Literal)obj.getRightExpression()).getValue();
				}else{
					//Could here be something else than literal?
//					throw new TranslatorException("Wrong DELETE Format");
				}
				super.visit(obj);
				return;
			}
		}
		getItemNamesForCriteria(obj);
		//non trivial DELETE StatementgetItemNamesForCriteria(obj);
		super.visit(obj);
	}
	
	private void getItemNamesForCriteria(Comparison obj){
		ArrayList<String> columns = new ArrayList<String>();
		if (itemNames == null){
			itemNames = new ArrayList<String>();
		}
		columns.add("itemName()");
		List<List<String>> response = apiClass.performSelect("SELECT itemName() FROM "+tableName+" WHERE "+SimpleDBSQLVisitor.getSQLString(obj), columns);
		for (List<String> list : response) {
			itemNames.add(list.get(0));
		}
	}
}
