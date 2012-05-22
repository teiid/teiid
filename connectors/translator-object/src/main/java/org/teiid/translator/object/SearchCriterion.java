package org.teiid.translator.object;

import org.teiid.metadata.Column;
import org.teiid.metadata.Table;


public class SearchCriterion {
	
	public enum Operator {
		
		EQUALS,
		IN,
		ALL  // no criteria, select all objects

	}

	private SearchCriterion addCondition;
	private boolean isAnd = false;
	
	private Operator operator;
	private String operatorString;
	private Column column;
	private Object value;
	private Class<?> type;
	private boolean isRootTableInSelect = false;
	
	public SearchCriterion() {
		this.operator = Operator.ALL;
	}
	
	
	public SearchCriterion(Column column, Object value, String operaterString, Class<?> type) {
		this.column = column;
		this.value = value;
		this.operatorString = operaterString;
		this.operator = Operator.EQUALS;
		this.type = type;
		
	}
	
	public SearchCriterion(Column column, Object value, String operaterString, Operator operator, Class<?> type) {
		this(column,  value, operaterString, type);
		this.operator = operator;
		
	}
	
	public Column getColumn() {
		return column;
	}


	public String getTableName() {
		Object p = column.getParent();
		if (p instanceof Table) {
			Table t = (Table)p;
			return t.getName();
		} else {
			// don't this would happen, but just in case at the moment
			assert(p.getClass().getName() != null);
		}
		
		return null;
	}

	public String getField() {
		return getNameInSourceFromColumn(this.column);
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public Operator getOperator() {
		return operator;
	}

	public void setOperator(Operator operator) {
		this.operator = operator;
	}
	
	public String getOperatorString() {
		return this.operatorString;
	}
	
	public void setOperatorString(String operatorString){
		this.operatorString = operatorString;

	}
	
	public Class<?> getType()
	{
		return this.type;
	}
	
	public void setType(Class<?> type) {
		this.type = type;
	}
	
	public void addAndCondition(SearchCriterion condition) {
		this.addCondition = condition;
		this.isAnd = true;
	}
	
	public void addOrCondition(SearchCriterion condition) {
		this.addCondition = condition;
		this.isAnd = false;		
	}
	
	public SearchCriterion getAddCondition() {
		return this.addCondition;
	}
	
	public boolean isAndCondition() {
		return this.isAnd;
	}

	public boolean isRootTableInSelect() {
		return isRootTableInSelect;
	}

	public void setRootTableInSelect(boolean isRootTableInSelect) {
		this.isRootTableInSelect = isRootTableInSelect;
	}
	
	private  String getNameInSourceFromColumn(Column c) {
		String name = c.getNameInSource();
		if(name == null || name.equals("")) {  //$NON-NLS-1$
			return c.getName();
		}
		return name;
	}  
	
	
//
//	public String getAuxValue() {
//		return auxValue;
//	}
//
//	public void setAuxValue(String auxValue) {
//		this.auxValue = auxValue;
//	}
//	
	

}
