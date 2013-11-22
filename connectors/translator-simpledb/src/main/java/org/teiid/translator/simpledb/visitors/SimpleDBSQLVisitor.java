package org.teiid.translator.simpledb.visitors;

import static org.teiid.language.SQLConstants.Reserved.FROM;
import static org.teiid.language.SQLConstants.Reserved.LIKE;
import static org.teiid.language.SQLConstants.Reserved.LIMIT;
import static org.teiid.language.SQLConstants.Reserved.SELECT;
import static org.teiid.language.SQLConstants.Reserved.WHERE;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.DerivedColumn;
import org.teiid.language.LanguageObject;
import org.teiid.language.Like;
import org.teiid.language.Limit;
import org.teiid.language.Not;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.Select;
import org.teiid.language.visitor.SQLStringVisitor;

public class SimpleDBSQLVisitor extends SQLStringVisitor {
	
	
	@Override
	public void visit(Select obj) {
		buffer.append(SELECT).append(Tokens.SPACE);
		if (obj.getDerivedColumns().size()>1){
			List<DerivedColumn> columnsList = new ArrayList<DerivedColumn>();
			for (DerivedColumn column : obj.getDerivedColumns()) {
				ColumnReference ref = (ColumnReference) column.getExpression();
				if (!ref.getName().equals("itemName()")){
					columnsList.add(column);
				}
			}
			append(columnsList);
		}else{
			append(obj.getDerivedColumns());
		}
		buffer.append(Tokens.SPACE);
		if (obj.getFrom() != null && !obj.getFrom().isEmpty()){
			buffer.append(FROM).append(Tokens.SPACE);
			append(obj.getFrom());
			buffer.append(Tokens.SPACE);
		}
		if (obj.getWhere() != null){
			buffer.append(WHERE).append(Tokens.SPACE);
			append(obj.getWhere());
		}
		if (obj.getLimit() != null){
			append(obj.getLimit());
		}
	}
	
	
	@Override
	public void visit(ColumnReference obj) {
		buffer.append(obj.getName());
	}
	
	public static String getSQLString(LanguageObject obj){
		SimpleDBSQLVisitor visitor = new SimpleDBSQLVisitor();
		visitor.append(obj);
		return visitor.toString();
	}
	
	@Override
	public void visit(Limit obj) {
		if (obj != null){
			buffer.append(LIMIT).append(Tokens.SPACE).append(obj.getRowLimit());
		}
	}
	
	@Override
	public void visit(Like obj) {
		if (obj != null){
			if (obj.getLeftExpression() instanceof ColumnReference){
				ColumnReference cr = (ColumnReference) obj.getLeftExpression();
				buffer.append(cr.getName()).append(Tokens.SPACE);
			}
			buffer.append(LIKE).append(Tokens.SPACE).append(obj.getRightExpression()).append(Tokens.SPACE);
		}
	}
	
	@Override
	public void visit(Comparison obj) {
		if (obj.getOperator().equals(Operator.NE)){
			Comparison c = new Comparison(obj.getLeftExpression(), obj.getRightExpression(), Operator.EQ);
			append(new Not(c));
		}else{
			if (obj.getLeftExpression() instanceof ColumnReference){
				ColumnReference left = (ColumnReference) obj.getLeftExpression();
				buffer.append(left.getName());
			}else{
				buffer.append(obj.getLeftExpression().toString());
			}
			buffer.append(Tokens.SPACE).append(obj.getOperator().toString()).append(Tokens.SPACE);
			if(obj.getRightExpression() instanceof ColumnReference){
				ColumnReference right = (ColumnReference) obj.getRightExpression();
				buffer.append(right.getName());
			}else{
				buffer.append(obj.getRightExpression().toString());
			}
			buffer.append(Tokens.SPACE);
		}
	}
}
