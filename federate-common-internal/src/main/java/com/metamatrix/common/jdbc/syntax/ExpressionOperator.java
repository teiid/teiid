/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.common.jdbc.syntax;

import java.util.*;
import java.io.*;
import java.text.MessageFormat;


/**
 * ADVANCED: The expression operator is used internally to define SQL operations and functions.
 * It is possible for an advanced user to define their own operators.
 */

public class ExpressionOperator implements Serializable {
	protected List selectors;
	protected String[] databaseStrings;
  protected String expression;
	protected boolean isPrefix = false;
	protected boolean isRepeating = false;
	protected Class nodeClass;

	protected static Hashtable allOperators;
	protected static Hashtable inverses;

	/** Aggregate functions */
	public static final String Count = "count"; //$NON-NLS-1$
	public static final String Sum = "sum"; //$NON-NLS-1$
	public static final String Average = "average"; //$NON-NLS-1$
	public static final String Maximum = "maximum"; //$NON-NLS-1$
	public static final String Minimum = "minimum"; //$NON-NLS-1$

	/** Ordering functions */
	public static final String Ascending = "ascending"; //$NON-NLS-1$
	public static final String Descending = "descending"; //$NON-NLS-1$
	
	/** Field functions */
	public static final String ToUpperCase = "toUpperCase"; //$NON-NLS-1$
	public static final String ToLowerCase = "toLowerCase"; //$NON-NLS-1$

/*
 * ADVANCED: Create a new operator.
 */

public ExpressionOperator() {
	this.selectors = new ArrayList();
}
/*
 * ADVANCED: Create a new operator with the given name(s) and strings to print.
 */ 
public ExpressionOperator(List newSelectors, List newDatabaseStrings) {
	this.selectors = newSelectors;
	this.printsAs(newDatabaseStrings);
}
public static ExpressionOperator abs() {

	return simpleFunction("abs","ABS"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator acos() {

	return simpleFunction("acos","ACOS"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator addMonths() {

	return simpleTwoArgumentFunction("addMonths","ADD_MONTHS"); //$NON-NLS-1$ //$NON-NLS-2$
}
/*
 * ADVANCED: Add an operator to the global list of operators.
 */ 
public static void addOperator(ExpressionOperator op) {

  for (Iterator it = op.getSelectors().iterator(); it.hasNext(); ) {
//	for (Enumeration e = op.getSelectors().elements();e.hasMoreElements();) {
		allOperators.put(it.next(), op);
	}
}
/*
 * ADVANCED: Add a selector (a name) to this operator.
 */
public void addSelector(String selector) {
	selectors.add(selector);
}
/*
 * INTERNAL: Create the AND operator.
 */ 
public static ExpressionOperator and() {

	ExpressionOperator result = new ExpressionOperator();
	result.addSelector("&"); //$NON-NLS-1$
	result.printsAs("AND"); //$NON-NLS-1$
	result.bePostfix();
//	result.setNodeClass(LogicalExpression.class);
	return result;
}
/*
 * INTERNAL: Apply this to an object in memory. Not implemented yet.
 */
public Object applyTo(Object arg) {
	return arg;  // we can't do this yet
}
/*
 * INTERNAL: Create the ASCENDING operator.
 */ 
public static ExpressionOperator ascending() {

	ExpressionOperator result = new ExpressionOperator();
	result.addSelector(Ascending);
  result.setExpression(" {0} " + "ASC"); //$NON-NLS-1$ //$NON-NLS-2$
	List v = new ArrayList();
	v.add("ASC"); //$NON-NLS-1$
	result.printsAs(v);
	result.bePostfix();
//	result.setNodeClass(FunctionExpression.class);
	return result;
}
public static ExpressionOperator ascii() {

	return simpleFunction("ascii","ASCII"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator asin() {

	return simpleFunction("asin","ASIN"); //$NON-NLS-1$ //$NON-NLS-2$
}
private static void associateInverses(String one, String theOther) {

	ExpressionOperator opOne = getOperator(one);
	ExpressionOperator opTheOther = getOperator(theOther);
	inverses.put(opOne,opTheOther);
	inverses.put(opTheOther,opOne);
}
public static ExpressionOperator atan() {

	return simpleFunction("atan","ATAN"); //$NON-NLS-1$ //$NON-NLS-2$
}
/*
 * INTERNAL: Create the AVERAGE operator.
 */ 
public static ExpressionOperator average() {

  return simpleFunction(Average, "AVG"); //$NON-NLS-1$
/*

	ExpressionOperator result = new ExpressionOperator();
	result.addSelector(Averge);
  result.setExpression("AVG" + "( {0} )");
	List v = new ArrayList();
	v.add("AVG(");
	v.add(")");
	result.printsAs(v);
	result.bePrefix();
//	result.setNodeClass(FunctionExpression.class);
	return result;
*/
}
/*
 * ADVANCED: Tell the operator to be postfix, i.e. its strings start printing after
 * those of its first argument.
 */
public void bePostfix() {
	isPrefix = false;
}
/*
 * ADVANCED: Tell the operator to be pretfix, i.e. its strings start printing before
 * those of its first argument.
 */
public void bePrefix() {
	isPrefix = true;
}
/*
 * INTERNAL: Make this a repeating argument. Currently unused.
 */
public void beRepeating() {
	isRepeating = true;
}
/*
 * INTERNAL: Create the BETWEEN Operator
 */ 
public static ExpressionOperator between() {

	ExpressionOperator result = new ExpressionOperator();
	result.addSelector("between"); //$NON-NLS-1$
	List v = new ArrayList();
	v.add("BETWEEN "); //$NON-NLS-1$
	v.add(" AND "); //$NON-NLS-1$
	result.printsAs(v);
	result.bePostfix();
//	result.setNodeClass(FunctionExpression.class);
	return result;
}
/*
 * INTERNAL: Return a list of binary relation operator names.
 */ 
public static List binaryRelationOperators() {

	List v = new ArrayList();
	v.add("="); //$NON-NLS-1$
	v.add("<"); //$NON-NLS-1$
	v.add("<="); //$NON-NLS-1$
	v.add(">"); //$NON-NLS-1$
	v.add(">="); //$NON-NLS-1$
	v.add("like"); //$NON-NLS-1$
	v.add("notLike"); //$NON-NLS-1$
	
	v.add("+"); //$NON-NLS-1$
	v.add("-"); //$NON-NLS-1$
	v.add("/"); //$NON-NLS-1$
	v.add("*"); //$NON-NLS-1$
	return v;
		
}
public static ExpressionOperator ceil() {

	return simpleFunction("ceil","CEIL"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator chr() {
	return simpleFunction("chr","CHR"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator concat() {
	return simpleTwoArgumentFunction("concat","CONCAT"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator cos() {

	return simpleFunction("cos","COS"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator cosh() {

	return simpleFunction("cosh","COSH"); //$NON-NLS-1$ //$NON-NLS-2$
}
/*
 * INTERNAL: Create the COUNT operator.
 */ 
public static ExpressionOperator count() {

  return simpleFunction(Count, "COUNT"); //$NON-NLS-1$
/*
	ExpressionOperator result = new ExpressionOperator();
	result.addSelector(Count);
  result.setExpression("COUNT" + "( {0} )");
	List v = new ArrayList();
	v.add("COUNT(");
	v.add(")");
	result.printsAs(v);
	result.bePrefix();
 //	result.setNodeClass(FunctionExpression.class);
	return result;
 */
}
public static ExpressionOperator dateToString() {

	return simpleFunction("dateToString","TO_CHAR"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator deref() {

	return simpleFunction("deref","DEREF"); //$NON-NLS-1$ //$NON-NLS-2$
}
/*
 * INTERNAL: Create the DESCENDING operator.
 */ 
public static ExpressionOperator descending() {

	ExpressionOperator result = new ExpressionOperator();
	result.addSelector(Descending);
  result.setExpression(" {0} " + "DESC"); //$NON-NLS-1$ //$NON-NLS-2$
	List v = new ArrayList();
	v.add("DESC"); //$NON-NLS-1$
	result.printsAs(v);
	result.bePostfix();
//	result.setNodeClass(FunctionExpression.class);
	return result;
}
/*
 * INTERNAL: Initialize the outer join operator
 * Note: This is merely a shell which is incomplete, and
 * so will be replaced by the platform's operator when we
 * go to print. We need to create this here so that the expression
 * class is correct, normally it assumes functions for unknown operators.
 */

public static ExpressionOperator equalOuterJoin()
{
	ExpressionOperator result = new ExpressionOperator();
	result.addSelector("=*"); //$NON-NLS-1$
//	result.setNodeClass(RelationExpression.class);
	return result;
	
}
/*
 * PUBLIC: Test for equality
 */
public boolean equals(Object arg) {

	if (!(arg instanceof ExpressionOperator))
		return false;
	ExpressionOperator otherOperator = (ExpressionOperator)arg;
	if (getSelectors().size() == 0) return false;  //Just shouldn't happen
	if (otherOperator.getSelectors().size() == 0) return false; // Ditto
	return getSelectors().get(0).equals(otherOperator.getSelectors().get(0));
}
public static ExpressionOperator exp() {

	return simpleFunction("exp","EXP"); //$NON-NLS-1$ //$NON-NLS-2$
}


public static ExpressionOperator floor() {

	return simpleFunction("floor","FLOOR"); //$NON-NLS-1$ //$NON-NLS-2$
}
/*
 * PUBLIC: Return the hashtable of all operators.
 */ 
public static Hashtable getAllOperators() {

	if (allOperators == null) {
		initializeOperators();
	}
	return allOperators;
}
/*
 * INTERNAL: Slow. Avoid using.
 */

public List getDatabaseStrings() {
	List result = new ArrayList(databaseStrings.length);
	for (int i=0;i<databaseStrings.length;i++) {
		result.add(databaseStrings[i]);
	}

	return result;
}
/*
 * INTERNAL: Return the inverse of a given operator, if applicable
 * e.g. for isNull return notNull. Used in the implementation of the NOT operator,
 * since it's not always enough to just add a NOT in the SQL.
 */ 
public static ExpressionOperator getInverse(ExpressionOperator operator) {

	return (ExpressionOperator)getInverses().get(operator);
}
/*
 * INTERNAL: Return the hashtable mapping operators to their inverses.
 */ 
public static Hashtable getInverses() {

	if (inverses == null) {
		initializeInverses();
	}
	return inverses;
}
/*
 * INTERNAL: 
 */
public Class getNodeClass() {
	return nodeClass;
}
/*
 * INTERNAL: Lookup the operator with the given name.
 */ 
public static ExpressionOperator getOperator(String name) {

	return (ExpressionOperator) getAllOperators().get(name);
}
/*
 * INTERNAL:
 */
public List getSelectors() {
	return selectors;
}
public static ExpressionOperator greatest() {

	return simpleTwoArgumentFunction("greatest","GREATEST"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator hexToRaw() {

	return simpleFunction("hexToRaw","HEXTORAW"); //$NON-NLS-1$ //$NON-NLS-2$
}
/*
 * INTERNAL: Create the IN operator.
 */ 
public static ExpressionOperator in() {

	ExpressionOperator result = new ExpressionOperator();
	result.addSelector("in"); //$NON-NLS-1$
	result.printsAs("IN"); //$NON-NLS-1$
//	result.setNodeClass(RelationExpression.class);
	return result;
}
public static ExpressionOperator initcap() {
	return simpleFunction("initcap","INITCAP"); //$NON-NLS-1$ //$NON-NLS-2$
}
/*
 * INTERNAL:
 */ 
protected static void initializeAggregateFunctionOperators() {

	addOperator(count());
	addOperator(sum());
	addOperator(average());
	addOperator(minimum());
	addOperator(maximum());
	addOperator(variance());
	addOperator(standardDeviation());
}
/*
 * INTERNAL:
 */ 
protected static void initializeFunctionOperators() {

	addOperator(notOperator());
}
private static void initializeInverses() {

	inverses = new Hashtable();
	associateInverses("=","!="); //$NON-NLS-1$ //$NON-NLS-2$
	associateInverses("<",">="); //$NON-NLS-1$ //$NON-NLS-2$
	associateInverses("<=",">"); //$NON-NLS-1$ //$NON-NLS-2$
	associateInverses("like","notLike"); //$NON-NLS-1$ //$NON-NLS-2$
	associateInverses("isNull","notNull"); //$NON-NLS-1$ //$NON-NLS-2$
}
/*
 * INTERNAL: 
 */ 
protected static void initializeLogicalOperators() {
	addOperator(and());
	addOperator(or());
	addOperator(isNull());
	addOperator(notNull());

}
/*
 * INTERNAL:
 */ 
public static Hashtable initializeOperators() {

	resetOperators();
	initializeFunctionOperators();
	initializeRelationOperators();
	initializeLogicalOperators();
	initializeAggregateFunctionOperators();
	return allOperators;
}
/*
 * INTERNAL: 
 */ 

protected static void initializeRelationOperators() {

  for (Iterator it = binaryRelationOperators().iterator(); it.hasNext(); ) {
		String name = (String)it.next();
		addOperator(simpleRelation(name));
	}
	addOperator(in());
	addOperator(notIn());
	addOperator(like());
	addOperator(notLike());
	addOperator(notEqual());
	addOperator(equalOuterJoin());
	addOperator(between());
		
}
public static ExpressionOperator instring() {

	return simpleTwoArgumentFunction("instring","INSTRING"); //$NON-NLS-1$ //$NON-NLS-2$
}
/*
 * INTERNAL: If we have all the required information, this operator is complete
 * and can be used as is. Otherwise we will need to look up a platform-
 * specific operator.
 */

public boolean isComplete() {
	return databaseStrings != null && databaseStrings.length != 0;
}
/*
 * INTERNAL: Create the ISNULL operator.
 */ 
public static ExpressionOperator isNull() {

	ExpressionOperator result = new ExpressionOperator();
	result.addSelector("isNull"); //$NON-NLS-1$
	result.printsAs("IS NULL"); //$NON-NLS-1$
	result.bePostfix();
//	result.setNodeClass(LogicalExpression.class);
	return result;
}
/*
 * ADVANCED: Return true if this is a prefix operator.
 */
public boolean isPrefix() {
	return isPrefix;
}
public static ExpressionOperator lastDay() {

	return simpleFunction("lastDay","LAST_DAY"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator least() {

	return simpleTwoArgumentFunction("least","LEAST"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator leftPad() {
	return simpleThreeArgumentFunction("leftPad","LPAD"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator leftTrim() {
	return simpleTwoArgumentFunction("leftTrim","LTRIM"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator length() {

	return simpleFunction("length","LENGTH"); //$NON-NLS-1$ //$NON-NLS-2$
}
/*
 * INTERNAL: Create the LIKE operator.
 */ 
public static ExpressionOperator like() {

	ExpressionOperator result = new ExpressionOperator();
	result.addSelector("like"); //$NON-NLS-1$
	result.printsAs("LIKE"); //$NON-NLS-1$
	result.bePostfix();
//	result.setNodeClass(RelationExpression.class);
	return result;
}
public static ExpressionOperator ln() {

	return simpleFunction("ln","LN"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator log() {
	return simpleFunction("log","LOG"); //$NON-NLS-1$ //$NON-NLS-2$
}
/*
 * INTERNAL: Create the MAXIMUM operator.
 */ 
public static ExpressionOperator maximum() {

  return simpleFunction(Maximum, "MAX"); //$NON-NLS-1$

/*
	ExpressionOperator result = new ExpressionOperator();
	result.addSelector(Maximum);
  result.setExpression("MAX" + "( {0} )");
	List v = new ArrayList();
	v.add("MAX(");
	v.add(")");
	result.printsAs(v);
	result.bePrefix();
//	result.setNodeClass(FunctionExpression.class);
	return result;
  */
}
/*
 * INTERNAL: Create the MINIMUM operator.
 */ 
public static ExpressionOperator minimum() {

  return simpleFunction(Minimum, "MIN"); //$NON-NLS-1$

/*
	ExpressionOperator result = new ExpressionOperator();
	result.addSelector(Minimum);
  result.setExpression("MIN" + "( {0} )");
	List v = new ArrayList();
	v.add("MIN(");
	v.add(")");
	result.printsAs(v);
	result.bePrefix();
//	result.setNodeClass(FunctionExpression.class);
	return result;
*/
}
public static ExpressionOperator mod() {
	return simpleTwoArgumentFunction("mod","MOD"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator monthsBetween() {

	return simpleTwoArgumentFunction("monthsBetween","MONTHS_BETWEEN"); //$NON-NLS-1$ //$NON-NLS-2$
}


public static ExpressionOperator nextDay() {

	return simpleTwoArgumentFunction("nextDay","NEXT_DAY"); //$NON-NLS-1$ //$NON-NLS-2$
}
/*
 * INTERNAL: Return the opposite of this operator
 */ 

public ExpressionOperator not() {
	return getInverse(this);
}
/*
 * INTERNAL: Create the NOTEQUAL operator.
 */ 
public static ExpressionOperator notEqual() {

	ExpressionOperator result = new ExpressionOperator();
	result.addSelector("!="); //$NON-NLS-1$
	result.printsAs("<>"); //$NON-NLS-1$
	result.bePostfix();
//	result.setNodeClass(RelationExpression.class);
	return result;
}
/*
 * INTERNAL: Create the NOTIN operator.
 */ 
public static ExpressionOperator notIn() {

	ExpressionOperator result = new ExpressionOperator();
	result.addSelector("notIn"); //$NON-NLS-1$
	result.printsAs("NOT IN"); //$NON-NLS-1$
//	result.setNodeClass(RelationExpression.class);
	return result;
}
/*
 * INTERNAL: Create the NOTLIKE operator.
 */ 
public static ExpressionOperator notLike() {

	ExpressionOperator result = new ExpressionOperator();
	result.addSelector("notLike"); //$NON-NLS-1$
	result.printsAs("NOT LIKE"); //$NON-NLS-1$
	result.bePostfix();
//	result.setNodeClass(RelationExpression.class);
	return result;
}
/*
 * INTERNAL: Create the NOTNULL operator.
 */ 
public static ExpressionOperator notNull() {

	ExpressionOperator result = new ExpressionOperator();
	result.addSelector("notNull"); //$NON-NLS-1$
	result.printsAs("IS NOT NULL"); //$NON-NLS-1$
	result.bePostfix();
//	result.setNodeClass(LogicalExpression.class);
	return result;
}
/*
 * INTERNAL: Create the NOT operator.
 */ 
public static ExpressionOperator notOperator() {

	ExpressionOperator result = new ExpressionOperator();
	result.addSelector("not"); //$NON-NLS-1$
	List v = new ArrayList();
	v.add("NOT ("); //$NON-NLS-1$
	v.add(")"); //$NON-NLS-1$
	result.printsAs(v);
	result.bePrefix();
//	result.setNodeClass(FunctionExpression.class);
	return result;
}
/*
 * INTERNAL: Create the OR operator.
 */ 
public static ExpressionOperator or() {

	ExpressionOperator result = new ExpressionOperator();
	result.addSelector("|"); //$NON-NLS-1$
	result.printsAs("OR"); //$NON-NLS-1$
	result.bePostfix();
//	result.setNodeClass(LogicalExpression.class);
	return result;
}
public static ExpressionOperator power() {
	return simpleTwoArgumentFunction("power","POWER"); //$NON-NLS-1$ //$NON-NLS-2$
}

public String getExpression() {
    return this.expression;
}

public void setExpression(String exp) {
    this.expression = exp;
}
/*
 * ADVANCED: Set the single string for this operator.
 */ 

public void printsAs(String s) {
	List v = new ArrayList(1);
	v.add(s);
	printsAs(v);
}
/*
 * ADVANCED: Set the strings for this operator.
 */ 
public void printsAs(List dbStrings) {
	databaseStrings = new String[dbStrings.size()];
  int i = 0;
  for (Iterator it = dbStrings.iterator(); it.hasNext(); i++) {
        databaseStrings[i] = (String)it.next();

  }

}
public static ExpressionOperator ref() {

	return simpleFunction("ref","REF"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator refToHex() {

	return simpleFunction("refToHex","REFTOHEX"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator refToValue() {

	return simpleFunction("refToValue","VALUE"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator replace() {
	return simpleThreeArgumentFunction("replace","REPLACE"); //$NON-NLS-1$ //$NON-NLS-2$
}
/*
 * INTERNAL: Test if this operator instance represents a comparison to null, which
 * we have to print specially.  Also special-cased for performance.
 */ 
 
public boolean representsEqualToNull(Object singleArgument) {
	if (singleArgument instanceof List)
		return representsEqualToNull((List)singleArgument);
	return (selectors.get(0).equals("=") && (singleArgument == null)); //$NON-NLS-1$
}
/*
 * INTERNAL: Test if this operator instance represents a comparison to null, which
 * we have to print specially.
 */ 
public boolean representsEqualToNull(List arguments) {
	return (selectors.get(0).equals("=") && (arguments.size() == 1) && (arguments.get(0) == null)); //$NON-NLS-1$
}
/*
 * INTERNAL: Test if this operator instance represents a comparison to null, which
 * we have to print specially.  Also special-cased for performance.
 */ 

public boolean representsNotEqualToNull(Object singleArgument) {
	if (singleArgument instanceof List)
		return representsNotEqualToNull((List)singleArgument);
	return (selectors.get(0).equals("!=") && (singleArgument == null)); //$NON-NLS-1$
}
/*
 * INTERNAL: Test if this operator instance represents a comparison to null, which
 * we have to print specially.
 */ 
public boolean representsNotEqualToNull(List arguments) {
	return (selectors.get(0).equals("!=") && (arguments.size() == 1) && (arguments.get(0) == null)); //$NON-NLS-1$
}
/*
 * INTERNAL: Reset all the operators.
 */
public static void resetOperators() {

	allOperators = new Hashtable();
}
public static ExpressionOperator rightPad() {

	return simpleThreeArgumentFunction("rightPad","RPAD"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator rightTrim() {

	return simpleTwoArgumentFunction("rightTrim","RTRIM"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator round() {
	return simpleTwoArgumentFunction("round","ROUND"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator roundDate() {

	return simpleTwoArgumentFunction("roundDate","ROUND"); //$NON-NLS-1$ //$NON-NLS-2$
}
/*
 * ADVANCED: Set the node class for this operator. For user-defined functions this is
 * normally FunctionExpression.class.
 */ 
public void setNodeClass(Class aClass) {
	nodeClass = aClass;
}
public static ExpressionOperator sign() {
	return simpleFunction("sign","SIGN"); //$NON-NLS-1$ //$NON-NLS-2$
}

public String buildExpression(String value) {

    String cmd = MessageFormat.format(getExpression(), new Object[] {value} );
    return cmd;
}

/*
 * ADVANCED: Create an operator for a simple function whose Java name
 * is the same as its database name.
 */ 
public static ExpressionOperator simpleFunction(String name) {

	return simpleFunction(name, name);
}
/*
 * ADVANCED: Create an operator for a simple function given a Java name and a single
 * String for the database (parentheses will be added automatically).
 */ 
public static ExpressionOperator simpleFunction(String selector, String dbString) {

	ExpressionOperator result = new ExpressionOperator();
	result.addSelector(selector);
  result.setExpression(dbString + "( {0} )"); //$NON-NLS-1$
	List v = new ArrayList();
	v.add(dbString + "("); //$NON-NLS-1$
	v.add(")"); //$NON-NLS-1$
	result.printsAs(v);
	result.bePrefix();
//	result.setNodeClass(FunctionExpression.class);
	return result;
}
/*
 * INTERNAL: Create an operator for a simple relation given a Java name and a single
 * String for the database (parentheses will be added automatically).
 */ 
public static ExpressionOperator simpleRelation(String name) {
	ExpressionOperator op = new ExpressionOperator();
	op.addSelector(name);
	op.printsAs(name);
	op.bePostfix();
//	op.setNodeClass(RelationExpression.class);
	return op;
}
public static ExpressionOperator simpleThreeArgumentFunction(String selector, String dbString) {

	ExpressionOperator result = new ExpressionOperator();
	result.addSelector(selector);
	List v = new ArrayList();
	v.add(dbString + "("); //$NON-NLS-1$
	v.add(","); //$NON-NLS-1$
	v.add(","); //$NON-NLS-1$
	v.add(")"); //$NON-NLS-1$
	result.printsAs(v);
	result.bePrefix();
//	result.setNodeClass(FunctionExpression.class);
	return result;
}
public static ExpressionOperator simpleTwoArgumentFunction(String selector, String dbString) {

	ExpressionOperator result = new ExpressionOperator();
	result.addSelector(selector);
	List v = new ArrayList();
	v.add(dbString + "("); //$NON-NLS-1$
	v.add(","); //$NON-NLS-1$
	v.add(")"); //$NON-NLS-1$
	result.printsAs(v);
	result.bePrefix();
//	result.setNodeClass(FunctionExpression.class);
	return result;
}
public static ExpressionOperator sin() {
	return simpleFunction("sin","SIN"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator sinh() {
	return simpleFunction("sinh","SINH"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator soundex() {

	return simpleFunction("soundex","SOUNDEX"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator sqrt() {
	return simpleFunction("sqrt","SQRT"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator standardDeviation() {

	return simpleFunction("standardDeviation","STDDEV"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator substring() {

	return simpleThreeArgumentFunction("substring","SUBSTR"); //$NON-NLS-1$ //$NON-NLS-2$
}
/*
 * INTERNAL: Create the SUM operator.
 */
public static ExpressionOperator sum() {

  return simpleFunction(Sum, "Sum"); //$NON-NLS-1$
/*

	ExpressionOperator result = new ExpressionOperator();
	result.addSelector(Sum);
	List v = new ArrayList();
	v.add("SUM(");
	v.add(")");
	result.printsAs(v);
	result.bePrefix();
//	result.setNodeClass(FunctionExpression.class);
	return result;
*/
}
public static ExpressionOperator tan() {
	return simpleFunction("tan","TAN"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator tanh() {
	return simpleFunction("tanh","TANH"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator toDate() {

	return simpleFunction("toDate","TO_DATE"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator today() {

	return simpleFunction("today","SYSDATE"); //$NON-NLS-1$ //$NON-NLS-2$
}
/*
 * INTERNAL: Create the toLowerCase operator.
 */
public static ExpressionOperator toLowerCase() {

	return simpleFunction("toLowerCase","LOWER"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator toNumber() {

	return simpleFunction("toNumber","TO_NUMBER"); //$NON-NLS-1$ //$NON-NLS-2$
}
/*
 * PUBLIC: Print a debug representation of this operator.
 */ 
public String toString() {
	if (selectors == null || (selectors.size() == 0))
		return "unknown operator"; //$NON-NLS-1$
	return "operator " + selectors.get(0); //$NON-NLS-1$
}
/*
 * INTERNAL: Create the TOUPPERCASE operator.
 */
public static ExpressionOperator toUpperCase() {

	return simpleFunction("toUpperCase","UCASE"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator translate() {

	return simpleThreeArgumentFunction("translate","TRANSLATE"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator trim() {

	return simpleTwoArgumentFunction("trim","TRIM"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator trunc() {
	return simpleTwoArgumentFunction("trunc","TRUNC"); //$NON-NLS-1$ //$NON-NLS-2$
}
public static ExpressionOperator variance() {

	return simpleFunction("variance","VARIANCE"); //$NON-NLS-1$ //$NON-NLS-2$
}
/**
 * Called from FunctionExpression.writeField(StringWriter, String) so that function strings can
 * also be added.
 */
protected void writeField(StringWriter writer, String fieldName) {
	if (isPrefix()) {
		writer.write(databaseStrings[0]);
		writer.write(fieldName);
	} else { // Are non prefix functions supported or even possible ???
		writer.write(fieldName);
	}
	if (databaseStrings.length > 1) {
		writer.write(databaseStrings[1]);
	}
}
}
