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
package org.teiid.translator.accumulo;

import java.math.BigDecimal;
import java.math.BigInteger;

import javax.resource.cci.ConnectionFactory;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.UpdateExecution;

@Translator(name="accumulo", description="Accumulo Translator, reads and writes the data to Accumulo Key/Value store")
public class AccumuloExecutionFactory extends ExecutionFactory<ConnectionFactory, AccumuloConnection> {
	private int queryThreadsCount = 10;
	
	public AccumuloExecutionFactory() {
		setSourceRequiredForMetadata(true);
	}
	
	@Override
	public void start() throws TranslatorException {
		super.start();
	}

	@TranslatorProperty(display="Execution Query Threads", description="Number of threads to use on Accumulo for Query", advanced=true)
	public int getQueryThreadsCount() {
		return queryThreadsCount;
	}

	public void setQueryThreadsCount(int queryThreadsCount) {
		this.queryThreadsCount = queryThreadsCount;
	}
	
	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			AccumuloConnection connection) throws TranslatorException {
		return new AccumuloQueryExecution(this, (Select) command, executionContext, metadata, connection);
	}
	
	@Override
	public UpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, AccumuloConnection connection) throws TranslatorException {
		return new AccumuloUpdateExecution(this, command, executionContext, metadata, connection);
	} 	
	
	@Override
	public void getMetadata(MetadataFactory metadataFactory, AccumuloConnection conn) throws TranslatorException {
		AccumuloMetadataProcessor processor = new AccumuloMetadataProcessor(metadataFactory, conn);
		processor.processMetadata();
	}
	
	@Override
	public boolean supportsAggregatesCountStar() {
		return true;
	}

	@Override
	public boolean supportsCompareCriteriaEquals() {
		return true;
	}
	
	@Override
	public boolean supportsNotCriteria() {
		return true;
	}

	@Override
	public boolean supportsCompareCriteriaOrdered() {
		return true;
	}

	@Override
	public boolean supportsInCriteria() {
		return true;
	}	
	
	@Override
	public boolean supportsOnlyLiteralComparison() {
		return true;
	}
	
	public Object retrieveValue(Value value, Class<?> expectedType) {
		if (value == null) {
			return null;
		}
		
		if (expectedType.isAssignableFrom(byte[].class)) {
			return value.get();
		}
		
		if (expectedType.isAssignableFrom(Integer.class)) {
			return new BigInteger(value.get()).intValue();
		}
		else if (expectedType.isAssignableFrom(Short.class)) {
			return new BigInteger(value.get()).shortValue();
		}
		else if (expectedType.isAssignableFrom(Byte.class)) {
			return new BigInteger(value.get()).byteValue();
		}
		else if (expectedType.isAssignableFrom(Double.class)) {
			return new BigInteger(value.get()).doubleValue();
		}
		else if (expectedType.isAssignableFrom(Float.class)) {
			return new BigInteger(value.get()).floatValue();
		}
		else if (expectedType.isAssignableFrom(String.class)) {
			return new String(value.get());
		}
		else if (expectedType.isAssignableFrom(Long.class)) {
			return new BigInteger(value.get()).longValue();
		}
		else if (expectedType.isAssignableFrom(Boolean.class)) {
			return new BigInteger(value.get()).byteValue() == 1;
		}
		else if (expectedType.isAssignableFrom(BigInteger.class)) {
			return new BigInteger(value.get());
		}
		else if (expectedType.isAssignableFrom(BigDecimal.class)) {
			return new BigDecimal(new BigInteger(value.get()));
		}
		else if (expectedType.isAssignableFrom(java.sql.Date.class)) {
			return new java.sql.Date(new BigInteger(value.get()).longValue());
		}
		else if (expectedType.isAssignableFrom(java.sql.Time.class)) {
			return new java.sql.Time(new BigInteger(value.get()).longValue());
		}
		else if (expectedType.isAssignableFrom(java.sql.Timestamp.class)) {
			return new java.sql.Timestamp(new BigInteger(value.get()).longValue());
		}
		//LOBs? is streaming provided by Accumulo?
		throw new RuntimeException("no lob support");
	}
	
	public Object retrieveValue(Text textvalue, Class<?> expectedType) {
		if (textvalue == null) {
			return null;
		}
		String value = new String(textvalue.getBytes());
		if (expectedType.isAssignableFrom(String.class)) {
			return value;
		}
		
		if (expectedType.isAssignableFrom(Integer.class)) {
			return Integer.valueOf(value);
		}
		else if (expectedType.isAssignableFrom(Short.class)) {
			return Short.valueOf(value);
		}
		else if (expectedType.isAssignableFrom(Byte.class)) {
			return Byte.valueOf(value);
		}
		else if (expectedType.isAssignableFrom(Double.class)) {
			return Double.valueOf(value);
		}
		else if (expectedType.isAssignableFrom(Float.class)) {
			return Float.valueOf(value);
		}
		else if (expectedType.isAssignableFrom(Long.class)) {
			return Long.valueOf(value);
		}
		else if (expectedType.isAssignableFrom(Boolean.class)) {
			return Boolean.valueOf(value);
		}
		else if (expectedType.isAssignableFrom(BigInteger.class)) {
			return new BigInteger(value);
		}
		else if (expectedType.isAssignableFrom(BigDecimal.class)) {
			return new BigDecimal(value);
		}
		else if (expectedType.isAssignableFrom(java.sql.Date.class)) {
			return new java.sql.Date(Long.valueOf(value));
		}
		else if (expectedType.isAssignableFrom(java.sql.Time.class)) {
			return new java.sql.Time(Long.valueOf(value));
		}
		else if (expectedType.isAssignableFrom(java.sql.Timestamp.class)) {
			return new java.sql.Timestamp(Long.valueOf(value));
		}
		//LOBs? is streaming provided by Accumulo?
		throw new RuntimeException("no lob support");
	}	
	
	public byte[] convertToAccumuloType(Object value, boolean charBinary) {
		if (value == null) {
			return null;
		}
		
		if (charBinary) {
			return value.toString().getBytes();
		}
		
		if (value instanceof String) {
			return ((String) value).getBytes();
		}
		else if (value instanceof Integer) {
			return BigInteger.valueOf((Integer)value).toByteArray();
		}
		else if (value instanceof Short) {
			return BigInteger.valueOf((Short)value).toByteArray();
		}
		else if (value instanceof Byte) {
			return BigInteger.valueOf((Byte)value).toByteArray();
		}
		else if (value instanceof Double) {
			return BigDecimal.valueOf((Double)value).toBigInteger().toByteArray();
		}
		else if (value instanceof Float) {
			return BigDecimal.valueOf((Float)value).toBigInteger().toByteArray();
		}
		else if (value instanceof Long) {
			return BigInteger.valueOf((Long)value).toByteArray();
		}
		else if (value instanceof Boolean) {
			return new byte[]{(byte) ((Boolean)value?1:0)};
		}
		else if (value instanceof BigInteger) {
			return ((BigInteger)value).toByteArray();
		}
		else if (value instanceof BigDecimal) {
			return ((BigDecimal)value).toBigInteger().toByteArray();
		}
		else if (value instanceof java.sql.Date) {
			return BigInteger.valueOf(((java.sql.Date)value).getTime()).toByteArray();
		}
		else if (value instanceof java.sql.Time) {
			return BigInteger.valueOf(((java.sql.Time)value).getTime()).toByteArray();
		}
		else if (value instanceof java.sql.Timestamp) {
			return BigInteger.valueOf(((java.sql.Timestamp)value).getTime()).toByteArray();
		}
		//LOBs? is streaming provided by Accumulo?
		throw new RuntimeException("no lob support");
	}	
}
