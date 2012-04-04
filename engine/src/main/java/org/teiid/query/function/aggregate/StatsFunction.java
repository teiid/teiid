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

package org.teiid.query.function.aggregate;

import java.util.List;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.sql.symbol.AggregateSymbol.Type;
import org.teiid.query.util.CommandContext;

public class StatsFunction extends SingleArgumentAggregateFunction {
	
	private double sum = 0;
	private double sumSq = 0;
	private int count = 0;
	private Type type;
	
	public StatsFunction(Type function) {
		this.type = function;
	}

	@Override
	public void reset() {
		sum = 0;
		sumSq = 0;
		count = 0;
	}
	
	@Override
	public void addInputDirect(Object input, List<?> tuple, CommandContext commandContext)
			throws FunctionExecutionException, ExpressionEvaluationException,
			TeiidComponentException {
		sum += ((Number)input).doubleValue();
		sumSq += Math.pow(((Number)input).doubleValue(), 2);
		count++;
	}
	
	@Override
	public Object getResult(CommandContext commandContext) throws FunctionExecutionException,
			ExpressionEvaluationException, TeiidComponentException {
		double result = 0;
		switch (type) {
		case STDDEV_POP:
		case VAR_POP:
			if (count == 0) {
				return null;
			}
			result = (sumSq - sum * sum / count) / count;
			if (type == Type.STDDEV_POP) {
				result = Math.sqrt(result);
			}
			break;
		case STDDEV_SAMP:
		case VAR_SAMP:
			if (count < 2) {
				return null;
			}
			result = (sumSq - sum * sum / count) / (count - 1);
			if (type == Type.STDDEV_SAMP) {
				result = Math.sqrt(result);
			}
			break;
		}
		return result;
	}

}
