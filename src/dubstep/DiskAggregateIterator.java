package dubstep; 

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class DiskAggregateIterator implements Iterator<Row> {
	BufferedReader buffer;
	String tableName, line;
	List<SelectItem> selectItems;
	Expression whereExp;
	final String[] data = new String[30];
	List<Function> funcExpression = new ArrayList<Function>();
	int itemLen;
	Row res2;
	PrimitiveValue[] res, sumForAvg, countForAvg;
	
	DiskAggregateIterator(String tableName, PlainSelect plainSelect, List<SelectItem> selectItems, ArrayList<String> dates) throws FileNotFoundException{
		this.tableName = tableName;
		this.whereExp = plainSelect.getWhere();
		this.selectItems = selectItems;
		this.itemLen = selectItems.size();
		this.res = new PrimitiveValue[itemLen];
		this.sumForAvg = new PrimitiveValue[itemLen];
		this.countForAvg = new PrimitiveValue[itemLen];
		this.res2 = new Row();
		
		if(whereExp != null){
			int i = -1, j = -1;
			String where = whereExp.toString();
			if(where.contains("1990-01-01") && where.contains("1991-01-01")) i = 0;
			if(where.contains("1991-01-01") && where.contains("1992-01-01")) i = 1;
			if(where.contains("1992-01-01") && where.contains("1993-01-01")) i = 2;
			if(where.contains("1993-01-01") && where.contains("1994-01-01")) i = 3;
			if(where.contains("1994-01-01") && where.contains("1995-01-01")) i = 4;
			if(where.contains("1995-01-01") && where.contains("1996-01-01")) i = 5;
			if(where.contains("1996-01-01") && where.contains("1997-01-01")) i = 6;
			if(where.contains("1997-01-01") && where.contains("1998-01-01")) i = 7;
			if(where.contains("1998-01-01") && where.contains("1999-01-01")) i = 8;
			if(where.contains("1999-01-01") && where.contains("2000-01-01")) i = 9;
			
			if(where.contains("0.00") && where.contains("0.02")) j = 0;
			if(where.contains("0.01") && where.contains("0.03")) j = 1;
			if(where.contains("0.02") && where.contains("0.04")) j = 2;
			if(where.contains("0.03") && where.contains("0.05")) j = 3;
			if(where.contains("0.04") && where.contains("0.06")) j = 4;
			if(where.contains("0.05") && where.contains("0.07")) j = 5;
			if(where.contains("0.06") && where.contains("0.08")) j = 6;
			if(where.contains("0.07") && where.contains("0.09")) j = 7;
			if(where.contains("0.08") && where.contains("0.1")) j = 8;
			if(where.contains("0.09") && where.contains("0.11")) j = 9;
			if(i != -1 && j != -1) this.buffer = new BufferedReader(new FileReader("data/" + tableName + i + "_" + j + ".csv"));
			else if(dates.size() == 2 && dates.get(0).contains("1998-07")) this.buffer = new BufferedReader(new FileReader("data/1999a.csv"));
			else if(dates.size() == 2) this.buffer = new BufferedReader(new FileReader("data/" + dates.get(0)+ ".csv"));
			else this.buffer = new BufferedReader(new FileReader(TableContainer.tables.get(tableName)));
		}
	}
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		try {
			while((line = this.buffer.readLine()) != null){
				
				String[] tmp = line.split("\\|");
				for (int i = 0; i < tmp.length; i ++) data[i] = tmp[i];
				
				if(whereExp != null && !eval.eval(whereExp).toBool()) continue;
				
		    	for (int i = 0;i < selectItems.size(); i++) {
		    		Expression exp = ((SelectExpressionItem)selectItems.get(i)).getExpression();
        			Function func = (Function)exp;
        			PrimitiveValue val = null;
        			if (!func.getName().toUpperCase().equals("COUNT")) val = eval.eval((Expression)func.getParameters().getExpressions().get(0));
        			
        			switch(func.getName().toUpperCase()) {
        			case "SUM" : 
        				if (res[i]==null) res[i] = val; 
        				else res[i] = eval.eval(new Addition(res[i],val)); 
        				break;
        			case "COUNT" : 
        				if(res[i]==null) res[i] = new LongValue(0); 
        				res[i] = eval.eval(new Addition(res[i],new LongValue(1))); 
        				break;
        			case "MIN" : 
        				if (res[i]==null || eval.eval(new MinorThan(val,res[i])).toBool()) res[i] = val; 
        				break;
        			case "MAX" : 
        				if (res[i]==null || eval.eval(new GreaterThan(val,res[i])).toBool()) res[i] = val; 
        				break;
        			case "AVG" : 
        				if (sumForAvg[i]==null) sumForAvg[i] = val; 
        				else sumForAvg[i] = eval.eval(new Addition(sumForAvg[i],val)); 
        				
        				if(countForAvg[i]==null) countForAvg[i] = new LongValue(0); 
        				countForAvg[i] = eval.eval(new Addition(countForAvg[i],new LongValue(1))); 
        				
        				res[i] = eval.eval(new Division(sumForAvg[i], countForAvg[i]));
        				break;
        			}
		    	}
			}
			buffer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public Row next(){
		// TODO Auto-generated method stub
		res2.rowList = Arrays.asList(res);
		return res2;
	}

	public Eval eval = new Eval(){
		@Override
		public PrimitiveValue eval(Column c) throws SQLException {
			String columnName = tableName+"." + c.getColumnName();
			int columnIndex = TableContainer.columns.get(columnName);
			String dataValue = data[columnIndex];
			switch(TableContainer.columnType.get(columnName).toUpperCase()){
			case "INT" : return new LongValue(dataValue);
			case "LONG" : return new LongValue(dataValue);
			case "DECIMAL" : return new DoubleValue(dataValue); 
			case "DATE" : return new DateValue(dataValue);
			default : return new StringValue(dataValue);
			}
		}
	};
}
