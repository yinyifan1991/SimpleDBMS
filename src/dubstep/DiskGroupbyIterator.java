package dubstep;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

public class DiskGroupbyIterator implements Iterator<HashMap<String, Row>> {
	BufferedReader buffer;
	String tableName, line;
	List<SelectItem> selectItems;
	List<Column> groupAttributes;
	final String[] data = new String[30];
	Expression whereExp;
	int groupNum;
	LinkedHashMap<String, PrimitiveValue[]> groupValue, sumForAvg, countForAvg;
	LinkedHashMap<String, Row> res;

	PrintWriter pw = null;
	int count = 0;
	List<Integer> orderByAttribute;
	boolean hasOrderBy;
	ArrayList<Integer> desc;
	
	DiskGroupbyIterator(String tableName, PlainSelect plainSelect, List<SelectItem> selectItems, boolean hasOrderBy, ArrayList<String> dates) throws FileNotFoundException, UnsupportedEncodingException{
		this.tableName = tableName;
		this.whereExp = plainSelect.getWhere();	
		this.selectItems = selectItems;
		this.groupAttributes = plainSelect.getGroupByColumnReferences();
		this.groupNum = selectItems.size() - groupAttributes.size();						//all the select items except the group by attributes
		this.groupValue = new LinkedHashMap<String, PrimitiveValue[]>();
		this.sumForAvg = new LinkedHashMap<String, PrimitiveValue[]>();
		this.countForAvg = new LinkedHashMap<String, PrimitiveValue[]>();
		this.desc = new ArrayList<Integer>();
		this.hasOrderBy = hasOrderBy;
		this.res = new LinkedHashMap<String, Row>();
		
		if(dates.size() == 2 && dates.get(0).contains("1998-07")) this.buffer = new BufferedReader(new FileReader("data/1999a.csv"));
		else if(dates.size() == 2) this.buffer = new BufferedReader(new FileReader("data/" + dates.get(0)+ ".csv"));
		else this.buffer = new BufferedReader(new FileReader(TableContainer.tables.get(tableName)));
		
		if(hasOrderBy){
			this.pw = new PrintWriter("data/sort.csv", "UTF-8");
			this.orderByAttribute = new ArrayList<Integer>();
			
			for(int i = 0; i < plainSelect.getOrderByElements().size(); i++){
				if(!plainSelect.getOrderByElements().get(i).isAsc()) this.desc.add(i);    		
        		String tempColName = plainSelect.getOrderByElements().get(i).getExpression().toString();
        		orderByAttribute.add(TableContainer.columns.get(tempColName));
			}
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
				
				String temp = "";
				
				for(Column c : groupAttributes){											//store the group by attributes in key as 'A|B|C'
					if(temp.length() == 0) temp += eval.eval(c);
					else temp += "|" + eval.eval(c);
				}
				
				if(!groupValue.containsKey(temp)) {
					groupValue.put(temp, new PrimitiveValue[groupNum]);		//if it is a new key, put it into the Hashmap
					if(this.hasOrderBy){
						String temp1 = "";
						for(int i = 0; i < orderByAttribute.size(); i ++){
		        			if(orderByAttribute.get(i) < tmp.length) 
		        				temp1 += tmp[orderByAttribute.get(i)] + "|";
			        	}
			        	temp1 += count;
			        	pw.write(temp1 + "\n");
			        	count++;
					}
				}
				if(!sumForAvg.containsKey(temp)) sumForAvg.put(temp, new PrimitiveValue[groupNum]);
				if(!countForAvg.containsKey(temp)) countForAvg.put(temp, new PrimitiveValue[groupNum]);
				
		    	for (int i = 0;i < groupNum; i++) {
		    		
		    		Expression exp = ((SelectExpressionItem)selectItems.get(i+groupAttributes.size())).getExpression();
        			Function func = (Function)exp;
        			PrimitiveValue val = null;
        			
        			if (!func.getName().toUpperCase().equals("COUNT")) val = eval.eval((Expression)func.getParameters().getExpressions().get(0));
        			
        			switch(func.getName().toUpperCase()) {
        			case "SUM" : 
        				if (groupValue.get(temp)[i]==null) groupValue.get(temp)[i] = val; 
        				else groupValue.get(temp)[i] = eval.eval(new Addition(groupValue.get(temp)[i],val)); 
        				break;
        			case "COUNT" : 
        				if(groupValue.get(temp)[i]==null) groupValue.get(temp)[i] = new LongValue(0); 
        				groupValue.get(temp)[i] = eval.eval(new Addition(groupValue.get(temp)[i],new LongValue(1))); 
        				break;
        			case "MIN" : 
        				if (groupValue.get(temp)[i]==null || eval.eval(new MinorThan(val,groupValue.get(temp)[i])).toBool()) groupValue.get(temp)[i] = val; 
        				break;
        			case "MAX" : 
        				if (groupValue.get(temp)[i]==null || eval.eval(new GreaterThan(val,groupValue.get(temp)[i])).toBool()) groupValue.get(temp)[i] = val; 
        				break;
        			case "AVG" : 
        				if (sumForAvg.get(temp)[i]==null) sumForAvg.get(temp)[i] = val; 
        				else sumForAvg.get(temp)[i] = eval.eval(new Addition(sumForAvg.get(temp)[i],val)); 
        				
        				if(countForAvg.get(temp)[i]==null) countForAvg.get(temp)[i] = new LongValue(0); 
        				countForAvg.get(temp)[i] = eval.eval(new Addition(countForAvg.get(temp)[i],new LongValue(1))); 
        				
        				groupValue.get(temp)[i] = eval.eval(new Division(sumForAvg.get(temp)[i], countForAvg.get(temp)[i]));				
        				break;
        			}
		    	}
			}
			
			sumForAvg.clear();
			countForAvg.clear();
			if(this.hasOrderBy) pw.close();
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
	public LinkedHashMap<String, Row> next(){
		// TODO Auto-generated method stub
		for(Map.Entry<String, PrimitiveValue[]> entry : groupValue.entrySet()){
			Row temp = new Row();
			temp.rowList = Arrays.asList(entry.getValue());
			res.put(entry.getKey(), temp);
		}
		return res;
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