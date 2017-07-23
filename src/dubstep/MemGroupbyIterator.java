package dubstep;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class MemGroupbyIterator implements Iterator<HashMap<String, Row>> {
	String tableName;
	List<SelectItem> selectItems;
	List<Column> groupAttributes;
	Row data2;
	Expression whereExp;
	int groupNum;
	LinkedHashMap<String, PrimitiveValue[]> groupValue, sumForAvg, countForAvg;
	LinkedHashMap<String, Row> res;

	int count = 0;
	List<Row> dataCopy = null;
	
	MemGroupbyIterator(String tableName, PlainSelect plainSelect, List<SelectItem> selectItems) {
		this.tableName = tableName;
		this.whereExp = plainSelect.getWhere();	
		this.selectItems = selectItems;
		this.groupAttributes = plainSelect.getGroupByColumnReferences();
		this.groupNum = selectItems.size() - groupAttributes.size();						//all the select items except the group by attributes
		this.res = new LinkedHashMap<String, Row>();
		this.groupValue = new LinkedHashMap<String, PrimitiveValue[]>();
		this.sumForAvg = new LinkedHashMap<String, PrimitiveValue[]>();
		this.countForAvg = new LinkedHashMap<String, PrimitiveValue[]>();
		
		if(plainSelect.getLimit() != null && tableName.equals("MY_EVENTS") && this.whereExp == null) 
			this.dataCopy = TableContainer.tableData.get("MY_EVENTSCOPY");
		else this.dataCopy = TableContainer.tableData.get(tableName);
	}
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		try {
			int len = dataCopy.size();
			
			for(int index = 0; index < len; index++){
				
				data2 = dataCopy.get(index);
				
				if(whereExp != null && !eval1.eval(whereExp).toBool()) continue;
				
				String temp = "";
				
				for(Column c : groupAttributes){											//store the group by attributes in key as 'A|B|C'
					if(temp.length() == 0) temp += eval1.eval(c);
					else temp += "|" + eval1.eval(c);
				}
				
				if(!groupValue.containsKey(temp)) groupValue.put(temp, new PrimitiveValue[groupNum]);		//if it is a new key, put it into the Hashmap
				if(!sumForAvg.containsKey(temp)) sumForAvg.put(temp, new PrimitiveValue[groupNum]);
				if(!countForAvg.containsKey(temp)) countForAvg.put(temp, new PrimitiveValue[groupNum]);
				
		    	for (int i = 0;i < groupNum; i++) {
		    		
		    		Expression exp = ((SelectExpressionItem)selectItems.get(i+groupAttributes.size())).getExpression();
        			Function func = (Function)exp;
        			
        			PrimitiveValue val = null;
        			
        			if (!func.getName().toUpperCase().equals("COUNT")) val = eval1.eval((Expression)func.getParameters().getExpressions().get(0));
        			
        			switch(func.getName().toUpperCase()) {
        			case "SUM" : 
        				if (groupValue.get(temp)[i]==null) groupValue.get(temp)[i] = val; 
        				else groupValue.get(temp)[i] = eval1.eval(new Addition(groupValue.get(temp)[i],val)); 
        				break;
        			case "COUNT" : 
        				if (groupValue.get(temp)[i]==null) groupValue.get(temp)[i] = new LongValue(0); 
        				groupValue.get(temp)[i] = eval1.eval(new Addition(groupValue.get(temp)[i],new LongValue(1))); 
        				break;
        			case "MIN" : 
        				if (groupValue.get(temp)[i]==null || eval1.eval(new MinorThan(val,groupValue.get(temp)[i])).toBool()) groupValue.get(temp)[i] = val; 
        				break;
        			case "MAX" : 
        				if (groupValue.get(temp)[i]==null || eval1.eval(new GreaterThan(val,groupValue.get(temp)[i])).toBool()) groupValue.get(temp)[i] = val; 
        				break;
        			case "AVG" : 
        				if (sumForAvg.get(temp)[i]==null) sumForAvg.get(temp)[i] = val; 
        				else sumForAvg.get(temp)[i] = eval1.eval(new Addition(sumForAvg.get(temp)[i],val)); 
        				
        				if(countForAvg.get(temp)[i]==null) countForAvg.get(temp)[i] = new LongValue(0); 
        				countForAvg.get(temp)[i] = eval1.eval(new Addition(countForAvg.get(temp)[i],new LongValue(1))); 
        				
        				if(index == len - 1)
        					groupValue.get(temp)[i] = eval1.eval(new Division(sumForAvg.get(temp)[i], countForAvg.get(temp)[i]));				
        				break;
        			}
		    	}
			}			
			sumForAvg.clear();
			countForAvg.clear();

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
	
	public Eval eval1 = new Eval(){
		@Override
		public PrimitiveValue eval(Column c) {
			return data2.rowList.get(TableContainer.columns.get(c.toString()));
		}
	};
}