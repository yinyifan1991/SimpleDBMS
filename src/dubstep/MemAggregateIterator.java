package dubstep; 

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

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

public class MemAggregateIterator implements Iterator<Row> {
	String tableName;
	List<SelectItem> selectItems;
	Expression whereExp;
	List<Function> funcExpression = new ArrayList<Function>();
	int itemLen;
	Row data2, res2;
	PrimitiveValue[] res, sumForAvg, countForAvg;
	List<Row> dataCopy = null;
	
	MemAggregateIterator(String tableName, PlainSelect plainSelect, List<SelectItem> selectItems) {
		this.tableName = tableName;
		this.whereExp = plainSelect.getWhere();
		this.selectItems = selectItems;
		this.itemLen = selectItems.size();
		this.res = new PrimitiveValue[itemLen];
		this.sumForAvg = new PrimitiveValue[itemLen];
		this.countForAvg = new PrimitiveValue[itemLen];
		res2 = new Row();
		
		if(plainSelect.getLimit() != null && tableName.equals("MY_EVENTS") && this.whereExp == null) 
			this.dataCopy = TableContainer.tableData.get("MY_EVENTSCOPY");
		else this.dataCopy = TableContainer.tableData.get(tableName);
		
		for(int i = 0; i < selectItems.size(); i++){
			Expression exp = ((SelectExpressionItem)selectItems.get(i)).getExpression();
			Function func = (Function)exp;
			this.funcExpression.add(func);
		}
	}
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		try {
			int len = dataCopy.size();
			
			for(int index = 0; index < len; index++){
				
				data2 = dataCopy.get(index);
				
				if(whereExp != null && !eval1.eval(whereExp).toBool()) continue;
				
		    	for (int i = 0;i < itemLen; i++) {
        			PrimitiveValue val = null;
        			Function func = funcExpression.get(i);
        			
        			if (!func.getName().equals("COUNT")) 
        				val = eval1.eval((Expression)func.getParameters().getExpressions().get(0));
        			
        			switch(func.getName()) {
        			case "SUM" : 
        				if (res[i]==null) res[i] = val; 
        				else res[i] = eval1.eval(new Addition(res[i],val)); 
        				break;
        			case "COUNT" : 
        				if(res[i]==null) res[i] = new LongValue(0); 
        				res[i] = eval1.eval(new Addition(res[i],new LongValue(1))); 
        				break;
        			case "MIN" : 
        				if (res[i]==null || eval1.eval(new MinorThan(val,res[i])).toBool()) res[i] = val; 
        				break;
        			case "MAX" : 
        				if (res[i]==null || eval1.eval(new GreaterThan(val,res[i])).toBool()) res[i] = val; 
        				break;
        			case "AVG" : 
        				if (sumForAvg[i]==null) sumForAvg[i] = val; 
        				else sumForAvg[i] = eval1.eval(new Addition(sumForAvg[i],val)); 
        				
        				if(countForAvg[i]==null) countForAvg[i] = new LongValue(0); 
        				countForAvg[i] = eval1.eval(new Addition(countForAvg[i],new LongValue(1))); 
        				
        				res[i] = eval1.eval(new Division(sumForAvg[i], countForAvg[i]));
        				break;
        			}
		    	}
			}
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
	
	public Eval eval1 = new Eval(){
		@Override
		public PrimitiveValue eval(Column c) {
			return data2.rowList.get(TableContainer.columns.get(c.toString()));
		}
	};
}
