package dubstep;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MemSelectIterator implements Iterator<Row> {

	String tableName, outerTableName;
	Expression whereExp;
	List<SelectItem> selectItems;
	Row res, data2;
	boolean hasOrderBy, flag2 = false;
	
	int index = 0, itemLen;
	List<Expression> selectItemsExpression = new ArrayList<Expression>();
	List<Row> dataCopy = null;
	
	MemSelectIterator(String tableName, String outerTableName, PlainSelect plainSelect, List<SelectItem> selectItems) {
		this.tableName = tableName;
		this.outerTableName = outerTableName;
		this.whereExp = plainSelect.getWhere();
		this.selectItems = selectItems;
		this.itemLen = selectItems.size();
		
		if(plainSelect.getLimit() != null && tableName.equals("MY_EVENTS") && this.whereExp == null) 
			this.dataCopy = TableContainer.tableData.get("MY_EVENTSCOPY");
		else this.dataCopy = TableContainer.tableData.get(tableName);
		
		for(int i = 0; i < selectItems.size(); i++)
			if(!selectItems.get(i).toString().equals("*")) 
				this.selectItemsExpression.add(((SelectExpressionItem) selectItems.get(i)).getExpression());
			
		if(selectItems.get(0).toString().equals("*")) this.flag2 = true;

	}
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		this.res = new Row();
		try {
			while(index < dataCopy.size()){
				data2 = dataCopy.get(index++);							
				if(whereExp != null && !eval1.eval(whereExp).toBool()) continue;	
							
				/*if the query is SELECT * FROM R*/
				if(flag2){
					res = data2;
				}else{
					/*if it is the regular query*/
					for (int i = 0; i < itemLen; i++) res.rowList.add(eval1.eval((selectItemsExpression.get(i))));
				}
				return true;
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
		return res;
	}
	
	public Eval eval1 = new Eval(){
		@Override
		public PrimitiveValue eval(Column c) {
			return data2.rowList.get(TableContainer.columns.get(c.toString()));
		}
	};
}
