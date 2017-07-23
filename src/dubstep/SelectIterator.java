package dubstep;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SelectIterator implements Iterator<Row> {

	BufferedReader buffer;
	String tableName, outerTableName, line;
	Expression whereExp;
	final String[] data = new String[30];
	List<SelectItem> selectItems;
	List<String> attributeColumns;
	ArrayList<Integer> desc;
	Row res, data2;
	boolean generateTable, isNested, hasOrderBy, flag = false, flag2 = false;
	
	PrintWriter pw = null;
	int count = 0, index = 0, itemLen;
	List<Integer> orderByAttribute;
	ArrayList<Integer> desc2;
	List<Expression> selectItemsExpression = new ArrayList<Expression>();
	List<Row> dataCopy = null;
	
	SelectIterator(String tableName, String outerTableName, PlainSelect plainSelect, List<SelectItem> selectItems, boolean isNested, boolean hasOrderBy, ArrayList<String> dates) throws FileNotFoundException, UnsupportedEncodingException {
		this.tableName = tableName;
		this.outerTableName = outerTableName;
		this.whereExp = plainSelect.getWhere();
		this.selectItems = selectItems;
		this.attributeColumns = TableContainer.tableAttributes.get(tableName);
		this.generateTable = false;
		this.isNested = isNested;
		this.desc = new ArrayList<>();
		this.hasOrderBy = hasOrderBy;
		this.desc2 = new ArrayList<>();
		this.flag = TableContainer.tableData.containsKey(tableName);
		this.itemLen = selectItems.size();
		if(plainSelect.getLimit() != null && tableName.equals("MY_EVENTS")) this.dataCopy = TableContainer.tableData.get("MY_EVENTSCOPY");
		else this.dataCopy = TableContainer.tableData.get(tableName);
		
		for(int i = 0; i < selectItems.size(); i++)
			if(!selectItems.get(i).toString().equals("*")) 
				this.selectItemsExpression.add(((SelectExpressionItem) selectItems.get(i)).getExpression());
			
		if(selectItems.get(0).toString().equals("*")) this.flag2 = true;
		
		if(!flag){
			if(dates.size() == 2 && (dates.get(0).contains("1998-07"))) this.buffer = new BufferedReader(new FileReader("data/1999a.csv"));
			else if(dates.size() == 2) this.buffer = new BufferedReader(new FileReader("data/" + dates.get(0)+ ".csv"));
			else this.buffer = new BufferedReader(new FileReader(TableContainer.tables.get(tableName)));
		}
		
		if(hasOrderBy){
			if(!flag) this.pw = new PrintWriter("data/sort.csv", "UTF-8");
			
			this.orderByAttribute = new ArrayList<>();			
			for(int i = 0; i < plainSelect.getOrderByElements().size(); i++){
				String colName = plainSelect.getOrderByElements().get(i).getExpression().toString();
				if(!plainSelect.getOrderByElements().get(i).isAsc()) {
					this.desc.add(i);
					this.desc2.add(TableContainer.columns.get(colName));
				}
        		orderByAttribute.add(TableContainer.columns.get(colName));
			}		
		}
	}
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		this.res = new Row();
		try {
			if(!flag){
				while((line = this.buffer.readLine()) != null){
					String[] tmp = line.split("\\|");
					for (int i = 0; i < tmp.length; i ++) data[i] = tmp[i];
					
					if(whereExp != null && !eval.eval(whereExp).toBool()) continue;
					
					if(this.hasOrderBy){
						String temp = "";
						for(int i = 0; i < orderByAttribute.size(); i ++){
							int index = orderByAttribute.get(i);
		        			if( index < tmp.length) temp += tmp[index] + "|";
			        	}
			        	temp += count;
			        	pw.write(temp + "\n");
			        	count++;
					}
					
					/*if the query is SELECT * FROM R*/
					if(selectItems.get(0).toString().equals("*")){
						for (int i = 0;i < attributeColumns.size(); i++){
							String cur = attributeColumns.get(i);
							switch(TableContainer.columnType.get(cur).toUpperCase()){
							case "INT" : res.rowList.add(new LongValue(tmp[i])); break;
							case "LONG" : res.rowList.add(new LongValue(tmp[i])); break;
							case "DECIMAL" : res.rowList.add(new DoubleValue(tmp[i])); break;
							case "DATE" : res.rowList.add(new DateValue(tmp[i])); break;
							default : res.rowList.add(new StringValue(tmp[i]));
							}
						}
					}else{
						/*if it is the regular query*/
						for (int i = 0;i < selectItems.size(); i++) {
							res.rowList.add(eval.eval(((SelectExpressionItem)selectItems.get(i)).getExpression()));
							if(!generateTable && ((SelectExpressionItem)selectItems.get(i)).getAlias() != null){
								String columnName = outerTableName+"."+((SelectExpressionItem)selectItems.get(i)).getAlias().toString();
								TableContainer.columnType.put(columnName, res.rowList.get(i).getType().toString());
							}
						}
						generateTable = true;
					}
					return true;
				}		
				if(this.hasOrderBy) pw.close();
				buffer.close();
			}else{
				while(index < dataCopy.size()){
					data2 = dataCopy.get(index++);							
					if(whereExp != null && !eval1.eval(whereExp).toBool()) continue;	
					
					if(hasOrderBy) {
						TableContainer.orderByData.add(new Row());
						for(PrimitiveValue v : data2.rowList)
							TableContainer.orderByData.get(count).rowList.add(v);	
						count++;
					}				
					/*if the query is SELECT * FROM R*/
					if(flag2){
						res = data2;
					}else{
						/*if it is the regular query*/
						for (int i = 0; i < itemLen; i++) res.rowList.add(eval1.eval((selectItemsExpression.get(i))));
					}
					return true;
				}		
			}
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
		return res;
	}

	public Eval eval = new Eval(){
		@Override
		public PrimitiveValue eval(Column c) {
			String columnName = tableName+"." + c.getColumnName().toString();
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
	
	public Eval eval1 = new Eval(){
		@Override
		public PrimitiveValue eval(Column c) {
			return data2.rowList.get(TableContainer.columns.get(c.toString()));
		}
	};
}
