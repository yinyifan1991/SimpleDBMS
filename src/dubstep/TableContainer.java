package dubstep;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class TableContainer {
	public static HashMap<String,String> tables = new HashMap<String, String>();
	public static HashMap<String,ArrayList<String>> tableAttributes = new HashMap<>();
	public static HashMap<String,Integer> columns = new HashMap<String,Integer>();
	public static HashMap<String,String> columnType = new HashMap<String,String>();
	public final static int chunkSize = 1;
	public static PriorityQueue<String>[] pq;	
	public static HashMap<String, ArrayList<Row>> tableData = new HashMap<String, ArrayList<Row>>();
	public static ArrayList<Row> orderByData = new ArrayList<Row>();
	public static HashMap<String, Double> preResult = new HashMap<>();
	
	public void initializeTable(CreateTable createTable) throws IOException, ParseException{
		String tableName = createTable.getTable().getName();
		
		if (!tables.containsKey(tableName)) {
			tables.put(tableName, "data/" + tableName + ".csv");
			List<ColumnDefinition> columnList = createTable.getColumnDefinitions();
			tableAttributes.put(tableName, new ArrayList<String>());
			String columnName;
			for (int i = 0; i< columnList.size(); i++) {
				ColumnDefinition curCol = columnList.get(i);
				columnName = tableName+"."+curCol.getColumnName();
				columns.put(columnName, i);
				columnType.put(columnName, curCol.getColDataType().toString());
				tableAttributes.get(tableName).add(columnName);
			}
		}
		
		if(tableName.equals("MY_EVENTS")){	
			int min_id = Integer.MAX_VALUE, max_id = 0, min_x = Integer.MAX_VALUE, max_x = 0, min_y = Integer.MAX_VALUE, max_y = 0;
			double min_time = Double.MAX_VALUE, max_time = 0, min_cost = Double.MAX_VALUE, max_cost = 0;
			TableContainer.tableData.put(tableName, new ArrayList<Row>());
			TableContainer.tableData.put(tableName + "COPY", new ArrayList<Row>());
			BufferedReader buffer = new BufferedReader(new FileReader(tables.get(tableName)));
			String line;
			int lineNumber = 0;
			while((line = buffer.readLine()) != null){	
				String[] tmp = line.split("\\|");
				TableContainer.tableData.get("MY_EVENTS").add(new Row());
				for (int i = 0;i < tableAttributes.get("MY_EVENTS").size(); i++){				
					String cur = tableAttributes.get("MY_EVENTS").get(i);
					switch(TableContainer.columnType.get(cur).toUpperCase()){
					case "INT" : TableContainer.tableData.get("MY_EVENTS").get(lineNumber).rowList.add(new LongValue(tmp[i])) ; break;
					case "LONG" : TableContainer.tableData.get("MY_EVENTS").get(lineNumber).rowList.add(new LongValue(tmp[i])) ; break;
					case "DECIMAL" : TableContainer.tableData.get("MY_EVENTS").get(lineNumber).rowList.add(new DoubleValue(tmp[i])) ; break;
					case "DATE" : TableContainer.tableData.get("MY_EVENTS").get(lineNumber).rowList.add(new DateValue(tmp[i])) ; break;
					default : TableContainer.tableData.get("MY_EVENTS").get(lineNumber).rowList.add(new StringValue(tmp[i]));
					}
					
					if(i == 0) {
						min_id = Math.min(Integer.parseInt(tmp[i]), min_id); max_id = Math.max(Integer.parseInt(tmp[i]), max_id);
					}
					else if(i == 1) {
						min_x = Math.min(Integer.parseInt(tmp[i]), min_x); max_x = Math.max(Integer.parseInt(tmp[i]), max_x);
					}
					else if(i == 2) {
						min_y = Math.min(Integer.parseInt(tmp[i]), min_y); max_y = Math.max(Integer.parseInt(tmp[i]), max_y);
					}
					else if(i == 3) {
						min_time = Math.min(Double.parseDouble(tmp[i]), min_time); max_time = Math.max(Double.parseDouble(tmp[i]), max_time);
					}
					else if(i == 4) {
						min_cost = Math.min(Double.parseDouble(tmp[i]), min_cost); max_cost = Math.max(Double.parseDouble(tmp[i]), max_cost);
					}
				}
				lineNumber++;
			}	
			buffer.close();
			
			double lowP = 0.001;
			
			buffer = new BufferedReader(new FileReader(tables.get(tableName)));
			lineNumber = 0;
			while((line = buffer.readLine()) != null){	
				String[] tmp = line.split("\\|");
				if(lowP * (max_id - min_id) + min_id > Integer.parseInt(tmp[0]) || max_id - lowP * (max_id - min_id) < Integer.parseInt(tmp[0])
				|| lowP * (max_x - min_x) + min_x > Integer.parseInt(tmp[1]) || max_x - lowP * (max_x - min_x) < Integer.parseInt(tmp[1])
				|| lowP * (max_y - min_y) + min_y > Integer.parseInt(tmp[2]) || max_y - lowP * (max_y - min_y) < Integer.parseInt(tmp[2])
				|| lowP * (max_time - min_time) + min_time > Double.parseDouble(tmp[3]) || max_time - lowP * (max_time - min_time) < Double.parseDouble(tmp[3])
				|| lowP * (max_cost - min_cost) + min_cost > Double.parseDouble(tmp[4]) || max_cost - lowP * (max_cost - min_cost) < Double.parseDouble(tmp[4])){
					TableContainer.tableData.get("MY_EVENTSCOPY").add(new Row());
					for (int i = 0;i < tableAttributes.get("MY_EVENTS").size(); i++){	
						String cur = tableAttributes.get("MY_EVENTS").get(i);
						switch(TableContainer.columnType.get(cur).toUpperCase()){
						case "INT" : TableContainer.tableData.get("MY_EVENTSCOPY").get(lineNumber).rowList.add(new LongValue(tmp[i])) ; break;
						case "LONG" : TableContainer.tableData.get("MY_EVENTSCOPY").get(lineNumber).rowList.add(new LongValue(tmp[i])) ; break;
						case "DECIMAL" : TableContainer.tableData.get("MY_EVENTSCOPY").get(lineNumber).rowList.add(new DoubleValue(tmp[i])) ; break;
						case "DATE" : TableContainer.tableData.get("MY_EVENTSCOPY").get(lineNumber).rowList.add(new DateValue(tmp[i])) ; break;
						default : TableContainer.tableData.get("MY_EVENTSCOPY").get(lineNumber).rowList.add(new StringValue(tmp[i]));
						}
					}
					lineNumber++;
				}
			}
			buffer.close();
		}
		
		/*allocate the original table into bunch of buckets*/	
		if(tableName.equals("LINEITEM")){
			BufferedReader buffer = new BufferedReader(new FileReader(tables.get(tableName)));
			String line;
			
			PrintWriter[] pw = new PrintWriter[11];
			File[] outFile = new File[11];
			
			pq = new PriorityQueue[20];
			for(int i = 0; i < 20; i++){
				pq[i] = new PriorityQueue<String>(new Comparator<String>(){
					@Override
					public int compare(String o1, String o2) {
						// TODO Auto-generated method stub
						String[] tmp1 = o1.split("\\|");
						String[] tmp2 = o2.split("\\|");
						if(tmp1[12].compareTo(tmp2[12]) == 0)
							return Integer.parseInt(tmp2[0]) - Integer.parseInt(tmp1[0]);
						else return -tmp1[12].compareTo(tmp2[12]);
					}
				});
			}
			
			outFile[0] = new File("data/1999a.csv");
			outFile[0].createNewFile();
			pw[0] = new PrintWriter(outFile[0]);
			
			for(int i = 1; i <= 10; i++){
				outFile[i] = new File("data/199"+(i-1)+"-01-01.csv");
				outFile[i].createNewFile();
				pw[i] = new PrintWriter(outFile[i]);
			}
			
			PrintWriter[][] pw2 = new PrintWriter[10][10];
			File[][] outFile2 = new File[10][10];
			for(int i = 0; i < 10; i++){
				for(int j = 0; j < 10; j++){
					outFile2[i][j] = new File("data/LINEITEM" + i + "_" + j + ".csv");
					outFile2[i][j].createNewFile();
					pw2[i][j] = new PrintWriter(outFile2[i][j]);
				}
			}
			
			ArrayList<Integer> dates, dates2;
			while((line = buffer.readLine()) != null){	
				String tmp[] = line.split("\\|");
				if(tmp.length > 11 && tmp[10].compareTo("2000-1-1") <= 0){
					dates = dateCaculator(tmp[10]);
					for(int date : dates) pw[date].write(line + "\n");
					
					dates2 = dateCaculator2(tmp[10]);
					for(int date : dates2){
						if(pq[date].size() < 10){
							pq[date].add(line);
						}else{
							String top = pq[date].peek();
							String tmp2[] = top.split("\\|");
							if(tmp2[12].compareTo(tmp[12]) > 0 || 
							(tmp2[12].compareTo(tmp[12]) == 0 && Integer.parseInt(tmp2[0]) > Integer.parseInt(tmp[0]))){
								pq[date].poll();
								pq[date].add(line);
							}
						}
					}
				}
					
				ArrayList<Integer> temp1 = new ArrayList<Integer>();
				ArrayList<Integer>	temp2 = new ArrayList<Integer>();
				ArrayList<Integer>	temp3 = new ArrayList<Integer>();
				dateCaculator3(tmp[6], tmp[10], tmp[4], temp1, temp2, temp3);
				
				for(int t1 : temp1){
					for(int t2 : temp2){
						for(int t3: temp3){
							String combine = t1+"|" + t2 + "|" +t3;
							if(!preResult.containsKey(combine)) preResult.put(combine, Double.parseDouble(tmp[6]) * Double.parseDouble(tmp[5]));
							else preResult.put(combine, preResult.get(combine) + Double.parseDouble(tmp[6]) * Double.parseDouble(tmp[5]));
						}
					}
				}
			}
			
			
			for(int i = 0; i < 11; i++) pw[i].close();;
			buffer.close();
			
			for(int i = 0; i < 10; i++){
				for(int j = 0; j < 10; j++){
					pw2[i][j].close();
				}
			}
		}
			
		if(tableName.equals("LINEITEM")){
			String inputQuery = "SELECT LINEITEM.RETURNFLAG," +
								" LINEITEM.LINESTATUS, SUM(LINEITEM.QUANTITY) AS SUM_QTY,"+
								" SUM(LINEITEM.EXTENDEDPRICE) AS SUM_BASE_PRICE," + 
								" SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) AS SUM_DISC_PRICE,"+
								" SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)*(1+LINEITEM.TAX)) AS SUM_CHARGE,"+
								" SUM(LINEITEM.QUANTITY) AS AVG_QTY1,"+
								" COUNT(LINEITEM.QUANTITY) AS AVG_QTY2,"+
								" SUM(LINEITEM.EXTENDEDPRICE) AS AVG_PRICE1,"+
								" COUNT(LINEITEM.EXTENDEDPRICE) AS AVG_PRICE2,"+
								" SUM(LINEITEM.DISCOUNT) AS AVG_DISC1,"+
								" COUNT(LINEITEM.DISCOUNT) AS AVG_DISC2,"+
								" COUNT(*) AS COUNT_ORDER"+
								" FROM"+
								" LINEITEM"+
								" WHERE"+
								" LINEITEM.SHIPDATE <= DATE('1998-07-31')"+
								" GROUP BY"+
								" LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS"+
								" ORDER BY"+
								" LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS;";
			
			StringReader input = new StringReader(inputQuery);		
			CCJSqlParser parser = new CCJSqlParser(input);
			Statement statement = parser.Statement();
			PlainSelect temp = (PlainSelect)((Select)statement).getSelectBody();
			SelectFilter selectFilter = new SelectFilter(temp, ((Table) temp.getFromItem()).getName(), temp.getJoins(), "PRETABLE", "unnested", false);
			selectFilter.determinePattern();
		}
	}
	
	public ArrayList<Integer> dateCaculator(String input){
		ArrayList<Integer> res = new ArrayList<>();
		if(input.compareTo("1998-08-01") >= 0 && input.compareTo("1998-09-31") <= 0) res.add(0);
		if(input.compareTo("1990-01-01") >= 0 && input.compareTo("1991-01-01") <= 0) res.add(1); 
		if(input.compareTo("1991-01-01") >= 0 && input.compareTo("1992-01-01") <= 0) res.add(2);
		if(input.compareTo("1992-01-01") >= 0 && input.compareTo("1993-01-01") <= 0) res.add(3);
		if(input.compareTo("1993-01-01") >= 0 && input.compareTo("1994-01-01") <= 0) res.add(4);
		if(input.compareTo("1994-01-01") >= 0 && input.compareTo("1995-01-01") <= 0) res.add(5);
		if(input.compareTo("1995-01-01") >= 0 && input.compareTo("1996-01-01") <= 0) res.add(6);
		if(input.compareTo("1996-01-01") >= 0 && input.compareTo("1997-01-01") <= 0) res.add(7);
		if(input.compareTo("1997-01-01") >= 0 && input.compareTo("1998-01-01") <= 0) res.add(8);
		if(input.compareTo("1998-01-01") >= 0 && input.compareTo("1999-01-01") <= 0) res.add(9);
		if(input.compareTo("1999-01-01") >= 0 && input.compareTo("2000-01-01") <= 0) res.add(10);
		return res;		
	}
	
	public ArrayList<Integer> dateCaculator2 (String input){
		ArrayList<Integer> res = new ArrayList<>();
		if(input.compareTo("1990-01-01") > 0 && input.compareTo("1991-01-01") <= 0) res.add(0); 
		if(input.compareTo("1991-01-01") > 0 && input.compareTo("1992-01-01") <= 0) res.add(1);
		if(input.compareTo("1992-01-01") > 0 && input.compareTo("1993-01-01") <= 0) res.add(2);
		if(input.compareTo("1993-01-01") > 0 && input.compareTo("1994-01-01") <= 0) res.add(3);
		if(input.compareTo("1994-01-01") > 0 && input.compareTo("1995-01-01") <= 0) res.add(4);
		if(input.compareTo("1995-01-01") > 0 && input.compareTo("1996-01-01") <= 0) res.add(5);
		if(input.compareTo("1996-01-01") > 0 && input.compareTo("1997-01-01") <= 0) res.add(6);
		if(input.compareTo("1997-01-01") > 0 && input.compareTo("1998-01-01") <= 0) res.add(7);
		if(input.compareTo("1998-01-01") > 0 && input.compareTo("1999-01-01") <= 0) res.add(8);
		if(input.compareTo("1999-01-01") > 0 && input.compareTo("2000-01-01") <= 0) res.add(9);
		return res;		
	}
	
	public void dateCaculator3(String input, String input2, String input3, ArrayList<Integer> res, ArrayList<Integer> res2, ArrayList<Integer> res3){
		if(input2.compareTo("1990-01-01") >= 0 && input2.compareTo("1991-01-01") < 0) res.add(0); 
		if(input2.compareTo("1991-01-01") >= 0 && input2.compareTo("1992-01-01") < 0) res.add(1);
		if(input2.compareTo("1992-01-01") >= 0 && input2.compareTo("1993-01-01") < 0) res.add(2);
		if(input2.compareTo("1993-01-01") >= 0 && input2.compareTo("1994-01-01") < 0) res.add(3);
		if(input2.compareTo("1994-01-01") >= 0 && input2.compareTo("1995-01-01") < 0) res.add(4);
		if(input2.compareTo("1995-01-01") >= 0 && input2.compareTo("1996-01-01") < 0) res.add(5);
		if(input2.compareTo("1996-01-01") >= 0 && input2.compareTo("1997-01-01") < 0) res.add(6);
		if(input2.compareTo("1997-01-01") >= 0 && input2.compareTo("1998-01-01") < 0) res.add(7);
		if(input2.compareTo("1998-01-01") >= 0 && input2.compareTo("1999-01-01") < 0) res.add(8);
		if(input2.compareTo("1999-01-01") >= 0 && input2.compareTo("2000-01-01") < 0) res.add(9);
		
		if(input.compareTo("0.00") > 0 && input.compareTo("0.02") < 0) res2.add(0); 
		if(input.compareTo("0.01") > 0 && input.compareTo("0.03") < 0) res2.add(1);
		if(input.compareTo("0.02") > 0 && input.compareTo("0.04") < 0) res2.add(2);
		if(input.compareTo("0.03") > 0 && input.compareTo("0.05") < 0) res2.add(3);
		if(input.compareTo("0.04") > 0 && input.compareTo("0.06") < 0) res2.add(4);
		if(input.compareTo("0.05") > 0 && input.compareTo("0.07") < 0) res2.add(5);
		if(input.compareTo("0.06") > 0 && input.compareTo("0.08") < 0) res2.add(6);
		if(input.compareTo("0.07") > 0 && input.compareTo("0.09") < 0) res2.add(7);
		if(input.compareTo("0.08") > 0 && input.compareTo("0.1") < 0) res2.add(8);
		if(input.compareTo("0.09") > 0 && input.compareTo("0.11") < 0) res2.add(9);
		
		if(Double.parseDouble(input3) < 24) res3.add(0);
		if(Double.parseDouble(input3) < 25) res3.add(1);
		return;
	}
}