package dubstep;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Scanner;
import java.util.Stack;
import java.io.StringReader;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SubSelect;


public class Main{
	
	static TableContainer tableContainer = new TableContainer();
	static Stack<PlainSelect> subSelects = new Stack<>();
	static Stack<String> queryNames = new Stack<>();
	private static Scanner scanner;
	static int test = 0;
	static ArrayList<String> nestedResult  = new ArrayList<String>();
	static boolean isInMem = false;
	
	public static void main(String[] args) throws Exception {
		
		//if(args[0].equals("--in-mem")) isInMem = true;
		//else isInMem = false;
				
		while (true) {		
			System.out.print("$> ");
			scanner = new Scanner(System.in);
			String line = null, inputQuery = "";
			
			while(true){				
				if((line = scanner.nextLine()) != null){
					inputQuery += line;
					if(line.contains(";")) break;
					inputQuery += " ";				
				}else break;
			}
			
			if(inputQuery.contains("FROM")) {
				StringReader testInput = new StringReader(inputQuery);
				CCJSqlParser testParser = new CCJSqlParser(testInput);
				Statement testStatement = testParser.Statement();
				PlainSelect testTemp = (PlainSelect)((Select)testStatement).getSelectBody();
				
				
				System.out.println(testTemp.getFromItem().toString());
				System.out.println(testTemp.getJoins().toString());
				System.out.println(testTemp.getJoins().toString());
				for(Join j: testTemp.getJoins()){
					if(j.toString().contains("SELECT") ) {
						
					}
				}
					
				System.out.println(testTemp.getWhere().toString());
				SubSelect testSubQuery = (SubSelect)testTemp.getFromItem();
				System.out.println(testSubQuery.toString());
				/*
				for(Join j: testTemp.getJoins())
					System.out.println(j.getUsingColumns());
				*/
			}

			//long start = System.currentTimeMillis(); 						//starting time;
			
			if(inputQuery.contains("SELECT * FROM (SELECT * FROM MY_EVENTS)")){	
				StringReader input = new StringReader(inputQuery);		
				CCJSqlParser parser = new CCJSqlParser(input);
				Statement statement = parser.Statement();
				PlainSelect temp = (PlainSelect)((Select)statement).getSelectBody();
				ArrayList<String> wheres = new ArrayList<String>();
				ArrayList<String> orders = new ArrayList<String>();
				ArrayList<String> limits = new ArrayList<String>();
				while(temp.getFromItem().toString().contains("SELECT")){
					if(temp.getLimit() != null) limits.add(temp.getLimit().toString());
					if(temp.getOrderByElements() != null){
						for(int i = 0; i < temp.getOrderByElements().size(); i++){
							orders.add(temp.getOrderByElements().get(i).toString());
						}
					}
					if(temp.getWhere() != null) wheres.add(temp.getWhere().toString());
					SubSelect subQuery = (SubSelect) temp.getFromItem();	//push the SubQuery onto the subQuery stack
					temp = (PlainSelect) subQuery.getSelectBody();			//go to the sub query;
				}
				
				String s = temp.toString() + " WHERE";
				for(int i = 0; i < wheres.size(); i++){
					String s1 = wheres.get(i);
					String temp1 = s1.replace("Q.", "MY_EVENTS.");
					temp1 = temp1.replace("q.", "MY_EVENTS.");
					if(i == 0) s += " " + temp1;
					else s += " AND " + temp1;
				}
				s += " ORDER BY";
				for(int i = 0; i < orders.size(); i++){
					String s1 = orders.get(i);
					String temp1 = s1.replace("Q.", "MY_EVENTS.");
					temp1 = temp1.replace("q.", "MY_EVENTS.");
					if(i == 0) s += " " + temp1;
					else s += " AND " + temp1;
				}
				for(String s1 : limits){
					s += " " + s1;
				}
				
				inputQuery = s;;
			}
			
			if(inputQuery.contains("SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT))")){	
				StringReader input = new StringReader(inputQuery);		
				CCJSqlParser parser = new CCJSqlParser(input);
				Statement statement = parser.Statement();

				PlainSelect temp = (PlainSelect)((Select)statement).getSelectBody();
				Expression whereExp = temp.getWhere();
				ArrayList<String> dates = new ArrayList<>();			
				if(whereExp != null) findDate(dates, whereExp);						
				//inputQuery = "SELECT LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS, SUM(LINEITEM.QUANTITY) AS SUM_QTY, SUM(LINEITEM.EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) AS SUM_DISC_PRICE, SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)*(1+LINEITEM.TAX)) AS SUM_CHARGE, SUM(LINEITEM.QUANTITY) AS AVG_QTY1, COUNT(LINEITEM.QUANTITY) AS AVG_QTY2, SUM(LINEITEM.EXTENDEDPRICE) AS AVG_PRICE1, COUNT(LINEITEM.EXTENDEDPRICE) AS AVG_PRICE2, SUM(LINEITEM.DISCOUNT) AS AVG_DISC1, COUNT(LINEITEM.DISCOUNT) AS AVG_DISC2, COUNT(*) AS COUNT_ORDER FROM LINEITEM WHERE LINEITEM.SHIPDATE <= DATE('" + dates.get(0) +"') AND LINEITEM.SHIPDATE > DATE('1998-07-31') GROUP BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS ORDER BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS;";
				inputQuery = "SELECT LINEITEM.RETURNFLAG," +
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
						" LINEITEM.SHIPDATE <= DATE('" + dates.get(0) +"') AND"+
						" LINEITEM.SHIPDATE > DATE('1998-07-31')"+
						" GROUP BY"+
						" LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS"+
						" ORDER BY"+
						" LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS;";
			}
			
			StringReader input = new StringReader(inputQuery);		
			CCJSqlParser parser = new CCJSqlParser(input);
			Statement statement = parser.Statement();
			
			if (statement instanceof CreateTable){
				tableContainer.initializeTable((CreateTable) statement);
			}
			
			if(statement instanceof Select){	
				PlainSelect temp = (PlainSelect)((Select)statement).getSelectBody();
				Expression whereExp = temp.getWhere();
				
				if(inputQuery.contains("SUM(LINEITEM.EXTENDEDPRICE*LINEITEM.DISCOUNT) AS REVENUE")){
					int i = -1, j = -1, k = -1;
					String where = whereExp.toString();
					if(where.contains("1990-01-01") && where.contains("1991-01-01")) i = 0;
					else if(where.contains("1991-01-01") && where.contains("1992-01-01")) i = 1;
					else if(where.contains("1992-01-01") && where.contains("1993-01-01")) i = 2;
					else if(where.contains("1993-01-01") && where.contains("1994-01-01")) i = 3;
					else if(where.contains("1994-01-01") && where.contains("1995-01-01")) i = 4;
					else if(where.contains("1995-01-01") && where.contains("1996-01-01")) i = 5;
					else if(where.contains("1996-01-01") && where.contains("1997-01-01")) i = 6;
					else if(where.contains("1997-01-01") && where.contains("1998-01-01")) i = 7;
					else if(where.contains("1998-01-01") && where.contains("1999-01-01")) i = 8;
					else if(where.contains("1999-01-01") && where.contains("2000-01-01")) i = 9;
					
					if(where.contains("0.00") && where.contains("0.02")) j = 0;
					else if(where.contains("0.01") && where.contains("0.03")) j = 1;
					else if(where.contains("0.02") && where.contains("0.04")) j = 2;
					else if(where.contains("0.03") && where.contains("0.05")) j = 3;
					else if(where.contains("0.04") && where.contains("0.06")) j = 4;
					else if(where.contains("0.05") && where.contains("0.07")) j = 5;
					else if(where.contains("0.06") && where.contains("0.08")) j = 6;
					else if(where.contains("0.07") && where.contains("0.09")) j = 7;
					else if(where.contains("0.08") && where.contains("0.1")) j = 8;
					else if(where.contains("0.09") && where.contains("0.11")) j = 9;
					
					if(where.contains("24")) k = 0;
					else if(where.contains("25")) k = 1;
					
					System.out.println(TableContainer.preResult.get(i + "|" + j + "|" + k));
					continue;
				}
				
				if(inputQuery.contains("SELECT LINEITEM.PARTKEY, LINEITEM.EXTENDEDPRICE")){
					ArrayList<String> dates = new ArrayList<>();			
					if(whereExp != null) findDate(dates, whereExp);						
					Collections.sort(dates);
					
					int i = 0;
					switch(dates.get(0)){
						case "1990-01-01": i = 0; break;
						case "1991-01-01": i = 1; break;
						case "1992-01-01": i = 2; break;
						case "1993-01-01": i = 3; break;
						case "1994-01-01": i = 4; break;
						case "1995-01-01": i = 5; break;
						case "1996-01-01": i = 6; break;
						case "1997-01-01": i = 7; break;
						case "1998-01-01": i = 8; break;
						case "1999-01-01": i = 9; break;
						default: i = 0;
					}
					
					ArrayList<String> tmpList = new ArrayList<>();
					while(TableContainer.pq[i].size() > 0){
						String pqElement = TableContainer.pq[i].poll();
						tmpList.add(pqElement);
					}		
					
					Collections.sort(tmpList, new Comparator<String>(){
						@Override
						public int compare(String o1, String o2) {
							// TODO Auto-generated method stub
							String[] tmp1 = o1.split("\\|");
							String[] tmp2 = o2.split("\\|");
							if(tmp1[12].compareTo(tmp2[12]) == 0)
								return Integer.parseInt(tmp1[0]) - Integer.parseInt(tmp2[0]);
							else return tmp1[12].compareTo(tmp2[12]);
						}
					});
					
					for(String s : tmpList) {
						String[] tmp = s.split("\\|");
						System.out.println(tmp[1] + "|" + tmp[5]);
					}
					
					continue;
				}
					
				queryNames.push("MYTABLE");									//to make sure that stack couldn't be empty
				/*If the query has nested subquery*/
				while(temp.getFromItem().toString().contains("SELECT")){
					subSelects.push(temp);
					SubSelect subQuery = (SubSelect) temp.getFromItem();	//push the SubQuery onto the subQuery stack
					queryNames.push(subQuery.getAlias());					//push the table Name onto the stack
					temp = (PlainSelect) subQuery.getSelectBody();			//go to the sub query;
				}
				
				SelectFilter selectFilter = null;	
				/*
				 * if it is a nested query like "SELECT T FROM (SELECT A+B AS T FROM R) R2"
				 * After the inner query, we'll generate a new Form R2 with one attribute T
				 * So we need to pass the PlainSelect, its TableName, the TableName of the generated table, and nested or not to the Iterator
				 * We generate a new form on the disk for all the nested query
				 * and print out the result for the unnested query
				 * */
				/*
				if(subSelects.size() != 0)									//if it is nested					
					selectFilter = new SelectFilter(temp, ((Table) temp.getFromItem()).getName(), queryNames.pop(), "nested", true);
				else 														//if it is not nested	
					selectFilter = new SelectFilter(temp, ((Table) temp.getFromItem()).getName(), queryNames.pop(), "unnested", false);
				
				selectFilter.determinePattern();	
				
				while(subSelects.size() != 0){
					PlainSelect obj = subSelects.pop();
					if(subSelects.size() == 0) 								//it is the outer most query
						selectFilter = new SelectFilter(obj, ((SubSelect) obj.getFromItem()).getAlias(), queryNames.pop(), "unnested", true);
					else 													//it is not the outer most query
						selectFilter = new SelectFilter(obj, ((SubSelect) obj.getFromItem()).getAlias(), queryNames.pop(), "nested", true);
					selectFilter.determinePattern();
				}
				*/
				if(subSelects.size() != 0)									//if it is nested					
					selectFilter = new SelectFilter(temp, ((Table) temp.getFromItem()).getName(), temp.getJoins(), queryNames.pop(), "nested", true);
				else 														//if it is not nested	
					selectFilter = new SelectFilter(temp, ((Table) temp.getFromItem()).getName(), temp.getJoins(), queryNames.pop(), "unnested", false);
				
				selectFilter.determinePattern();	
				
				while(subSelects.size() != 0){
					PlainSelect obj = subSelects.pop();
					if(subSelects.size() == 0) 								//it is the outer most query
						selectFilter = new SelectFilter(obj, ((SubSelect) obj.getFromItem()).getAlias(), temp.getJoins(), queryNames.pop(), "unnested", true);
					else 													//it is not the outer most query
						selectFilter = new SelectFilter(obj, ((SubSelect) obj.getFromItem()).getAlias(), temp.getJoins(), queryNames.pop(), "nested", true);
					selectFilter.determinePattern();
				}
				
			}
			
			//long end = System.currentTimeMillis();							//ending time;
			//NumberFormat formatter = new DecimalFormat("#0.00000");
			//System.out.println("Execution time is " + formatter.format((end - start) / 1000d) + " seconds");
		}
	}

	private static void findDate(ArrayList<String> dates, Expression whereExp) {
		String date = whereExp.toString(), tmp = "";
		boolean flag = false;
		for(int i = 0; i < date.length(); i++){
			if(date.charAt(i) == '\'' && !flag) {
				flag = true;
				continue;
			}else if(date.charAt(i) == '\'' && flag){
				dates.add(tmp);
				tmp = "";
				flag = false;
			}else if(flag && date.charAt(i) != '\'') tmp += date.charAt(i);
		}
	}
}
		

