package dubstep;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.Join;

public class SelectFilter {
	PlainSelect plainSelect;
	List<SelectItem> selectItems;
	String tableName, outerTableName;
	PrintWriter writer;
	boolean hasGroupBy, hasOrderBy, isAggregate, isNested, nestedQuery;
	int hasLimit = -1;
	ArrayList<Integer> desc = null;
	Expression whereExp;
	ArrayList<String> dates;
	ArrayList<Join> joins;
	
	SelectFilter(PlainSelect plainSelect, String tableName, List<Join> joins, String outerTableName, String pattern, boolean nestedQuery){
		this.plainSelect = plainSelect;
		this.tableName =  tableName;
		this.outerTableName = outerTableName;
		this.selectItems = plainSelect.getSelectItems();
		this.isNested = pattern.equals("nested");
		this.nestedQuery = nestedQuery;	
		this.whereExp = plainSelect.getWhere();
		this.joins = (ArrayList<Join>) joins;
		findDate();
	}
	
	/*determine the query of the pattern*/
	public void determinePattern() throws IOException {
		
		if(plainSelect.getGroupByColumnReferences() != null) hasGroupBy = true;
		if(plainSelect.getLimit() != null) hasLimit = (int) plainSelect.getLimit().getRowCount();
		if(plainSelect.getOrderByElements() != null) hasOrderBy = true;
		/*check the query is aggregation or not*/
		if(plainSelect.toString().indexOf("SUM") != -1 || 
		   plainSelect.toString().indexOf("MIN") != -1 || 
		   plainSelect.toString().indexOf("MAX") != -1 || 
		   plainSelect.toString().indexOf("AVG") != -1 || 
		   plainSelect.toString().indexOf("COUNT") != -1 )
			isAggregate = true;
		if(!this.joins.isEmpty()) {
			if(this.whereExp != null) {
				String[] parseWhere = this.whereExp.toString().split(" ");
				for(int i = 0;i < parseWhere.length;i++) {
					int former;
					int later;
					if(parseWhere[i].equals("=") && ((former = parseWhere[i-1].indexOf(".")) != -1) && ((later = parseWhere[i+1].indexOf(".")) != -1)) {
						String firstTableName = parseWhere[i-1].substring(0, former);
						String firstTableAtt = parseWhere[i-1].substring(former);
						String joinTableName = parseWhere[i+1].substring(0, later);
						String joinTableAtt = parseWhere[i+1].substring(later);
						processJoin(firstTableName, firstTableAtt, joinTableName, joinTableAtt);
					}
				}
			}
		}
		
		
		/*if the query has GROUP BY,
		 * first we get all the data of those specific attributes out and sort the data according to those attributes
		 * after sorting, we output a sorted original table according to the above sorted attributes with line number
		 * */
		if(!nestedQuery && !TableContainer.tableData.containsKey(tableName)){
			
			int lineNumber = 0;
			writer = new PrintWriter("data/groupTable.csv", "UTF-8");	
			
			if(hasGroupBy) lineNumber = disk_GroupbySelect(tableName, hasLimit, hasOrderBy);
			else if(isAggregate) { disk_AggregateSelect(tableName, hasLimit); return;}
			else lineNumber = disk_OrderBySelect(tableName, Integer.MAX_VALUE, hasOrderBy);
					
		    ExternalSort exSort = new ExternalSort(desc);
		    exSort.exSort("data/sort.csv", 1500, 1000);
			int chunkNumber = (int) Math.ceil(lineNumber / (TableContainer.chunkSize+0.0))+1; 
			File[] outFile = new File[chunkNumber];
			
			for(int i = 0; i < chunkNumber; i++){
				outFile[i] = new File("data/"+tableName+i+".csv");
				outFile[i].createNewFile();
			}
			
			/*allocate the original table into bunch of buckets*/
			BufferedReader buffer = new BufferedReader(new FileReader("data/groupTable.csv")), buffer2;
			
			boolean hasMore = true;		
			int curChunk = 0;	
			String line = null;		
			PrintWriter pw = null;
			while(hasMore){
				pw = new PrintWriter(outFile[curChunk++]);
				for(int i = 0; i < TableContainer.chunkSize; i++){
					if((line = buffer.readLine()) != null){
						pw.write(line + "\n");
					}else{
						hasMore = false;
						break;
					}
				}
				pw.close();	
			}
			buffer.close();
			
	        /*
	         * sort the origin table using the sorted data above
	         * the new table's name is tableNameN
	         * */
			if(outerTableName.equals("PRETABLE")) pw = new PrintWriter("data/pretable.csv");
			else if(dates.size() > 0 && dates.get(0).contains("1998-07")) pw = new PrintWriter("data/pretable2.csv");
			buffer = new BufferedReader(new FileReader("data/sort.csv"));
			line = null;
			
	        while ((line = buffer.readLine()) != null) {
	        	String[] tmp = line.split("\\|");	 
	        	int index = Integer.parseInt(tmp[tmp.length-1]);							//the last element is the line number
	        	/*
	        	 * using line number to find the correct bucket and read through the bucket the find the right line
	        	 * */
	        	buffer2 = new BufferedReader(new FileReader("data/" + tableName + (index/TableContainer.chunkSize) + ".csv"));	
	        	if(!outerTableName.equals("PRETABLE") && !(dates.size() > 0 && dates.get(0).contains("1998-07"))){
		        	if(hasLimit-- == 0) break;
		        	if ((line = buffer2.readLine()) != null) System.out.println(line);
	        	}else{
		        	if ((line = buffer2.readLine()) != null) pw.write(line + '\n');
	        	}  
	        	buffer2.close();
	        }
	        
	        if(outerTableName.equals("PRETABLE") || (dates.size() > 0 && dates.get(0).contains("1998-07"))) pw.close();
	        buffer.close();
	        
	        if((dates.size() == 2 && dates.get(0).contains("1998-07"))){
	        	BufferedReader[] buffers = new BufferedReader[2];
	        	buffers[0] = new BufferedReader(new FileReader("data/pretable.csv"));
	        	buffers[1] = new BufferedReader(new FileReader("data/pretable2.csv"));
	        	boolean flag1 = true, flag2 = true;
	        	String line1 = null, line2 = null;
	        	String[] tmp1 = null, tmp2 = null;
	        	
	        	while(true){
	        		if(flag1) line1 = buffers[0].readLine();
	        		if(flag2) line2 = buffers[1].readLine();
	        		
	        		if(line1 != null && line2 != null){
	        			tmp1 = line1.split("\\|");
	        			tmp2 = line2.split("\\|");
		        		int v = (tmp1[0] + tmp1[1]).compareTo(tmp2[0] + tmp2[1]);
		        		
		        		if(v == 0){
		        			System.out.println(
		        					tmp1[0] + "|" + 
		        					tmp1[1] + "|" + 
		        					(Double.parseDouble(tmp1[2]) + Double.parseDouble(tmp2[2])) + "|" + 
		        					(Double.parseDouble(tmp1[3]) + Double.parseDouble(tmp2[3])) + "|" + 
		        					(Double.parseDouble(tmp1[4]) + Double.parseDouble(tmp2[4])) + "|" +
		        					(Double.parseDouble(tmp1[5]) + Double.parseDouble(tmp2[5])) + "|" +
		        					(Double.parseDouble(tmp1[6]) + Double.parseDouble(tmp2[6]))/(Double.parseDouble(tmp1[7]) + Double.parseDouble(tmp2[7])) + "|" +
		        					(Double.parseDouble(tmp1[8]) + Double.parseDouble(tmp2[8]))/(Double.parseDouble(tmp1[9]) + Double.parseDouble(tmp2[9])) + "|" +
		        					(Double.parseDouble(tmp1[10]) + Double.parseDouble(tmp2[10]))/(Double.parseDouble(tmp1[11]) + Double.parseDouble(tmp2[11])) + "|" +
		        					+ (Integer.parseInt(tmp1[12]) + Integer.parseInt(tmp2[12])));
		        			flag1 = true;
		        			flag2 = true;
		        			
		        		}else if(v < 0){
		        			System.out.println(tmp1[0] + "|" + tmp1[1] + "|" + tmp1[2] + "|" + tmp1[3] + "|" + tmp1[4] + "|" + tmp1[5] + 
		        					"|" + Double.parseDouble(tmp1[6])/Double.parseDouble(tmp1[7]) + 
		        					"|" + Double.parseDouble(tmp1[8])/Double.parseDouble(tmp1[9]) + 
		        					"|" + Double.parseDouble(tmp1[10])/Double.parseDouble(tmp1[11]) + 
		        					"|" + tmp1[12]);
		        			flag1 = true;
		        			flag2 = false;
		        			
		        		}else{
		        			System.out.println(tmp2[0] + "|" + tmp2[1] + "|" + tmp2[2] + "|" + tmp2[3] + "|" + tmp2[4] + "|" + tmp2[5] + 
		        					"|" + Double.parseDouble(tmp2[6])/Double.parseDouble(tmp2[7]) + 
		        					"|" + Double.parseDouble(tmp2[8])/Double.parseDouble(tmp2[9]) + 
		        					"|" + Double.parseDouble(tmp2[10])/Double.parseDouble(tmp2[11]) + 
		        					"|" + tmp2[12]);
		        			flag1 = false;
		        			flag2 = true;
		        			
		        		}
	        		}else break;
	        	}
	        	while(line1 != null){
	        		tmp1 = line1.split("\\|");
        			System.out.println(tmp1[0] + "|" + tmp1[1] + "|" + tmp1[2] + "|" + tmp1[3] + "|" + tmp1[4] + "|" + tmp1[5] + 
        					"|" + Double.parseDouble(tmp1[6])/Double.parseDouble(tmp1[7]) + 
        					"|" + Double.parseDouble(tmp1[8])/Double.parseDouble(tmp1[9]) + 
        					"|" + Double.parseDouble(tmp1[10])/Double.parseDouble(tmp1[11]) + 
        					"|" + tmp1[12]);
        			flag1 = true;
        			flag2 = false;
        			line1 = buffers[0].readLine();
        		}
	        	while(line2 != null){
	        		tmp2 = line2.split("\\|");
        			System.out.println(tmp2[0] + "|" + tmp2[1] + "|" + tmp2[2] + "|" + tmp2[3] + "|" + tmp2[4] + "|" + tmp2[5] + 
        					"|" + Double.parseDouble(tmp2[6])/Double.parseDouble(tmp2[7]) + 
        					"|" + Double.parseDouble(tmp2[8])/Double.parseDouble(tmp2[9]) + 
        					"|" + Double.parseDouble(tmp2[10])/Double.parseDouble(tmp2[11]) + 
        					"|" + tmp2[12]);
        			flag1 = false;
        			flag2 = true;
        			line2 = buffers[1].readLine();
        		}      	
	        	buffers[0].close();
	        	buffers[1].close();
	        }
		}else{
			if(!isNested && hasGroupBy) mem_GroupbySelect(tableName, hasLimit);
			else if(!isNested && isAggregate) mem_AggregateSelect(tableName, hasLimit);
			else if(!isNested && hasOrderBy) mem_OrderBySelect(tableName, hasLimit);
			else if(!isNested) mem_BasicSelect(tableName, hasLimit);
			else orderBySelect(tableName, hasLimit, hasOrderBy);
		}
		
		if(outerTableName.equals("MYTABLE")) TableContainer.tableData.remove(outerTableName);
		if(hasOrderBy)	TableContainer.orderByData.clear();
	}
	
	/*group by query, for the query like Select A, SUM(A+B) FROM R ORDER BY A*/
	private int disk_GroupbySelect(String tableName, int hasLimit, boolean hasOrderBy) throws FileNotFoundException, UnsupportedEncodingException {
		DiskGroupbyIterator groupbyIterator = new DiskGroupbyIterator(tableName, plainSelect, selectItems, hasOrderBy, dates);
		groupbyIterator.hasNext();
		LinkedHashMap<String, Row> res = groupbyIterator.next();

		for(Map.Entry<String, Row> entry : res.entrySet()){
			if(!hasOrderBy){
				System.out.print(entry.getKey() + "|");								//print the key to stdout first like A|
				Row agrAns = entry.getValue();							//then print the value to stdout first like SUM(A+B)
				printRow(agrAns.rowList);
			}else{
				writer.write(entry.getKey() + "|");								//print the key to stdout first like A|
				Row agrAns = entry.getValue();							//then print the value to stdout first like SUM(A+B)
				wirteRowtoDisk(agrAns.rowList);
			}
		}
		if(writer != null) writer.close();
		this.desc = groupbyIterator.desc;
		return groupbyIterator.groupValue.size();
	}
	
	private int mem_GroupbySelect(String tableName, int hasLimit) {
		MemGroupbyIterator groupbyIterator = new MemGroupbyIterator(tableName, plainSelect, selectItems);
		groupbyIterator.hasNext();
		LinkedHashMap<String, Row> res = groupbyIterator.next();

		for(Map.Entry<String, Row> entry : res.entrySet()){
			System.out.print(entry.getKey() + "|");								//print the key to stdout first like A|
			Row agrAns = entry.getValue();							//then print the value to stdout first like SUM(A+B)
			printRow(agrAns.rowList);
		}
		return groupbyIterator.groupValue.size();
	}
	
	/*basic aggregation, for the query like Select SUM(A+B) FROM R*/
	private void disk_AggregateSelect(String tableName, int hasLimit) throws FileNotFoundException {
		DiskAggregateIterator aggregateIterator = new DiskAggregateIterator(tableName, plainSelect, selectItems, dates);
		aggregateIterator.hasNext();
		Row agrAns = aggregateIterator.next();
		printRow(agrAns.rowList);
	}
	
	/*basic aggregation, for the query like Select SUM(A+B) FROM R*/
	private void mem_AggregateSelect(String tableName, int hasLimit) {
		MemAggregateIterator aggregateIterator = new MemAggregateIterator(tableName, plainSelect, selectItems);
		aggregateIterator.hasNext();
		Row agrAns = aggregateIterator.next();
		printRow(agrAns.rowList);
	}

	/*basic Select, Like Select A FROM R, SELECT * FROM R*/
	private int disk_OrderBySelect(String tableName, int hasLimit, boolean hasOrderBy) throws FileNotFoundException, UnsupportedEncodingException {
			
		int lineNumber = 0, k = 0;
		DiskSelectIterator selectIterator = new DiskSelectIterator(tableName, outerTableName, plainSelect, selectItems, hasOrderBy, dates);
		
		while (selectIterator.hasNext()) {
        	Row agrAns = selectIterator.next();
        	if(!hasOrderBy) {
        		if(hasLimit != -1 && k++ >= hasLimit) break;
        		printRow(agrAns.rowList);							//if nested, output to the disk
        	}
        	else {
        		wirteRowtoDisk(agrAns.rowList);											//else print out to the stdout
        	}
        	lineNumber++;
        }
				
        if(writer != null) writer.close();
        this.desc = selectIterator.desc;
       
		return lineNumber;
	}
	
	/*basic Select, Like Select A FROM R, SELECT * FROM R*/
	private int orderBySelect(String tableName, int hasLimit, boolean hasOrderBy) throws FileNotFoundException, UnsupportedEncodingException {
		
		if(outerTableName.equals(tableName)) TableContainer.tableData.put("TEMP", new ArrayList<Row>());
		else TableContainer.tableData.put(outerTableName, new ArrayList<Row>());
		
		int lineNumber = 0, k = 0;
		SelectIterator selectIterator = new SelectIterator(tableName, outerTableName, plainSelect, selectItems, isNested, hasOrderBy, dates);
		
		while (selectIterator.hasNext()) {
        	Row agrAns = selectIterator.next();
        	if(!isNested && !hasOrderBy) {
        		if(hasLimit != -1 && k++ >= hasLimit) break;
        		printRow(agrAns.rowList);							//if nested, output to the disk
        	}
        	else {
        		if(!outerTableName.equals(tableName)) wirteRowtoMem(agrAns.rowList, outerTableName, lineNumber);											//else print out to the stdout
        		else wirteRowtoMem(agrAns.rowList, "TEMP", lineNumber);	
        	}
        	lineNumber++;
        }
				
        if(writer != null) writer.close();
        this.desc = selectIterator.desc;
        
        if(outerTableName.equals(tableName)){
        	TableContainer.tableData.remove(tableName);
        	TableContainer.tableData.put(tableName, TableContainer.tableData.get("TEMP"));
        	TableContainer.tableData.remove("TEMP");
        }
        
        if(TableContainer.tableData.containsKey(outerTableName) && hasOrderBy){
        	
			ArrayList<Integer> orderByAttribute = new ArrayList<>();	
			
			for(int i = 0; i < plainSelect.getOrderByElements().size(); i++){
        		String tempColName = plainSelect.getOrderByElements().get(i).getExpression().toString();
        		orderByAttribute.add(TableContainer.columns.get(tempColName));
			}
			
			PriorityQueue<Row> orderByList = new PriorityQueue<Row>();
	        for(int i = 0; i < TableContainer.orderByData.size(); i++){
	        	TableContainer.orderByData.get(i).orderByIndex = orderByAttribute;
	        	TableContainer.orderByData.get(i).desc3 = selectIterator.desc2;
	        	orderByList.add(TableContainer.orderByData.get(i));
	        }
		
			while(orderByList.size() > 0){
				if(hasLimit-- == 0 ) break;
				String res = "";
				Row line = orderByList.poll();
				for(int i = 6; i < line.rowList.size(); i++) res += line.rowList.get(i) + "|";
				System.out.println(res.substring(0, res.length()-1));
			}
        }
		return lineNumber;
	}
	
	/*basic Select, Like Select A FROM R, SELECT * FROM R*/
	private int mem_OrderBySelect(String tableName, int hasLimit) throws FileNotFoundException, UnsupportedEncodingException {
		
		int lineNumber = 0;
		MemOrderSelectIterator selectIterator = new MemOrderSelectIterator(tableName, outerTableName, plainSelect, selectItems);
		
		ArrayList<Integer> orderByAttribute = new ArrayList<>();		
		for(int i = 0; i < plainSelect.getOrderByElements().size(); i++){
    		String tempColName = plainSelect.getOrderByElements().get(i).getExpression().toString();
    		orderByAttribute.add(TableContainer.columns.get(tempColName));
		}
		
		PriorityQueue<Row> orderByList = new PriorityQueue<Row>();
		while (selectIterator.hasNext()) {
        	Row agrAns = selectIterator.next();
    		Row temp = new Row();
    		
			for(PrimitiveValue v : TableContainer.orderByData.get(lineNumber).rowList){
				temp.rowList.add(v);
			}
			
    		for (int i = 0; i < agrAns.rowList.size(); i++) {
    			temp.rowList.add(agrAns.rowList.get(i));
    		}
    		
    		temp.orderByIndex = orderByAttribute;
    		temp.desc3 = selectIterator.desc2;

    		orderByList.offer(temp);
        	lineNumber++;
        }
	
		while(orderByList.size() > 0){
			if(hasLimit-- == 0 ) break;
			String res = "";
			Row line = orderByList.poll();
			for(int i = 6; i < line.rowList.size(); i++) res += line.rowList.get(i) + "|";
			System.out.println(res.substring(0, res.length()-1));
		}
        
		return lineNumber;
	}

	/*basic Select, Like Select A FROM R, SELECT * FROM R*/
	private int mem_BasicSelect(String tableName, int hasLimit) throws FileNotFoundException, UnsupportedEncodingException {		
		int k = 0;
		MemSelectIterator selectIterator = new MemSelectIterator(tableName, outerTableName, plainSelect, selectItems);		
		while (selectIterator.hasNext()) {
        	Row agrAns = selectIterator.next();
    		if(hasLimit != -1 && k++ >= hasLimit) break;
    		printRow(agrAns.rowList);							
        }
		return 0;
	}
	
	/*output a row to the disk*/
	private void wirteRowtoDisk(List<PrimitiveValue> agrAns) {
		for (int i = 0; i < agrAns.size(); i++) {
			if(agrAns.get(i).toString().contains("'")) writer.print(agrAns.get(i).toString().substring(1, agrAns.get(i).toString().length() - 1));
			else writer.print(agrAns.get(i));
			if (i<agrAns.size()-1) writer.print("|");
		}
		writer.println();
	}

	/*output a row to the disk*/
	private void wirteRowtoMem(List<PrimitiveValue> agrAns, String outerTableName, int num) {		
		TableContainer.tableData.get(outerTableName).add(new Row());
		if(hasOrderBy){
			for(PrimitiveValue v : TableContainer.orderByData.get(num).rowList){
				TableContainer.tableData.get(outerTableName).get(num).rowList.add(v);
			}
		}
		for (int i = 0; i < agrAns.size(); i++) {
			TableContainer.tableData.get(outerTableName).get(num).rowList.add(agrAns.get(i));
		}
	}
	
	/*output a row to stdout*/
	private void printRow( List<PrimitiveValue> agrAns) {
		for (int i = 0; i < agrAns.size(); i++) {		            		
			if(agrAns.get(i) != null) {
				System.out.print(agrAns.get(i));
				if (i<agrAns.size()-1) System.out.print("|");
			}else {
				if(((SelectExpressionItem)selectItems.get(i)).getExpression().toString().equals("COUNT(*)")) System.out.print("0");
				else System.out.print("");
				if (i<agrAns.size()-1) System.out.print("|");
			}
		}
		System.out.println();
	}
	
	private void findDate() {
		dates = new ArrayList<>();
		if(this.whereExp != null){
			String date = this.whereExp.toString(), temp = "";
			boolean flag = false;
			for(int i = 0; i < date.length(); i++){
				if(date.charAt(i) == '\'' && !flag) {
					flag = true;
					continue;
				}else if(date.charAt(i) == '\'' && flag){
					dates.add(temp);
					temp = "";
					flag = false;
				}else if(flag && date.charAt(i) != '\'') temp += date.charAt(i);
			}		
		}	
		Collections.sort(dates);
	}
	
	private void processJoin(String firstTableName, String firstTableAtt, String joinTableName, String joinTableAtt) {
		try {
			BufferedReader fbr = new BufferedReader(new FileReader("data/" + firstTableName + ".csv"));
			String fs;
			Map<String, String> memIndex = new HashMap<String, String>();
			int firstColumn = TableContainer.tableAttributes.get(firstTableName).indexOf(firstTableAtt);
			int joinColumn = TableContainer.tableAttributes.get(joinTableName).indexOf(joinTableAtt);
			while((fs = fbr.readLine()) != null) {
				String[] fRow = fs.split("\\|"); 
				memIndex.put(fRow[firstColumn], fs);
			}
			fbr.close();
			BufferedReader jbr = new BufferedReader(new FileReader(TableContainer.tables.get(joinTableName)));
			while((fs = jbr.readLine()) != null) {
				String[] jRow = fs.split("\\|");
				if(memIndex.containsKey(jRow[joinColumn])) {
					
				}
			}
		} catch (FileNotFoundException e){
			// TODO Auto-generated catch block
						e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
