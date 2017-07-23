package dubstep;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.lang.Comparable;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;

public class Row implements Comparable<Row>{
	ArrayList<Integer> orderByIndex = new ArrayList<Integer>();;
	ArrayList<Integer> desc3;
	List<PrimitiveValue> rowList = new ArrayList<PrimitiveValue>();;
	
	public Eval eval = new Eval(){
		@Override
		public PrimitiveValue eval(Column c) {
			return rowList.get(TableContainer.columns.get(c.toString()));
		}
	};
	
	public String toString(){
		return rowList.toString();
		
	}

	@Override
	public int compareTo(Row o) {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		for(int i = 0; i < orderByIndex.size(); i++){
			int index = orderByIndex.get(i);
			if(this.rowList.get(index) == (o.rowList.get(index))){
				continue;
			}else{
				try {
				if(!desc3.contains(index)) return eval.eval(new GreaterThan(this.rowList.get(index),o.rowList.get(index))).toBool() == true ? 1 : -1;
				else return eval.eval(new GreaterThan(this.rowList.get(index),o.rowList.get(index))).toBool() == true ? -1 : 1;
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return 0;
	}
}
