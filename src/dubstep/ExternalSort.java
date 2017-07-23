package dubstep;


 
import java.util.*;
import java.io.*;
 
public class ExternalSort {
	
	ArrayList<Integer> desc;
	
	ExternalSort(ArrayList<Integer> desc){
		this.desc = desc;
	}
	
	PriorityQueue<String> pq = new PriorityQueue<>(new Comparator<String>(){
		@Override
		public int compare(String o1, String o2) {
			// TODO Auto-generated method stub
			String[] tmp1 = o1.split("\\|");
			String[] tmp2 = o2.split("\\|");
			
			for(int i = 0; i < tmp1.length-1; i++){
				if(tmp1[i].matches("[0-9]+")){
					if(!tmp1[i].equals(tmp2[i])){
						if(!desc.contains(i)) return Integer.parseInt(tmp1[i]) - Integer.parseInt(tmp2[i]);
						else return Integer.parseInt(tmp2[i]) - Integer.parseInt(tmp1[i]);
					}else continue;
				}
				else{
					if(!tmp1[i].equals(tmp2[i])){
						if(!desc.contains(i)) return tmp1[i].compareTo(tmp2[i]);
						else return tmp2[i].compareTo(tmp1[i]);
					}else continue;
				}
			}
			return 0;
		}
	});
	
	public void exSort(String input, int run_size, int chunkNumber) throws IOException{
		initialSetting(input, run_size, chunkNumber);
		mergeFiles(input, chunkNumber);
	}

	private void mergeFiles(String outputFile, int chunkNumber) throws IOException {
		// TODO Auto-generated method stub
		File[] inFile = new File[chunkNumber];
		String tmpFile = "data/temp";
		BufferedReader[] buffer = new BufferedReader[chunkNumber];
		for(int i = 0; i < chunkNumber; i++) inFile[i] = new File(tmpFile+i);
		
		File outFile = new File(outputFile);
		PrintWriter writer2 = new PrintWriter(outFile);
		
		for(int i = 0; i < chunkNumber; i++){
			buffer[i] = new BufferedReader(new FileReader(inFile[i]));
			String line;		
			if((line = buffer[i].readLine()) == null) continue;
			pq.add(line+"#"+i);
		}
		
		while(pq.size() > 0){
			String cur = pq.poll();
			String[] tmp = cur.split("#");
			writer2.write(tmp[0] + "\n");
			
			String line;
			if((line = buffer[Integer.parseInt(tmp[1])].readLine()) == null) continue;
			pq.add(line+"#"+tmp[1]);
		}
		
		for(int j = 0; j < chunkNumber; j++) {
			inFile[j].delete();
			buffer[j].close();
		}
		writer2.close();
	}

	private void initialSetting(String inputFile, int chunkSize, int chunkNumber) throws IOException {
		// TODO Auto-generated method stub
		File inFile = new File(inputFile);
		if(!inFile.exists()) inFile.createNewFile();
		File[] outFile = new File[chunkNumber];
		BufferedReader buffer = new BufferedReader(new FileReader(inFile));
		String tmpFile = "data/temp";
		FileWriter writer2 = null;
		
		for(int i = 0; i < chunkNumber; i++){
			outFile[i] = new File(tmpFile+i);
			outFile[i].createNewFile();
		}
		
		boolean hasMore = true;
		int res = 0;
				
		int i;
		while(hasMore){
			ArrayList<String> arr = new ArrayList<String>();
			String line;
			for(i = 0; i < chunkSize; i++){
				if((line = buffer.readLine()) != null){
					arr.add(line);
				}else{
					hasMore = false;
					break;
				}
			}
			
			Collections.sort(arr, new Comparator<String>(){
				@Override
				public int compare(String o1, String o2) {
					// TODO Auto-generated method stub
					String[] tmp1 = o1.split("\\|");
					String[] tmp2 = o2.split("\\|");
					
					for(int i = 0; i < tmp1.length-1; i++){
						if(tmp1[i].matches("[0-9]+")){
							if(!tmp1[i].equals(tmp2[i])){
								if(!desc.contains(i)) return Integer.parseInt(tmp1[i]) - Integer.parseInt(tmp2[i]);
								else return Integer.parseInt(tmp2[i]) - Integer.parseInt(tmp1[i]);
							}else continue;
						}
						else{
							if(!tmp1[i].equals(tmp2[i])){
								if(!desc.contains(i)) return tmp1[i].compareTo(tmp2[i]);
								else return tmp2[i].compareTo(tmp1[i]);
							}else continue;
						}
					}
					return 0;
				}
			});
			
			writer2 = new FileWriter(outFile[res], true);
			for(int j = 0; j < i; j++){
				writer2.write(arr.get(j)+"\n");
			}
			
			writer2.close();
			res++;
		}
		
		buffer.close();;
	}
	
	
}