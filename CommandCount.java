package assignment3;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class CommandCount {
	
	private static int[] timesPer15Mins = new int[11];
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception{
		SparkConf sparkConf = new SparkConf().setAppName("CommandCount").setMaster("local");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		//Splits the file into lines
		JavaRDD<String> lines = ctx.textFile(args[0], 1);
		
		JavaRDD<String> words = lines.map(new Function<String, String>() {
			//takes in a string and outputs a string
			@Override
			public String call(String s) throws Exception {
				//System.out.println("2");
				String str = s.replace("<", "");
				str=str.replace(">","");
				str=str.replace(">/", "");
				str = str.replace("</Command>","");
				str=str.replace("=", " ");
				str=str.replace("\"", "");
				
				String beg="",end="";
				String[] temp = str.split(" ",4);
				if(temp.length==4){
					beg = temp[2];
					end=temp[3];
				}
				if(!beg.matches("Command"))
					return "";
				return end;
			}
		});
	
		//gets the timestamp from the line
		JavaRDD<Integer> getTimestamp = words.map(new Function<String, Integer>(){

			@Override
			public Integer call(String line) throws Exception {
				
				//System.out.println("3");
				String[] chopped_line = line.split(" ");
				int timestamp=0;
				for(int i=0;i<chopped_line.length;i++){
					if(chopped_line[i].matches("timestamp")){
						timestamp= Integer.parseInt(chopped_line[i+1]);
						break;}
				}
				return timestamp;
			}
		});	
	
		int addToArray = getTimestamp.reduce(new Function2<Integer, Integer, Integer>(){
			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				if(b==0)
					return 0;
				int index=b/900000;
				timesPer15Mins[index]++;
				return 0;
			}
		});
		
		File file = new File(args[1]);
		BufferedWriter outputBW = new BufferedWriter(new FileWriter(file));
		for(int i=0;i<11;i++)
			outputBW.write(Integer.toString(i) + '\t' + Integer.toString(timesPer15Mins[i])+'\n');
		outputBW.close();
		ctx.stop();
		ctx.close();
	}
}
