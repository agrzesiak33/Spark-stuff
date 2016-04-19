package assignment3;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class AverageTime {
	
	private static int numCommands=0;
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception{
		SparkConf sparkConf = new SparkConf().setAppName("AverageTime").setMaster("local");
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
		
		JavaRDD<Integer> times = words.map(new Function<String, Integer>(){

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
		
		@SuppressWarnings("unused")
		int lowest = times.reduce(new Function2<Integer,Integer,Integer>(){
			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				int aa=a;
				int bb=b;
				if(a==0)
					return b;
				if(b==0)
					return a;
				return (a<b) ? a:b;
			}
		});
		
		int highest = times.reduce(new Function2<Integer, Integer, Integer>(){
			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				if(b!=0)
					numCommands++;
				if(a==0)
					return b;
				if(b==0)
					return a;
				return (a<b) ? b:a;
			}
		});
		
		File file = new File(args[1]);
		BufferedWriter output = new BufferedWriter(new FileWriter(file));
		output.write(Integer.toString((highest-lowest)/numCommands));
		output.close();
		ctx.stop();
		ctx.close();
	}
}