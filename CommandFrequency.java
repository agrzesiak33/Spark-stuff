package assignment3;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class CommandFrequency {
	
	private static String[][] numCommands = new String[24][2];
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception{
		for(int i=0;i<numCommands.length;i++){
			numCommands[i][0]="none";
			numCommands[i][1]="0";
		}
		
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
		
		JavaRDD<String> getType = words.map(new Function<String,String>(){

			@Override
			public String call(String line) throws Exception {
				String[] chopped_line = line.split(" ");
				String type="none";
				for(int i = 0; i<chopped_line.length;i++){
					if(chopped_line[i].matches("_type")){
						type=chopped_line[i+1];
						if(type.matches("EclipseCommand"))
							type=type+": "+chopped_line[i+3];
						break;}
				}
				return type;
			}
		});
		
		String addToArray = getType.reduce(new Function2<String,String,String>(){

			@Override
			public String call(String a, String b) throws Exception {
				if(b=="none")
					return"";
				for(int i =0;i<numCommands.length;i++){
					if(numCommands[i][0].matches(b)){
						int prevNum = Integer.parseInt(numCommands[i][1]);
						prevNum++;
						numCommands[i][1]=Integer.toString(prevNum);
						return "";
					}
					else if(numCommands[i][0].matches("none")){
						numCommands[i][0]=b;
						numCommands[i][1]="1";
						return "";
					}
				}
				return "";
			}
		});
		File file = new File(args[1]);
		BufferedWriter output = new BufferedWriter(new FileWriter(file));
		for(int i=0;i<numCommands.length;i++)
			output.write(numCommands[i][0]+'\t' + numCommands[i][1]+'\n');
		output.close();
		ctx.stop();
		ctx.close();
		
	}
}









