import java.util.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;


public class equijoin {

	public static class mapperClass extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key,Text eachRow, Context context) throws IOException, InterruptedException {
			String[] elems = eachRow.toString().split(",");
			Text _Key = new Text(elems[1]);
			context.write(_Key, eachRow);
		}
	}

	public static class reducerClass extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException{
			Iterator<Text> iterate = values.iterator();
			ArrayList<String> table1 = new ArrayList<String>();
			ArrayList<String> table2 = new ArrayList<String>();
			
			String T1="";

			boolean flag = true;
			while(iterate.hasNext()){
                String _Elem = iterate.next().toString();
				String eachElemRow[] = _Elem.split(",");
				if (flag == true)
				{
					T1 = eachElemRow[0];
					flag = false;
				}
				if(T1.equals(eachElemRow[0])){
					table1.add(_Elem);
				   }
				else
				{
					table2.add(_Elem);
				   }
			}
			for(int i=0; i<table1.size(); i++){
				for(int j=0;j<table2.size();j++){
					String finalResultEachKey = table1.get(i) + "," + " " + table2.get(j);
					context.write(null, new Text(finalResultEachKey));	        		
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Code for Equijoin");	
		job.setJarByClass(equijoin.class);	
		job.setMapperClass(mapperClass.class);
		job.setReducerClass(reducerClass.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
