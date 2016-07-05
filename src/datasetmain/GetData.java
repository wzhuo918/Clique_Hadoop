 package datasetmain;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import degreecomplete.DegreeMapperComplete;
import degreecomplete.DegreeReducerComplete;
import degreepartial.DegreeMapperPartial;
import degreepartial.DegreeReducerPartial;

import pairtype.PairType;
import trianglecandidate.TriangleMapperCandidate;
import trianglecandidate.TriangleReducerCandidate;
import triangleresult.TriangleMapperResult;
import triangleresult.TriangleReducerResult;


public class GetData {
	
	public static class CopyMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String str = value.toString();
			String[] s = str.split("\t");
			if(s.length == 2){
				String[] str1 = s[1].split(",");
				if(str1.length == 2){
					String one = s[1];
					String two = s[0] +"," + str1[1];
					String three = s[0] +"," + str1[0];
					
					context.write(new Text(s[0]), new Text(one));
					context.write(new Text(str1[0]), new Text(two));
					context.write(new Text(str1[1]), new Text(three));
				}
			}
		}
	}

	public static class CopyReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
			for (Text val : values) {
				context.write(new Text(key), val);
			}
			
		}
	}
	
	
	

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String in=args[0];
		String pre=args[1];
		int reducenum=Integer.valueOf(args[2]);
		
		Job job1 = new Job(conf,"degree count one");
		job1.setNumReduceTasks(reducenum);
		job1.setJarByClass(GetData.class);
		job1.setMapperClass(DegreeMapperPartial.class);
		job1.setReducerClass(DegreeReducerPartial.class);
		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(PairType.class);
		job1.setOutputKeyClass(PairType.class);
		job1.setOutputValueClass(PairType.class);
		FileInputFormat.setInputPaths(job1, new Path(in));
		FileOutputFormat.setOutputPath(job1, new Path("TempTriangle/"+pre+"1"));
		
		Job job2 = new Job(conf,"degree count two");
		job2.setNumReduceTasks(reducenum);
		job2.setJarByClass(GetData.class);
		job2.setMapperClass(DegreeMapperComplete.class);
		job2.setReducerClass(DegreeReducerComplete.class);	
		job2.setMapOutputKeyClass(PairType.class);
		job2.setMapOutputValueClass(PairType.class);
		job2.setOutputKeyClass(PairType.class);
		job2.setOutputValueClass(PairType.class);
		FileInputFormat.setInputPaths(job2, new Path("TempTriangle/"+pre+"1"));
		FileOutputFormat.setOutputPath(job2, new Path("TempTriangle/"+pre+"2"));

			
		Job job3 = new Job(conf,"triangle one");
		job3.setNumReduceTasks(reducenum);
		job3.setJarByClass(GetData.class);
		FileInputFormat.setInputPaths(job3, new Path("TempTriangle/"+pre+"2"));
		FileOutputFormat.setOutputPath(job3, new Path("TempTriangle/"+pre+"3"));
		job3.setMapperClass(TriangleMapperCandidate.class);
		job3.setReducerClass(TriangleReducerCandidate.class);
		job3.setMapOutputKeyClass(LongWritable.class);
		job3.setMapOutputValueClass(PairType.class);
		job3.setOutputKeyClass(PairType.class);
		job3.setOutputValueClass(LongWritable.class);
		
		Job job4 = new Job(conf,"triangle two");	
		job4.setNumReduceTasks(reducenum);
		job4.setJarByClass(GetData.class);
		FileInputFormat.addInputPath(job4, new Path("TempTriangle/"+pre+"2"));
		FileInputFormat.addInputPath(job4, new Path("TempTriangle/"+pre+"3"));
		FileOutputFormat.setOutputPath(job4, new Path("Triangledata/" +pre+"_TriData"));
		job4.setMapperClass(TriangleMapperResult.class);
		job4.setReducerClass(TriangleReducerResult.class);		
		job4.setMapOutputKeyClass(PairType.class);
		job4.setMapOutputValueClass(Text.class);
		job4.setOutputKeyClass(LongWritable.class);
		job4.setOutputValueClass(PairType.class);
		
		
		//job
		Job job5 = new Job(conf, "Copy Data one To three");
		job5.setNumReduceTasks(reducenum);
		job5.setJarByClass(GetData.class);
		FileInputFormat.addInputPath(job5, new Path("Triangledata/" +pre+"_TriData"));
		FileOutputFormat.setOutputPath(job5, new Path("CliqueInputdata/"+pre));
		job5.setMapperClass(CopyMapper.class);
		job5.setReducerClass(CopyReducer.class);
		job5.setMapOutputKeyClass(Text.class);
		job5.setMapOutputValueClass(Text.class);
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(Text.class);

		
		
		
		long t1  = System.currentTimeMillis();
		job1.waitForCompletion(true);
		long t2 = System.currentTimeMillis();
		
		job2.waitForCompletion(true);	
		long t3 = System.currentTimeMillis();
	
		job3.waitForCompletion(true);
		long t4 = System.currentTimeMillis();
	
		job4.waitForCompletion(true);	
		long t5 = System.currentTimeMillis();
		
		job5.waitForCompletion(true);	
		long t6 = System.currentTimeMillis();

		
//		
//		System.out.println("degree count one: "+(t2-t1));
//		System.out.println("degree count two: "+(t3-t2));
//		System.out.println("triangle one: "+(t4-t3));
//		System.out.println("triangle two: "+(t5-t4));
		
	}

}
