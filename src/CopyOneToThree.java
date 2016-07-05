import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CopyOneToThree {

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
		//int reducenum=Integer.valueOf(args[2]);
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = new Job(conf, "Copy Data one To three");
		job.setJarByClass(CopyOneToThree.class);
		job.setMapperClass(CopyMapper.class);
		job.setNumReduceTasks(20);
		job.setReducerClass(CopyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
