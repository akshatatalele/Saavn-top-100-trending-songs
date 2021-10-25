package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * This is a Driver class of Map reduce program.
 */
public class SaavnDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int returnStatus = ToolRunner.run(new Configuration(), new SaavnDriver(), args);
		System.exit(returnStatus);
	}

	public int run(String[] args) throws Exception {
//		@SuppressWarnings("deprecation")
//		Job job = new Job(getConf());
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "saavn");
//		job.setJobName("Log Partitioner");
		
		//Specify driver class
		job.setJarByClass(SaavnDriver.class);

		//Specify MR output key and value type
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//Specify Map pahse output key and value type
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		//Specify Mapper class
		job.setMapperClass(SaavnMapper.class);
		
		//Specify Reducer class
		job.setReducerClass(SaavnReducer.class);
		
		//Specify Partitioner class
		job.setPartitionerClass(SaavnPartitioner.class);
		
		//Specify number of reducer task
		job.setNumReduceTasks(7);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		try {
			return job.waitForCompletion(true) ? 0 : 1;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

}
