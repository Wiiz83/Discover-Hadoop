package TP4_3;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;


public class WordCount 
{

  @SuppressWarnings("deprecation")
public static void main(String[] args) throws Exception 
  {
    Job job = new Job();
    
    job.setJobName("Word Count");
    
    job.setJarByClass(TP4_3.WordCount.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(TP4_3.WordCountMapper.class);        
    job.setReducerClass(TP4_3.WordCountReducer.class);
    job.setCombinerClass(TP4_3.WordCombiner.class);

    FileInputFormat.setInputPaths(job, args[0]);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    FileSystem fs = FileSystem.get(job.getConfiguration());
    fs.delete(new Path(args[1]));
				
    job.waitForCompletion(true);
  }
}
