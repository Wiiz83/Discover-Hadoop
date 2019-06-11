package TP5_3;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;


public class WordCount {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
	    conf.set("nomFichier-1", args[2]);
	    conf.set("colonneID-1", args[3]);
	    conf.set("listeDesColonnesProjetees-1", args[4]);
	    conf.set("nomFichier-2", args[5]);
	    conf.set("colonneID-2", args[6]);
	    conf.set("listeDesColonnesProjetees-2", args[7]);
	    conf.set("ordre", args[8]);
	    conf.set("separateur", args[9]);
	    Job job = new Job(conf);
	      
	    job.setJobName("Word Count");
	    
	    job.setJarByClass(TP5_3.WordCount.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	
	    job.setMapperClass(TP5_3.WordCountMapper.class);        
	    job.setReducerClass(TP5_3.WordCountReducer.class);
	
	    FileInputFormat.setInputPaths(job, args[0]);
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	    FileSystem fs = FileSystem.get(job.getConfiguration());
	    fs.delete(new Path(args[1]));
					
	    job.waitForCompletion(true);
	}
	
}
