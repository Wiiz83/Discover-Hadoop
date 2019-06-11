package TP5_4;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;


public class WordCount 
{

  @SuppressWarnings("deprecation")
public static void main(String[] args) throws Exception 
  {
    Configuration conf = new Configuration();
    conf.set("nomFichier-1", args[3]);
    conf.set("colonneID-1", args[4]);
    conf.set("listeDesColonnesProjetees-1", args[5]);
    conf.set("nomFichier-2", args[6]);
    conf.set("colonneID-2", args[7]);
    conf.set("listeDesColonnesProjetees-2", args[8]);
    conf.set("ordre", args[9]);
    conf.set("separateur", args[10]);
    Job job = new Job();
    
    job.setJobName("Equipement");
    
    job.setJarByClass(TP5_4.WordCount.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(TP5_4.WordCountMapper.class);        
    job.setReducerClass(TP5_4.WordCountReducer.class);
    
    /* PATH ETAPE 2 */
    FileInputFormat.setInputPaths(job, new Path(args[1])); // output1
    FileOutputFormat.setOutputPath(job, new Path(args[2])); // output2

    FileSystem fs = FileSystem.get(job.getConfiguration());
    fs.delete(new Path(args[2]));
   
    
    Job jobActivite = new Job(conf);
    jobActivite.setJobName("Activite");
    
    jobActivite.setJarByClass(TP5_4.WordCount.class);
    
    jobActivite.setMapOutputKeyClass(Text.class);
    jobActivite.setMapOutputValueClass(Text.class);
    
    jobActivite.setOutputKeyClass(Text.class);
    jobActivite.setOutputValueClass(Text.class);

    jobActivite.setMapperClass(TP5_4.WordCountMapper2.class);        
    jobActivite.setReducerClass(TP5_4.WordCountReducer2.class);

    /* PATH ETAPE 1 */
    FileInputFormat.setInputPaths(jobActivite, args[0]); // TP5
    FileOutputFormat.setOutputPath(jobActivite, new Path(args[1])); // output1

    FileSystem fs2 = FileSystem.get(jobActivite.getConfiguration());
    fs2.delete(new Path(args[1]));
    

    jobActivite.waitForCompletion(true);
    job.waitForCompletion(true);
  }
}
