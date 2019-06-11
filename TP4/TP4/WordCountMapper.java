package TP4;
import java.util.StringTokenizer;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class WordCountMapper extends Mapper<LongWritable,Text, Text, IntWritable> {

	public void map(LongWritable cle, Text valeur, Context sortie) throws IOException {
		if((cle.get() > 0) && (valeur.toString().length() > 0)){
			try {			
				String line = valeur.toString();
				String value = line.split(";")[3];
				sortie.write(new Text(value), new IntWritable(1));
			} 
			catch (Exception ee) {
				throw new RuntimeException(ee.getMessage());
			}
		}
	}
}