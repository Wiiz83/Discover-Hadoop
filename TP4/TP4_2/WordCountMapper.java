package TP4_2;
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
				String region = line.split(";")[1];
				String typeEquipement = line.split(";")[9];
				sortie.write(new Text(region+"#"+typeEquipement), new IntWritable(1));
			} 
			catch (Exception ee) {
				throw new RuntimeException(ee.getMessage());
			}
		}
	}
}