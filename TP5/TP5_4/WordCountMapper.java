package TP5_4;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;


public class WordCountMapper extends Mapper<LongWritable,Text, Text, Text> {
	
	//private Logger logger = Logger.getLogger(WordCountMapper.class);

	public void map(LongWritable cle, Text valeur, Context sortie) throws IOException, InterruptedException {
		if((cle.get() > 0) && (valeur.toString().length() > 0)){
			
				String line = valeur.toString();
				if(line.split("\t").length>1){
					String id = line.split("\t")[0];
					String listeEquipement = line.split("\t")[1];		
					sortie.write(new Text(id), new Text(listeEquipement));			
				}
		}
	}
}