package TP4_3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class WordCombiner extends Reducer<Text, Text, Text, Text> {
	
	//private Logger logger = Logger.getLogger(WordCombiner.class);

	public void reduce(Text key, Iterable<Text> values, Context sortie)
			throws IOException, InterruptedException {

		try {
			Iterator<Text> it = values.iterator();
			double distance = 0;
			double cpt = 0;
			
			while (it.hasNext()) {
				cpt++;
				distance += Double.parseDouble(it.next().toString()); // on cumule la distance
			}
			//logger.info("Distance = " + distance + " Compteur = " + cpt + " Region = " + key);
			sortie.write(new Text(key), new Text(distance + "#" + cpt));
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
	}
}