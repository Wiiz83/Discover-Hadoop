package TP6_1;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends  Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context sortie) throws IOException, InterruptedException 
	{
		
		Iterator<IntWritable> it = values.iterator();
		int nb = 0;
		// pour chaque valeur associée à la clé faire : {"Hello",{1}, {1}}
		while (it.hasNext()) 
		{
			nb+=it.next().get();
		}
		sortie.write(key, new IntWritable(nb));

	}
}
 