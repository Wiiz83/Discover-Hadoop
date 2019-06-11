package snippet;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends  Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<DoubleWritable> values, Context sortie) throws IOException, InterruptedException 
	{
		
		Iterator<DoubleWritable> it = values.iterator();
		int nb = 0;
		double somme = 0;
		// pour chaque valeur associée à la clé faire : {"Hello",{1}, {1}}
		while (it.hasNext()) 
		{
			nb+=1;
			somme+=it.next().get();
		}

		sortie.write(key, new DoubleWritable(somme/nb));

	}
}
 