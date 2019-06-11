package q3;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends  Reducer<Text, Text, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<Text> values, Context sortie) throws IOException, InterruptedException 
	{
		
		Iterator<Text> it = values.iterator();
		int nb = 0;
		double somme = 0;
		// pour chaque valeur associée à la clé faire : {"Hello",{1}, {1}}
		while (it.hasNext()) 
		{
			String value=it.next().toString();
			Double moyenne=Double.parseDouble(value.split(" ")[0]);
			int coefficient=Integer.parseInt(value.split(" ")[1]);
			nb+=coefficient;
			somme+=moyenne;
		}

		sortie.write(key, new DoubleWritable(somme/nb));

	}
}
 