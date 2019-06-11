package q5;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class WordCountReducer extends  Reducer<Text, Text, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<Text> values, Context sortie) throws IOException, InterruptedException 
	{
		
		Iterator<Text> it = values.iterator();
		int nb = 0;
		double somme = 0;
		// pour chaque valeur associée à la clé faire : {"Hello",{1}, {1}}
		double maximumall=-260;
		double minimumall=1000;
		while (it.hasNext()) 
		{
			String value=it.next().toString();
			Double maximum=Double.parseDouble(value.split(" ")[0]);
			Double minimum=Double.parseDouble(value.split(" ")[1]);

			if(maximum>maximumall){
				maximumall=maximum;
			}
			if(minimum<minimumall){
				minimumall=minimum;
			}
			
		}

		sortie.write(new Text("Maximum : "),new DoubleWritable(maximumall));
		sortie.write(new Text("Minimum : "),new DoubleWritable(minimumall));

	}
}
 