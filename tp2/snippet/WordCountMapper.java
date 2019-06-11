package snippet;
import java.util.StringTokenizer;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable,Text, Text,DoubleWritable> {

	public void map(LongWritable cle, Text valeur, Context sortie) throws IOException {
		if((cle.get() > 0) && (valeur.toString().length() > 0)){
			Text clef = new Text();
			try {			
				String line = valeur.toString();
				StringTokenizer itr = new StringTokenizer(line, "\t");
				String region=itr.nextToken().trim();
				double somme=0;
				while(itr.hasMoreTokens()){
					somme+=Double.parseDouble(itr.nextToken().trim());
				}
				sortie.write(new Text(region), new DoubleWritable(somme));
			} 
			catch (Exception ee) {
				throw new RuntimeException(ee.getMessage());
			}
		}
	}
}
