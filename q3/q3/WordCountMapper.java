package q3;
import java.util.StringTokenizer;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable,Text, Text, Text> {

	public void map(LongWritable cle, Text valeur, Context sortie) throws IOException {
		if((cle.get() > 0) && (valeur.toString().length() > 0)){
			try {			
				String line = valeur.toString();
				StringTokenizer itr = new StringTokenizer(line, "\t");
				String region=itr.nextToken().trim();
				double somme=0;
				int cmpt=0;
				while(itr.hasMoreTokens()){
					String value2=itr.nextToken().trim();
					if(value2 != null && !value2.toString().equals("") && !value2.toString().equals(" ")){
						System.out.println("value2 = " +value2);
						double value = Double.parseDouble(value2);
						somme+=value;
						cmpt++;
					}
				}

				sortie.write(new Text(region), new Text(somme+" "+cmpt));
			} 
			catch (Exception ee) {
				throw new RuntimeException(ee.getMessage());
			}
		}
	}
}
