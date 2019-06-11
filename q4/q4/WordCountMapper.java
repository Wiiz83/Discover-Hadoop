package q4;
import java.util.StringTokenizer;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class WordCountMapper extends Mapper<LongWritable,Text, Text, Text> {

	public void map(LongWritable cle, Text valeur, Context sortie) throws IOException {
		if((cle.get() > 0) && (valeur.toString().length() > 0)){
			try {			
				String line = valeur.toString();
				StringTokenizer itr = new StringTokenizer(line, "\t");
				String region=itr.nextToken().trim();
				double somme=0;
				int cmpt=0;
				double minimum=1000;
				double maximum=-260;
				while(itr.hasMoreTokens()){
					String value2=itr.nextToken().trim();
					if(value2 != null && !value2.toString().equals("") && !value2.toString().equals(" ")){
						double value = Double.parseDouble(value2);
						somme+=value;
						cmpt++;
						if(value<minimum){
							minimum=value;
						}
						if(value>maximum){
							maximum=value;
						}
					}
				}

				sortie.write(new Text("TotalMoyenne"), new Text(somme+" "+cmpt+" "+maximum+" "+minimum));
			} 
			catch (Exception ee) {
				throw new RuntimeException(ee.getMessage());
			}
		}
	}
}
