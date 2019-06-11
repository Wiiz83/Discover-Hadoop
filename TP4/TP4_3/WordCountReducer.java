package TP4_3;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends  Reducer<Text, Text, Text, Text> {
	
	//private Logger logger = Logger.getLogger(WordCountReducer.class);

	public void reduce(Text key, Iterable<Text> values, Context sortie) throws IOException, InterruptedException 
	{
		try{
			Iterator<Text> it = values.iterator();
			double nb = 0;
			double distanceMoyenne = 0;
			String valeur = "";
			while (it.hasNext()) 
			{
				valeur=it.next().toString();
				//System.out.println("Valeur = "+valeur);
				nb+= Double.parseDouble(valeur.split("#")[1]);
				distanceMoyenne += Double.parseDouble(valeur.split("#")[0]);
			}
			
			//logger.info("Distance = " + distanceMoyenne/nb + " Compteur = " + nb + " Region = " + key);
			sortie.write(new Text(key),new Text("Nb d'equipements sportifs :" +nb));
			sortie.write(new Text(key),new Text("Distance moyenne"+distanceMoyenne/nb));
		} catch (Exception e){
			throw new RuntimeException(e.getMessage());
		}

	}
}
 