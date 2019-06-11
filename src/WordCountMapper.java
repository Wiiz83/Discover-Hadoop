import java.util.HashMap;
import java.util.StringTokenizer;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable cle, Text valeur, Context sortie) throws IOException {
		HashMap<String,Integer> tab=new HashMap<String,Integer>();
		Text word = new Text();
		try {			
			// r�cup�rer la ligne
			String line = valeur.toString();
			// r�cup�rer les mots de la ligne
			StringTokenizer itr = new StringTokenizer(line, " .,?!:;");
			// pour chaque mot 
			while (itr.hasMoreTokens()) 
			{
				word.set(itr.nextToken().trim());
				// on r�cup�re la premi�re lette du mot
				String firstChar = word.toString().toLowerCase().substring(0, 1);
				// si on ne l'a jamais rencontr� 
				if(!tab.containsKey(firstChar)){
					// on l'ajoute dans notre hashmap avec son compteur � 1
					tab.put(firstChar, 1);
				// si on l'a d�j� rencontr� 
				}else{
					// on incr�mente le compteur de ce mot 
					tab.put(firstChar, tab.get(firstChar)+1);
				}
			}
			// pour chaque mot rencontr� 
			for(String word2 : tab.keySet()){
				word.set(word2);
				// renvoyer le mot et son compteur final 
				sortie.write(word, new IntWritable(tab.get(word2)));
			}
		} 
		catch (Exception ee) {
			throw new RuntimeException(ee.getMessage());
		}
	}
}
