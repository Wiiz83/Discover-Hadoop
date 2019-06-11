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
			// récupérer la ligne
			String line = valeur.toString();
			// récupérer les mots de la ligne
			StringTokenizer itr = new StringTokenizer(line, " .,?!:;");
			// pour chaque mot 
			while (itr.hasMoreTokens()) 
			{
				word.set(itr.nextToken().trim());
				// on récupère la première lette du mot
				String firstChar = word.toString().toLowerCase().substring(0, 1);
				// si on ne l'a jamais rencontré 
				if(!tab.containsKey(firstChar)){
					// on l'ajoute dans notre hashmap avec son compteur à 1
					tab.put(firstChar, 1);
				// si on l'a déjà rencontré 
				}else{
					// on incrémente le compteur de ce mot 
					tab.put(firstChar, tab.get(firstChar)+1);
				}
			}
			// pour chaque mot rencontré 
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
