package TP5_4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, Text, Text, Text> {

	// private Logger logger = Logger.getLogger(WordCountReducer.class);

	public void reduce(Text key, Iterable<Text> values, Context sortie)
			throws IOException, InterruptedException {
		try {
			Iterator<Text> it = values.iterator();
			double nb = 0;
			String equipement = "";
			ArrayList<String> listeSport = new ArrayList<String>();
			String valeur = "";
			while (it.hasNext()) {
				valeur = it.next().toString();
				if (equipement == "") {
					equipement = valeur.split("¤")[1];
				}
				listeSport.add(valeur.split("¤")[2]);
			}

			sortie.write(new Text(key), new Text(equipement + " : "
					+ listeSport.toString()));
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}

	}
}
