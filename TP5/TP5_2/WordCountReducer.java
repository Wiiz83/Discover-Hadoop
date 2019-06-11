package TP5_2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context sortie) throws IOException, InterruptedException {

		Iterator<Text> it = values.iterator();
		String value;
		String ListeActivites = "";
		String LibEquipement = "";
		String NomCommune = "";
		while (it.hasNext()) {
			value = it.next().toString();
			if (value.startsWith("1")) {
				value = value.substring(2);
				ListeActivites= ListeActivites+"¤"+value;
			}
			else if (value.startsWith("2")) {
				value = value.substring(2);
				LibEquipement = value.split(";")[0];
				NomCommune = value.split(";")[1];
				
			}	
		}
		if(!ListeActivites.isEmpty()){
			ListeActivites=ListeActivites.substring(1);
		}
		
		for (String activite : ListeActivites.split("¤")) {
			sortie.write(new Text(key), new Text(NomCommune + "¤" + LibEquipement+ "¤" + activite));
		}
	}
}
