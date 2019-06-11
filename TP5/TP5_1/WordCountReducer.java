package TP5_1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context sortie)
			throws IOException, InterruptedException {

		Iterator<Text> it = values.iterator();
		String value;
		String ListeActivites = "";
		String LibEquipement = "";
		while (it.hasNext()) {
			value = it.next().toString();
			if (value.startsWith("1")) {
				value = value.substring(2);
				ListeActivites= ListeActivites+"¤"+value;
			}
			else if (value.startsWith("2")) {
				value = value.substring(2);
				LibEquipement=value;
			}	
		}
		if(!ListeActivites.isEmpty()){
			ListeActivites=ListeActivites.substring(1);
		}
		sortie.write(new Text(key), new Text(LibEquipement+ " : " + ListeActivites));
	}
}
