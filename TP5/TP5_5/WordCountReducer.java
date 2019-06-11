package TP5_5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context sortie)
			throws IOException, InterruptedException {
		Iterator<Text> it = values.iterator();
		String value;
		int nombreEquipement = 0;
		String densite = "";
		String nomCommune = "";
		while (it.hasNext()) {
			value = it.next().toString();
			if (value.startsWith("1")) {
				value = value.substring(2);
				if (value != "") {
					densite = value;
				}

			}
			// equipement
			else if (value.startsWith("2")) {
				value = value.substring(2);
				nomCommune = value;
				nombreEquipement++;
			}
		}
		if (densite != "" && densite != "0") {
			sortie.write(new Text(nomCommune), new Text((nombreEquipement
					/ Double.parseDouble((densite)) + "")));
		}
	}

}
