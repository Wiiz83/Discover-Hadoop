package TP5_5;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable cle, Text valeur, Context sortie)
			throws IOException, InterruptedException {
		String nomFichier = ((FileSplit) sortie.getInputSplit()).getPath()
				.getName();
		Configuration conf = sortie.getConfiguration();

		String ordre = conf.get("ordre");
		String fichier1;
		String fichier2;
		if (ordre.equals("1")) {
			fichier1 = conf.get("nomFichier-1");
			fichier2 = conf.get("nomFichier-2");
		} else {
			fichier1 = conf.get("nomFichier-2");
			fichier2 = conf.get("nomFichier-1");
		}

		if (nomFichier.equals(fichier2)) { // "villes_france.csv"
			if (valeur.toString().length() > 0) {
				try {
					String line = valeur.toString();

					String idCommune = line.split(",")[10];
					idCommune = idCommune.substring(1, idCommune.length() - 1);
					String densite = line.split(",")[18];
					densite = densite.substring(1, densite.length() - 1);
					sortie.write(new Text(idCommune), new Text("1 " + densite));
				} catch (ArrayIndexOutOfBoundsException e) {
				}
			}
		} else {
			if (nomFichier.equals(fichier1)) { // "20140410_RES_FichesEquipements.csv"
				if ((cle.get() > 0) && (valeur.toString().length() > 0)) {
					String line = valeur.toString();
					String idCommune = line.split(";")[2];
					String nomCommune = line.split(";")[3];
					String idEquipement = line.split(";")[6];
					sortie.write(new Text(idCommune), new Text("2 "
							+ nomCommune));
				}
			}
		}

	}
}
