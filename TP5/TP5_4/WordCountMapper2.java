package TP5_4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WordCountMapper2 extends Mapper<LongWritable,Text, Text, Text> {
	
	public void map(LongWritable cle, Text valeur, Context sortie) throws IOException, InterruptedException {
		String nomFichier=((FileSplit) sortie.getInputSplit()).getPath().getName();
		Configuration conf = sortie.getConfiguration();
		
		String ordre = conf.get("ordre");
		String fichier1;
		String fichier2;
		if(ordre.equals("1")){
			fichier1 = conf.get("nomFichier-1");
			fichier2 = conf.get("nomFichier-2");
		} else {
			fichier1 = conf.get("nomFichier-2");
			fichier2 = conf.get("nomFichier-1");
		}

		if(nomFichier.equals(fichier2)){ //"20140410_RES_FichesEquipementsActivites.csv"
			if((cle.get() > 0) && (valeur.toString().length() > 0)){	
				try{	
					int numColonneId = Integer.parseInt(conf.get("colonneID-2"));
					String line = valeur.toString();
					
					String idEquipement = line.split(";")[numColonneId];			
					String selectedValues = "";
					String delimiter = conf.get("separateur");
					for(String s : conf.get("listeDesColonnesProjetees-2").split(",")){
						selectedValues = selectedValues+delimiter+line.split(";")[Integer.parseInt(s)];
					}
					selectedValues.substring(1);
					sortie.write(new Text(idEquipement), new Text("1 "+selectedValues));
				}catch(ArrayIndexOutOfBoundsException e){
				}
			} 
		}else{
			if(nomFichier.equals(fichier1)){ //"20140410_RES_FichesEquipements.csv"
				if((cle.get() > 0) && (valeur.toString().length() > 0)){
					int numColonneId = Integer.parseInt(conf.get("colonneID-1"));
					String line = valeur.toString();
					String idEquipement = line.split(";")[numColonneId];			
					String selectedValues = "";
					String delimiter = conf.get("separateur");
					for(String s : conf.get("listeDesColonnesProjetees-1").split(",")){
						selectedValues = selectedValues+delimiter+line.split(";")[Integer.parseInt(s)];
					}
					selectedValues.substring(1);
					sortie.write(new Text(idEquipement), new Text("2 "+selectedValues));
				} 
			}
		}
		
		}	
	}
	
