package config;

import java.util.List; 
import java.util.ArrayList; 
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
public class Project {
	public final static String PATH = "";
	//public final static String[] HOSTS = {"localhost","localhost"}; 
	public static String[] HOSTS ; 
	public final static Integer NBHOSTS ;
    //public final static Integer[] PORTS = {2015, 2016, 2017, 2018, 2019};
	public final static int[] PORTS = {2020,2021};
	public static final String DATA = "/data/";
    public static long Taillefrag = 128000000;
    public static final int Portinit = 2020;
    public static int Nbmachines ;
    //public static int Nbmachines = HOSTS.length;
    public static int Taillesousfrag = 1000000;

	public static void main(String args[]) {
		try {

		File myfile = new File("config/configu.txt");
    	//FileInputStream raf = new FileInputStream(myfile);
        BufferedReader read = new BufferedReader(new FileReader(myfile));
	    //int compteur = 0;
        List<String> list = new ArrayList<String>();
        String num = read.readLine();
        int numport = Integer.parseInt(num); 
        System.out.println("port initiale : "+ numport );
        String ligne;
	    
        while ((ligne = read.readLine() )!= null) {
                    System.out.println("serveur : "+ ligne );
                    list.add(ligne);
                    

	    }
        HOSTS = new String[list.size()]; 
		Nbmachines = list.size();
        NBHOSTS = (Integer) Nbmachines;
        HOSTS = list.toArray(HOSTS);
        System.out.println("Nb machines : "+ Nbmachines );
        System.out.println( HOSTS[0] );
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
   
}
