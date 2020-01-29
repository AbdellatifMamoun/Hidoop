package config;

public class Project {
	public final static String PATH = "";
	public final static String[] HOSTS = {"localhost", "localhost", "localhost", "localhost", "localhost"}; 
	public final static Integer NBHOSTS = HOSTS.length;
    //public final static Integer[] PORTS = {2015, 2016, 2017, 2018, 2019};
	public final static Integer[] PORTS = {2015, 2016, 2017, 2018, 2019};
	public static final String DATA = "/data/";
    public static long Taillefrag = 3100000;
    public static final int Portinit = 2020;
    public static int Nbmachines = 5;
    public static int Taillesousfrag = 300;
}
