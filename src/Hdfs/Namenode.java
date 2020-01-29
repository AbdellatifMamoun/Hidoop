package hdfs;



 import java.rmi.*;
import java.util.HashMap;

public interface Namenode extends Remote {
	
	public void setHashMap(HashMap<String,HashMap<Integer,String>> hm) throws RemoteException;
	
	public HashMap<String,HashMap<Integer,String>> getHashMap() throws RemoteException;
	
	public HashMap<Integer,String> getFragments(String name) throws RemoteException;
	
	public void Ajouter(String filename,HashMap<Integer,String> fragments) throws RemoteException;
	
	public void Afficher() throws RemoteException;

	public void supprimer(String name) throws RemoteException;
	
	

}
