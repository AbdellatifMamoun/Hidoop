package hdfs;

import java.io.IOException;
import java.net.*;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.rmi.server.UnicastRemoteObject;




/*import formats.*;
/*
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat; */

public class NameNodeImpl extends UnicastRemoteObject implements Namenode {
	//static int portclient ; //port de communication avec le HdfsClient de la même machine
	//int[] portserveurs;  //liste des ports des autres Hdfs Serveurs
	//int nbmachines = 5;
    //public ServerSocket sSocket ;
    //public String IP = "localhost";
    //private boolean isRunning = true;
    //catalogue des ports de chaque machine
    //private HashMap<String,Integer> catalogueport = new HashMap<String,Integer>();
    //catalogue des fragment
	
    private HashMap<Integer,String> cataloguegfragment = new HashMap<Integer,String>();
    
    
    //catalogue des machines , nom fichier , <identifiant de fragment de ce fichier,emplacement de ce 
    	//fragment>
    private HashMap<String,HashMap<Integer,String>> cataloguemachine ;
    
    public NameNodeImpl() throws RemoteException {
    	cataloguemachine = new HashMap<String,HashMap<Integer,String>>();
    }
    

	@Override
	public void setHashMap(HashMap<String, HashMap<Integer, String>> hm) throws RemoteException {
		this.cataloguemachine = hm;
	}

	@Override
	public HashMap<String, HashMap<Integer, String>> getHashMap() throws RemoteException {
		// TODO Auto-generated method stub
		return this.cataloguemachine;
	}

	@Override
	public HashMap<Integer, String> getFragments(String name) throws RemoteException {
		// TODO Auto-generated method stub
		return this.cataloguemachine.get(name);
	}


	@Override
	public void Ajouter(String filename, HashMap<Integer, String> fragments) {
		
		this.cataloguemachine.put(filename,fragments);
	}
	
	@Override
	public void Afficher() {
		try { System.out.println(cataloguemachine.toString()); }
        catch (Exception e) {
            System.out.println("nbre des fichiers dans HDFS : "+cataloguemachine.size());
        }
		
	}

	@Override
	public void supprimer(String name) throws RemoteException {
		// TODO Auto-generated method stub
		this.cataloguemachine.remove(name);
	}
	
	public static void main(String args[]) {
		try {
			NameNodeImpl namenode = new NameNodeImpl();
			
		
		Registry registry = LocateRegistry.createRegistry(2019);
		Naming.rebind("//localhost:2019/namenode", namenode);
		System.out.println("namenode bound in registry");
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
    	
  }
    
    


