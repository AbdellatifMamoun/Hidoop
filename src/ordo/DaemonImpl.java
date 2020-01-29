package ordo;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

import formats.Format;
import formats.Format.OpenMode;
import map.Mapper;
import map.Reducer;

public class DaemonImpl extends UnicastRemoteObject implements Daemon {

	public DaemonImpl() throws RemoteException{
	}
	@Override
	public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
		System.out.println("Daemon : Commencer une tache");
		System.out.println("Daemon : Ouvrir le lecteur");
		reader.open(OpenMode.R);
		System.out.println("Daemon : Ouvrir le rédacteur");
		writer.open(OpenMode.W);
		System.out.println("Daemon : Lancer le Map");
		m.map(reader, writer);
		writer.close();
		reader.close();
        try {
		    cb.inc();
        } catch (Exception e) {
                   
        }
		System.out.println("Daemon : Fin de la tache");
	}
	
	public void runReduce(Reducer r, Format reader, Format writer) {
		System.out.println();
		reader.open(OpenMode.R);
		writer.open(OpenMode.W);
		r.reduce(reader, writer); 
		reader.close();
		writer.close();
	}
	
	public static void main(String[] args) {
		try {
			int port = Integer.parseInt(args[0]);
			LocateRegistry.createRegistry(port);
			Naming.rebind("//" + InetAddress.getLocalHost().getHostName() + ":" + port + "/daemon", new DaemonImpl());
		} catch (RemoteException e) {
			e.getCause();
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.getCause();
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.getCause();
			e.printStackTrace();
		}
	}
}
