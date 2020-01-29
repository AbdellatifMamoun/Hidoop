package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;
public interface CallBack extends Remote {
	/** */
	public void inc() throws RemoteException;
	/** */
	public void dec() throws RemoteException, InterruptedException;
}
