package DBMS.distributed.resourceManager.message.types;

import java.io.Serializable;
import java.util.ArrayList;

@SuppressWarnings("serial")
public class LogTransactionMessage implements Serializable{

	
	private String leaderIP;
	private ArrayList<String> participantsIP;
	private String transactionID;
	
	public LogTransactionMessage() {
		this.participantsIP = new ArrayList<String>();
	}
	
	public String getLeaderIP() {
		return leaderIP;
	}
	public void setLeaderIP(String leaderIP) {
		this.leaderIP = leaderIP;
	}
	public ArrayList<String> getParticipantsIP() {
		return participantsIP;
	}
	public void setParticipantsIP(ArrayList<String> participantsIP) {
		this.participantsIP = participantsIP;
	}
	public String getTransactionID() {
		return transactionID;
	}
	public void setTransactionID(String transactionID) {
		this.transactionID = transactionID;
	}

}