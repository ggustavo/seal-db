package DBMS.distributed.commitprotocols;

import java.util.ArrayList;

import DBMS.distributed.ResourceManagerConnection;

public class ThreePhaseProtocol extends CommitProtocol {

	public ThreePhaseProtocol(ArrayList<ResourceManagerConnection> rs) {
		super(rs);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean start() {
		// TODO Auto-generated method stub
		return false;
	}

}
