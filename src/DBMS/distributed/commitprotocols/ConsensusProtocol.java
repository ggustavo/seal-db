package DBMS.distributed.commitprotocols;

import java.util.ArrayList;

import DBMS.distributed.ResourceManagerConnection;

public class ConsensusProtocol extends CommitProtocol {

	public ConsensusProtocol(ArrayList<ResourceManagerConnection> rs) {
		super(rs);
	}

	@Override
	public boolean start() {
		// TODO Auto-generated method stub
		return false;
	}

}
