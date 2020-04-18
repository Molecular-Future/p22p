package org.mos.p22p;

import onight.tfw.ojpa.api.ServiceSpec;
import org.mos.mcore.odb.ODBDao;

public class ODSP22p extends ODBDao {

	public ODSP22p(ServiceSpec serviceSpec) {
		super(serviceSpec);
	}

	@Override
	public String getDomainName() {
		return "pzp";
	}

	
}
