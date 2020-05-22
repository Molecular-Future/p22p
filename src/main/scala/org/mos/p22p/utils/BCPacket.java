package org.mos.p22p.utils;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import onight.tfw.otransio.api.beans.ExtHeader;
import onight.tfw.otransio.api.beans.FixHeader;
import onight.tfw.otransio.api.beans.FramePacket;
import onight.tfw.outils.serialize.SerializerFactory;
import org.mos.p22p.model.P22P.MRSPacket;

import java.util.List;

public class BCPacket extends FramePacket {
	protected ExtHeader genExtHeader() {
		return new BCExtHeader();
	}

	public static BCPacket buildSyncFrom(Message body, String cmd, String module) {
		BCPacket ret = new BCPacket();
		ret.setFbody(body);
		FixHeader fh = new FixHeader();
		fh.setCmd(cmd);
		fh.setVer('b');
		fh.setSync(true);
		fh.setModule(module);
		fh.setEnctype(SerializerFactory.SERIALIZER_PROTOBUF);
		ret.setFixHead(fh);
		return ret;
	}

	public static BCPacket buildSyncFrom(byte[] body, String cmd, String module) {
		BCPacket ret = new BCPacket();
		ret.setBody(body);
		FixHeader fh = new FixHeader();
		fh.setCmd(cmd);
		fh.setVer('b');
		fh.setSync(true);
		fh.setModule(module);
		fh.setEnctype(SerializerFactory.SERIALIZER_PROTOBUF);
		ret.setFixHead(fh);
		return ret;
	}

	public static BCPacket buildAsyncFrom(byte[] body, String cmd, String module) {
		BCPacket ret = buildSyncFrom(body, cmd, module);
		ret.getFixHead().setSync(false);
		return ret;
	}

	public static BCPacket buildAsyncFrom(Message body, String cmd, String module) {
		BCPacket ret = buildSyncFrom(body, cmd, module);
		ret.getFixHead().setSync(false);
		return ret;
	}

	public static BCPacket wrapPacket(BCPacket origin, String locid, List<String> bcuids, List<String> uris,String nid,String origin_bcuid
			,String origin_messageid) {
		BCPacket ret = new BCPacket();

		byte[] bodyb = origin.genBodyBytes();
		byte[] extb = origin.genExtBytes();
		origin.getFixHead().setExtsize(extb.length);
		origin.getFixHead().setBodysize(bodyb.length);

		MRSPacket.Builder mspack = MRSPacket.newBuilder().addAllUris(uris).addAllBcUids(bcuids).setLocId(locid);
		mspack.setFixhead(ByteString.copyFrom(origin.getFixHead().genBytes()));
		if (extb.length > 0) {
			mspack.setExthead(ByteString.copyFrom(extb));
		}
		if (bodyb.length > 0) {
			mspack.setBody(ByteString.copyFrom(bodyb));
		}
		mspack.setNid(nid);
		mspack.setOriginBcuid(origin_bcuid);
		mspack.setOriginMessageid(origin_messageid);

		ret.setFbody(mspack);
		FixHeader fh = new FixHeader();
		fh.setCmd("MRS");
		fh.setVer('b');
		fh.setSync(true);
		fh.setModule("PZP");
		fh.setEnctype(SerializerFactory.SERIALIZER_PROTOBUF);
		ret.setFixHead(fh);
		return ret;
	}

	public static BCPacket extractWrapPacket(MRSPacket packet) {
		BCPacket ret = new BCPacket();

		if (!packet.getBody().isEmpty()) {
			ret.setBody(packet.getBody().toByteArray());
		}
		FixHeader fh = FixHeader.parseFrom(packet.getFixhead().toByteArray());
		ret.setFixHead(fh);
		if (!packet.getExthead().isEmpty()) {
			ret.setExtHead(ExtHeader.buildFrom(packet.getExthead().toByteArray()));
		}
		return ret;
	}

}
