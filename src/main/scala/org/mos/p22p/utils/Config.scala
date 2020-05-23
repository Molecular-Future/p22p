package org.mos.p22p.utils

import onight.tfw.outils.conf.PropHelper

object Config {
  val prop: PropHelper = new PropHelper(null);
  //前缀
  val PROP_DOMAIN = "org.bc.pzp."

  //投票的超时时间
  val TIMEOUT_STATE_VIEW = prop.get(PROP_DOMAIN + "timeout.state.view", 60 * 1000);

  //投票的超时重投时间
  val TIMEOUT_STATE_VIEW_RESET = prop.get(PROP_DOMAIN + "timeout.state.view.reset", 360 * 1000);
  //投票的最少间隔时间
  val MIN_EPOCH_EACH_VOTE = prop.get(PROP_DOMAIN + "min.epoch.each.vote", 10 * 1000)

  //每次投票收的等待时间
  val MAX_VOTE_SLEEP_MS = prop.get(PROP_DOMAIN + "max.vote.sleep.ms", 60000);

  //投票被拒绝的时候，随机等待时间的最大值
  val BAN_FOR_VOTE_SLEEP_MS = prop.get(PROP_DOMAIN + "ban.for.vote.sleep.ms", 60000);
  //投票被拒绝的时候，随机等待时间的最小值
  val BAN_FOR_VOTE_MIN_SLEEP_MS = prop.get(PROP_DOMAIN + "ban.for.vote.min.sleep.ms", 10000);
  //投票被拒绝的时候，随机等待时间
  def getRandSleepForBan(): Int = {
    (Math.abs(Math.random()) * BAN_FOR_VOTE_SLEEP_MS + BAN_FOR_VOTE_MIN_SLEEP_MS).asInstanceOf[Int];
  }
  //投票最少的等待时间
  val MIN_VOTE_SLEEP_MS = prop.get(PROP_DOMAIN + "min.vote.sleep.ms", 10000);

  //没有变化时，最少的等待时间
  val MIN_VOTE_WITH_NOCHANGE_SLEEP_MS = prop.get(PROP_DOMAIN + "min.vote.sleep.nochange.ms", 120 * 1000);

  //心跳线程间隔时间
  val TICK_CHECK_HEALTHY = prop.get(PROP_DOMAIN + "tick.check.healthy", 10);
  
  //新节点加入检查线程间隔时间
  val TICK_JOIN_NETWORK = prop.get(PROP_DOMAIN + "tick.join.network", 60);
  //投票线程的间隔时间
  val TICK_VOTE_MAP = prop.get(PROP_DOMAIN + "tick.vote.map", 10);
  
  //消息处理线程间隔时间
  val TICK_VOTE_WORKER = prop.get(PROP_DOMAIN + "tick.vote.worker", 1);
  //快照的数量
  val NUM_VIEWS_EACH_SNAPSHOT = prop.get(PROP_DOMAIN + "num.views.each.snapshot", 10); //每快照有几个
  //消息超时时间
  val TIMEOUT_MS_MESSAGE = prop.get(PROP_DOMAIN + "timeout.ms.message", 60 * 1000); 

  //每次重启时，是否重置该节点
  val RESET_NODEINFO = prop.get(PROP_DOMAIN + "reset.nodeinfo", 0);

  //心跳超时重试次数，超过该值，则认为该节点已经掉线
  val HB_FAILED_COUNT = prop.get(PROP_DOMAIN + "hb.failed.count", 3);

  //一直时pending，没有加入到dnode的最大时间。防止该节点一直在pending中国，则删除
  val TIMEOUT_SEC_PENDING_CONFIRM = prop.get(PROP_DOMAIN + "timeout.sec.pending.confirm", 60); 

  val TIMEOUT_SEC_DNODE_CONFIRM = prop.get(PROP_DOMAIN + "timeout.sec.dnode.confirm", 60);
  
  
  val MESSAGE_SIGN = prop.get(PROP_DOMAIN + "message.sign", 0);
  
  val STR_REJECT = "__REJECT";

  //节点状态进行URL操作时，ip地址白名单
  val IP_WHITE_LIST = prop.get(PROP_DOMAIN + "ip.white.list", "localhost,127.0.0.1,0:0:0:0:0:0:0:1,");

  val LOC_ID = prop.get(PROP_DOMAIN + "loc.id", "local");
  
  val LOC_GWURIS = prop.get(PROP_DOMAIN + "loc.gwuris", "tcp://127.0.0.1:5100");
  
  
  val VOTE_VIEW_CACHE_MAX_SIZE = prop.get(PROP_DOMAIN + "vote.view.cache.max.size", 2048);
  
  val VOTE_VIEW_CACHE_EXPIRE_SEC = prop.get(PROP_DOMAIN + "vote.view.cache.expire.sec", 600);
  
  def VOTE_DEBUG: Boolean = {
    //    NodeHelper.getCurrNodeListenOutPort != 5100;
    false
  }
}