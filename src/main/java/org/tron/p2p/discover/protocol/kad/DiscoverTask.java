package org.tron.p2p.discover.protocol.kad;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.tron.p2p.discover.Node;
import org.tron.p2p.discover.protocol.kad.table.KademliaOptions;
import org.tron.p2p.utils.NetUtil;

@Slf4j(topic = "net")
public class DiscoverTask {

  private ScheduledExecutorService discoverer = Executors.newSingleThreadScheduledExecutor(
      BasicThreadFactory.builder().namingPattern("discoverTask").build());

  private KadService kadService;

  private int loopNum = 0;
  private byte[] nodeId;

  public DiscoverTask(KadService kadService) {
    this.kadService = kadService;
  }

  public void init() {
    discoverer.scheduleWithFixedDelay(() -> {
      try {
        loopNum++;
        if (loopNum % KademliaOptions.MAX_LOOP_NUM == 0) {
          loopNum = 0;
          nodeId = kadService.getPublicHomeNode().getId();
        } else {
          nodeId = NetUtil.getNodeId();
        }
        discover(nodeId);
      } catch (Exception e) {
        log.error("DiscoverTask fails to be executed", e);
      }
    }, 1, KademliaOptions.DISCOVER_CYCLE, TimeUnit.MILLISECONDS);
    log.debug("DiscoverTask started");
  }

  void discover(byte[] targetId) {
    discover(targetId, 0, new HashSet<>());
  }

  private void discover(byte[] targetId, int round, Set<String> queriedIds) {
    Set<String> closestIds = new HashSet<>();
    int sent = 0;
    for (Node candidate : kadService.getTable().getNodesByDistance(targetId)) {
      String id = candidate.getHexId();
      // Limit the shortlist by ID while allowing another endpoint if a local send fails.
      if (closestIds.size() < KademliaOptions.BUCKET_SIZE) {
        closestIds.add(id);
      }
      if (!closestIds.contains(id) || queriedIds.contains(id)) {
        continue;
      }
      try {
        kadService.getNodeHandler(candidate).sendFindNode(targetId);
        queriedIds.add(id);
        sent++;
      } catch (Exception e) {
        log.error("Unexpected Exception occurred while sending FindNodeMessage", e);
      }
      if (sent == KademliaOptions.ALPHA) {
        break;
      }
    }

    try {
      Thread.sleep(KademliaOptions.WAIT_TIME);
    } catch (InterruptedException e) {
      log.warn("Discover task interrupted");
      Thread.currentThread().interrupt();
    }

    if (sent > 0 && round + 1 < KademliaOptions.MAX_STEPS) {
      discover(targetId, round + 1, queriedIds);
    }
  }

  public void close() {
    discoverer.shutdownNow();
  }
}
