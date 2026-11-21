package org.tron.p2p.discover.protocol.kad;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.discover.DiscoverService;
import org.tron.p2p.discover.Node;
import org.tron.p2p.discover.message.MessageType;
import org.tron.p2p.discover.message.kad.FindNodeMessage;
import org.tron.p2p.discover.message.kad.KadMessage;
import org.tron.p2p.discover.message.kad.NeighborsMessage;
import org.tron.p2p.discover.message.kad.PingMessage;
import org.tron.p2p.discover.message.kad.PongMessage;
import org.tron.p2p.discover.protocol.kad.table.NodeTable;
import org.tron.p2p.discover.socket.UdpEvent;

@Slf4j(topic = "net")
public class KadService implements DiscoverService {

  private static final int MAX_NODES = 2000;
  private static final int NODES_TRIM_THRESHOLD = 3000;
  static final int MAX_PENDING_PONG_TASKS = 20000;
  @Getter
  @Setter
  private static long pingTimeout = 15_000;

  private final List<Node> bootNodes = new ArrayList<>();

  private volatile boolean inited = false;

  private final Map<InetSocketAddress, NodeHandler> nodeHandlerMap = new ConcurrentHashMap<>();

  private Consumer<UdpEvent> messageSender;

  @Getter
  private NodeTable table;
  private Node homeNode;

  private ScheduledThreadPoolExecutor pongTimer;
  private DiscoverTask discoverTask;

  public void init() {
    for (InetSocketAddress address : Parameter.p2pConfig.getSeedNodes()) {
      bootNodes.add(new Node(address));
    }
    for (InetSocketAddress address : Parameter.p2pConfig.getActiveNodes()) {
      bootNodes.add(new Node(address));
    }
    this.pongTimer = new ScheduledThreadPoolExecutor(1,
        BasicThreadFactory.builder().namingPattern("pongTimer").build());
    pongTimer.setRemoveOnCancelPolicy(true);
    this.homeNode = new Node(Parameter.p2pConfig.getNodeID(), Parameter.p2pConfig.getIp(),
        Parameter.p2pConfig.getIpv6(), Parameter.p2pConfig.getPort());
    this.table = new NodeTable(homeNode);

    if (Parameter.p2pConfig.isDiscoverEnable()) {
      discoverTask = new DiscoverTask(this);
      discoverTask.init();
    }
  }

  synchronized ScheduledFuture<?> schedulePongTimeout(Runnable task) {
    // Keep the capacity check and enqueue atomic across discovery and retry threads.
    if (pongTimer.getQueue().size() >= MAX_PENDING_PONG_TASKS) {
      throw new RejectedExecutionException("Pong timer queue is full");
    }
    return pongTimer.schedule(task, pingTimeout, TimeUnit.MILLISECONDS);
  }

  public void close() {
    try {
      if (pongTimer != null) {
        pongTimer.shutdownNow();
      }

      if (discoverTask != null) {
        discoverTask.close();
      }
    } catch (Exception e) {
      log.error("Close nodeManagerTasksTimer or pongTimer failed", e);
      throw e;
    }
  }

  public List<Node> getConnectableNodes() {
    return getAllNodes().stream()
        .filter(node -> node.isConnectible(Parameter.p2pConfig.getNetworkId()))
        .filter(node -> node.getPreferInetSocketAddress() != null)
        .collect(Collectors.toList());
  }

  public List<Node> getTableNodes() {
    return table.getTableNodes();
  }

  public List<Node> getAllNodes() {
    List<Node> nodeList = new ArrayList<>();
    for (NodeHandler nodeHandler : nodeHandlerMap.values()) {
      nodeList.add(nodeHandler.getNode());
    }
    return nodeList;
  }

  @Override
  public void setMessageSender(Consumer<UdpEvent> messageSender) {
    this.messageSender = messageSender;
  }

  @Override
  public void channelActivated() {
    if (!inited) {
      inited = true;

      for (Node node : bootNodes) {
        getNodeHandler(node);
      }
    }
  }

  @Override
  public void handleEvent(UdpEvent udpEvent) {
    KadMessage m = (KadMessage) udpEvent.getMessage();
    InetSocketAddress sender = udpEvent.getAddress();
    if (sender == null || sender.isUnresolved()) {
      return;
    }
    NodeHandler nodeHandler = nodeHandlerMap.get(sender);
    // A response belongs to an existing endpoint's probe; it cannot introduce a sender.
    if (nodeHandler == null && m.getType() != MessageType.KAD_PING
        && m.getType() != MessageType.KAD_FIND_NODE) {
      return;
    }
    Node contact = nodeHandler == null ? m.getFrom().atEndpoint(sender) : nodeHandler.getNode();
    if (contact.getPreferInetSocketAddress() == null) {
      log.debug("Drop discovery message from {}: no compatible reply address", sender);
      return;
    }
    if (nodeHandler == null) {
      nodeHandler = getOrCreateHandler(sender, contact);
    }

    switch (m.getType()) {
      case KAD_PING:
        nodeHandler.handlePing((PingMessage) m, sender);
        break;
      case KAD_PONG:
        nodeHandler.handlePong((PongMessage) m);
        break;
      case KAD_FIND_NODE:
        nodeHandler.handleFindNode((FindNodeMessage) m, sender);
        break;
      case KAD_NEIGHBORS:
        nodeHandler.handleNeighbours((NeighborsMessage) m, sender);
        break;
      default:
        break;
    }
  }

  public NodeHandler getNodeHandler(Node n) {
    InetSocketAddress endpoint = n.getPreferInetSocketAddress();
    if (endpoint == null) {
      return new NodeHandler(n, this);
    }
    // A neighbor advertisement supplies a candidate to probe, never an alias for another record.
    NodeHandler existing = nodeHandlerMap.get(endpoint);
    return existing != null ? existing : getOrCreateHandler(endpoint, n.atEndpoint(endpoint));
  }

  private NodeHandler getOrCreateHandler(InetSocketAddress endpoint, Node contact) {
    trimTable();
    NodeHandler candidate = new NodeHandler(contact, this, endpoint);
    NodeHandler existing = nodeHandlerMap.putIfAbsent(endpoint, candidate);
    if (existing != null) {
      return existing;
    }
    // Publish before sending the first ping so an immediate pong can find its pending probe.
    try {
      candidate.sendPing();
    } catch (RuntimeException e) {
      nodeHandlerMap.remove(endpoint, candidate);
      throw e;
    }
    return candidate;
  }

  public Node getPublicHomeNode() {
    return homeNode;
  }

  public void sendOutbound(UdpEvent udpEvent) {
    if (Parameter.p2pConfig.isDiscoverEnable() && messageSender != null) {
      messageSender.accept(udpEvent);
    }
  }

  private void trimTable() {
    if (nodeHandlerMap.size() > NODES_TRIM_THRESHOLD) {
      nodeHandlerMap.values().forEach(handler -> {
        if (!handler.getNode().isConnectible(Parameter.p2pConfig.getNetworkId())) {
          nodeHandlerMap.values().remove(handler);
        }
      });
    }
    if (nodeHandlerMap.size() > NODES_TRIM_THRESHOLD) {
      List<NodeHandler> sorted = new ArrayList<>(nodeHandlerMap.values());
      sorted.sort(Comparator.comparingLong(o -> o.getNode().getUpdateTime()));
      for (NodeHandler handler : sorted) {
        nodeHandlerMap.values().remove(handler);
        if (nodeHandlerMap.size() <= MAX_NODES) {
          break;
        }
      }
    }
  }
}
