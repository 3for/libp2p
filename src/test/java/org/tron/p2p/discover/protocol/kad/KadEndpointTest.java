package org.tron.p2p.discover.protocol.kad;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tron.p2p.P2pConfig;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.discover.Node;
import org.tron.p2p.discover.message.MessageType;
import org.tron.p2p.discover.message.kad.FindNodeMessage;
import org.tron.p2p.discover.message.kad.NeighborsMessage;
import org.tron.p2p.discover.message.kad.PingMessage;
import org.tron.p2p.discover.message.kad.PongMessage;
import org.tron.p2p.discover.socket.UdpEvent;

public class KadEndpointTest {
  private static final InetSocketAddress V4 = new InetSocketAddress("192.0.2.2", 18888);
  private static final InetSocketAddress V6 = new InetSocketAddress("2001:db8::2", 18888);
  private final Node peer = new Node(new byte[64], V4.getHostString(), V6.getHostString(),
      V4.getPort());
  private final List<UdpEvent> outbound = new ArrayList<>();
  private final KadService service = new KadService();
  private P2pConfig previousConfig;
  private long previousTimeout;

  @Before
  public void setUp() {
    previousConfig = Parameter.p2pConfig;
    previousTimeout = KadService.getPingTimeout();
    P2pConfig config = new P2pConfig();
    config.setIp("192.0.2.1");
    config.setIpv6("2001:db8::1");
    byte[] homeId = new byte[64];
    homeId[0] = 1;
    config.setNodeID(homeId);
    config.setDiscoverEnable(false);
    Parameter.p2pConfig = config;
    KadService.setPingTimeout(60_000);
    service.init();
    service.setMessageSender(outbound::add);
    config.setDiscoverEnable(true);
  }

  @After
  public void tearDown() {
    service.close();
    KadService.setPingTimeout(previousTimeout);
    Parameter.p2pConfig = previousConfig;
  }

  @Test
  public void dualStackRequestsUseTheirSourceAddressAndPort() {
    InetSocketAddress translated = new InetSocketAddress(V6.getAddress(), V6.getPort() + 1);
    for (InetSocketAddress sender : Arrays.asList(V4, V6, translated)) {
      outbound.clear();
      service.handleEvent(new UdpEvent(new PingMessage(peer, service.getPublicHomeNode()), sender));
      FindNodeMessage find = new FindNodeMessage(peer, new byte[64]);
      service.handleEvent(new UdpEvent(find, sender));

      assertEquals(3, outbound.size());
      assertDestination(0, MessageType.KAD_PING, sender);
      assertDestination(1, MessageType.KAD_PONG, sender);
      assertDestination(2, MessageType.KAD_NEIGHBORS, sender);
      assertEquals(find.getTimestamp(), ((NeighborsMessage) outbound.get(2).getMessage())
          .getTimestamp());
    }
    assertEquals(3, service.getAllNodes().size());
    assertNull(record(V4).getHostV6());
    assertNull(record(V6).getHostV4());
    assertFalse(record(translated).isConnectible(Parameter.p2pConfig.getNetworkId()));
    assertTrue(record(V6).isConnectible(Parameter.p2pConfig.getNetworkId()));
  }

  @Test
  public void disabledSourceFamilyCannotFallBackToAnAdvertisedAddress() {
    Parameter.p2pConfig.setIpv6(null);
    service.handleEvent(new UdpEvent(new PingMessage(peer, service.getPublicHomeNode()), V6));
    service.handleEvent(new UdpEvent(new FindNodeMessage(peer, new byte[64]), V6));
    assertTrue(outbound.isEmpty());
    assertTrue(service.getAllNodes().isEmpty());
  }

  @Test
  public void referralsProbeOneEndpointWithoutMergingFamilies() {
    NodeHandler ipv6 = service.getNodeHandler(peer.atEndpoint(V6));
    service.handleEvent(new UdpEvent(new PongMessage(peer), V6));
    assertEquals(NodeHandler.State.ACTIVE, ipv6.getState());

    ipv6.sendFindNode(peer.getId());
    outbound.clear();
    service.handleEvent(new UdpEvent(new NeighborsMessage(peer,
        Collections.singletonList(peer), 1), V6));
    assertEquals(1, outbound.size());
    assertDestination(0, MessageType.KAD_PING, V4);
    assertNull(ipv6.getNode().getHostV4());
    assertEquals(2, service.getAllNodes().size());
    NodeHandler ipv4 = service.getNodeHandler(peer);
    assertEquals(NodeHandler.State.DISCOVERED, ipv4.getState());
    assertNull(ipv4.getNode().getHostV6());
    assertSame(ipv6, service.getNodeHandler(peer.atEndpoint(V6)));
    service.handleEvent(new UdpEvent(new PongMessage(peer), V4));
    assertEquals(NodeHandler.State.ACTIVE, ipv4.getState());
  }

  @Test
  public void repliesOnlyUpdateTheEndpointWithAnOutstandingRequest() {
    NodeHandler handler = service.getNodeHandler(peer.atEndpoint(V6));
    outbound.clear();
    service.handleEvent(new UdpEvent(new PongMessage(peer), V4));
    InetSocketAddress otherPort = new InetSocketAddress(V6.getAddress(), V6.getPort() + 1);
    service.handleEvent(new UdpEvent(new PongMessage(peer), otherPort));
    service.handleEvent(new UdpEvent(new NeighborsMessage(peer,
        Collections.singletonList(peer), 1), V4));
    assertTrue(outbound.isEmpty());
    assertEquals(1, service.getAllNodes().size());
    assertEquals(NodeHandler.State.DISCOVERED, handler.getState());

    service.handleEvent(new UdpEvent(new PongMessage(peer), V6));
    assertEquals(NodeHandler.State.ACTIVE, handler.getState());
    byte[] newId = peer.getId().clone();
    newId[0] = 2;
    Node restartedPeer = new Node(newId, null, V6.getHostString(), V6.getPort());
    service.handleEvent(new UdpEvent(new PongMessage(restartedPeer), V6));
    assertArrayEquals(peer.getId(), handler.getNode().getId());
    service.handleEvent(new UdpEvent(new NeighborsMessage(restartedPeer,
        Collections.singletonList(peer), 1), V6));
    assertArrayEquals(peer.getId(), handler.getNode().getId());
    assertEquals(1, service.getAllNodes().size());
    assertTrue(outbound.isEmpty());
  }

  @Test
  public void failedInitialProbeCanBeRetried() {
    service.setMessageSender(event -> {
      throw new IllegalStateException("Test sender unavailable");
    });
    assertThrows(IllegalStateException.class, () -> service.getNodeHandler(peer));
    assertTrue(service.getAllNodes().isEmpty());
    service.setMessageSender(outbound::add);
    service.getNodeHandler(peer);
    assertEquals(1, outbound.size());
    assertDestination(0, MessageType.KAD_PING, V4);
  }

  @Test
  public void newHandlerIsRegisteredBeforeItsFirstProbe() {
    service.setMessageSender(event -> {
      assertEquals(MessageType.KAD_PING, event.getMessage().getType());
      service.handleEvent(new UdpEvent(new PongMessage(peer), event.getAddress()));
    });
    // A configured seed initially has a locally generated ID and learns its ID from its own pong.
    NodeHandler handler = service.getNodeHandler(new Node(V4));
    assertEquals(NodeHandler.State.ACTIVE, handler.getState());
    assertSame(handler.getNode(), record(V4));
    assertArrayEquals(peer.getId(), handler.getNode().getId());
  }

  private Node record(InetSocketAddress endpoint) {
    return service.getAllNodes().stream()
        .filter(node -> endpoint.equals(node.getPreferInetSocketAddress())).findFirst().get();
  }

  private void assertDestination(int index, MessageType type, InetSocketAddress expected) {
    UdpEvent event = outbound.get(index);
    assertEquals(type, event.getMessage().getType());
    assertEquals(expected, event.getAddress());
  }
}
