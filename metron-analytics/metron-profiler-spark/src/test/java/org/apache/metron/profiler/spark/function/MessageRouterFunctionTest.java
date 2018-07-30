package org.apache.metron.profiler.spark.function;

import com.google.common.collect.Lists;
import org.adrianwalker.multilinestring.Multiline;
import org.apache.metron.common.configuration.profiler.ProfileConfig;
import org.apache.metron.common.configuration.profiler.ProfilerConfig;
import org.apache.metron.profiler.MessageRoute;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Tests the {@link MessageRouterFunction}.
 */
public class MessageRouterFunctionTest {

  /**
   * { "ip_src_addr": "192.168.1.22" }
   */
  @Multiline
  private String goodMessage;

  /**
   * { "ip_src_addr": "192.168.1.22"
   */
  private String badMessage;

  @Test
  public void testFindRoutes() throws Exception {
    MessageRouterFunction function = new MessageRouterFunction(oneProfile(), getGlobals());
    Iterator<MessageRoute> iter = function.call(goodMessage);

    List<MessageRoute> routes = Lists.newArrayList(iter);
    Assert.assertEquals(1, routes.size());
    Assert.assertEquals("profile1", routes.get(0).getProfileDefinition().getProfile());
  }

  /**
   * A bad or invalid message should return no routes.
   */
  @Test
  public void testWithBadMessage() throws Exception {
    MessageRouterFunction function = new MessageRouterFunction(oneProfile(), getGlobals());
    Iterator<MessageRoute> iter = function.call(badMessage);

    List<MessageRoute> routes = Lists.newArrayList(iter);
    Assert.assertEquals(0, routes.size());
  }

  @Test
  public void testFindMultipleRoutes() throws Exception {
    MessageRouterFunction function = new MessageRouterFunction(twoProfiles(), getGlobals());
    Iterator<MessageRoute> iter = function.call(goodMessage);

    List<MessageRoute> routes = Lists.newArrayList(iter);
    Assert.assertEquals(2, routes.size());
    Assert.assertEquals("profile1", routes.get(0).getProfileDefinition().getProfile());
    Assert.assertEquals("profile2", routes.get(1).getProfileDefinition().getProfile());
  }

  private ProfilerConfig oneProfile() {
    ProfileConfig profile = new ProfileConfig()
            .withProfile("profile1")
            .withForeach("ip_src_addr")
            .withUpdate("count", "count + 1")
            .withResult("count");

    return new ProfilerConfig()
            .withProfile(profile);
  }

  private ProfilerConfig twoProfiles() {
    ProfileConfig profile1 = new ProfileConfig()
            .withProfile("profile1")
            .withForeach("ip_src_addr")
            .withUpdate("count", "count + 1")
            .withResult("count");
    ProfileConfig profile2 = new ProfileConfig()
            .withProfile("profile2")
            .withForeach("ip_src_addr")
            .withUpdate("count", "count + 1")
            .withResult("count");
    return new ProfilerConfig()
            .withProfile(profile1)
            .withProfile(profile2);
  }

  private Map<String, String> getGlobals() {
    return Collections.emptyMap();
  }
}
