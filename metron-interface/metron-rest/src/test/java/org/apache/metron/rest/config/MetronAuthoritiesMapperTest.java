package org.apache.metron.rest.config;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.metron.rest.MetronRestConstants.SECURITY_ROLE_ADMIN;
import static org.apache.metron.rest.MetronRestConstants.SECURITY_ROLE_PREFIX;
import static org.apache.metron.rest.MetronRestConstants.SECURITY_ROLE_USER;

public class MetronAuthoritiesMapperTest {

  private MetronAuthoritiesMapper mapper;

  @Test
  public void shouldMapUserGroup() {
    mapper = new MetronAuthoritiesMapper();
    mapper.setUserRole("ACME_METRON_USER");
    mapper.setAdminRole("ACME_METRON_ADMIN");

    List<GrantedAuthority> input = new ArrayList<>();
    input.add(new SimpleGrantedAuthority("ACME_METRON_USER"));

    // ROLE_USER == ACME_METRON_USER
    Collection<? extends GrantedAuthority> actuals = mapper.mapAuthorities(input);
    Assert.assertEquals(1, actuals.size());
    Assert.assertEquals(SECURITY_ROLE_PREFIX + SECURITY_ROLE_USER, actuals.iterator().next().getAuthority());
  }

  @Test
  public void shouldMapAdminGroup() {
    mapper = new MetronAuthoritiesMapper();
    mapper.setUserRole("ACME_METRON_USER");
    mapper.setAdminRole("ACME_METRON_ADMIN");

    List<GrantedAuthority> input = new ArrayList<>();
    input.add(new SimpleGrantedAuthority("ACME_METRON_ADMIN"));

    // ROLE_ADMIN == ACME_METRON_ADMIN
    Collection<? extends GrantedAuthority> actuals = mapper.mapAuthorities(input);
    Assert.assertEquals(1, actuals.size());
    Assert.assertEquals(SECURITY_ROLE_PREFIX + SECURITY_ROLE_ADMIN, actuals.iterator().next().getAuthority());
  }

  @Test
  public void shouldNotMapOtherGroups() {
    mapper = new MetronAuthoritiesMapper();
    mapper.setUserRole("ACME_METRON_USER");
    mapper.setAdminRole("ACME_METRON_ADMIN");

    List<GrantedAuthority> input = new ArrayList<>();
    input.add(new SimpleGrantedAuthority("ANOTHER_GROUP"));

    Collection<? extends GrantedAuthority> actuals = mapper.mapAuthorities(input);
    Assert.assertEquals(1, actuals.size());
    Assert.assertEquals("ANOTHER_GROUP", actuals.iterator().next().getAuthority());
  }
}
