package org.apache.metron.rest.config;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.authority.mapping.GrantedAuthoritiesMapper;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import static org.apache.metron.rest.MetronRestConstants.SECURITY_ROLE_ADMIN;
import static org.apache.metron.rest.MetronRestConstants.SECURITY_ROLE_PREFIX;
import static org.apache.metron.rest.MetronRestConstants.SECURITY_ROLE_USER;

/**
 * Maps the authorities used in Metron to the roles defined at the authentication provider.
 */
@Configuration
public class MetronAuthoritiesMapper implements GrantedAuthoritiesMapper {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * The name of the role at the authentication provider that maps to ROLE_USER.
   */
  @Value("${authorities.user}")
  private String userRole;

  /**
   * The name of the role at the authentication provider that maps to ROLE_ADMIN.
   */
  @Value("${authorities.admin}")
  private String adminRole;

  public MetronAuthoritiesMapper() {
    // by default, assume the roles used at the authentication provider are the same as those defined by Metron
    userRole = SECURITY_ROLE_PREFIX + SECURITY_ROLE_USER;
    adminRole = SECURITY_ROLE_PREFIX + SECURITY_ROLE_ADMIN;
  }

  @Override
  public Collection<? extends GrantedAuthority> mapAuthorities(Collection<? extends GrantedAuthority> authorities) {
    LOG.debug("Mapping authorities to roles; {}={}, {}={}",
            SECURITY_ROLE_PREFIX + SECURITY_ROLE_USER, userRole,
            SECURITY_ROLE_PREFIX + SECURITY_ROLE_ADMIN, adminRole);

    HashSet<GrantedAuthority> mapped = new HashSet(authorities.size());
    Iterator<? extends GrantedAuthority> iterator = authorities.iterator();
    while(iterator.hasNext()) {
      GrantedAuthority authority = iterator.next();
      mapped.add(mapAuthority(authority.getAuthority()));
    }

    return mapped;
  }

  public GrantedAuthority mapAuthority(String authority) {
    GrantedAuthority grantedAuthority;
    if(StringUtils.equals(authority, userRole)) {
      grantedAuthority = new SimpleGrantedAuthority(SECURITY_ROLE_PREFIX + SECURITY_ROLE_USER);

    } else if(StringUtils.equals(authority, adminRole)) {
      grantedAuthority = new SimpleGrantedAuthority(SECURITY_ROLE_PREFIX + SECURITY_ROLE_ADMIN);

    } else {
      // otherwise, no mapping required
      grantedAuthority = new SimpleGrantedAuthority(authority);
    }

    LOG.debug("Mapped authority {} to {}", authority, grantedAuthority.getAuthority());
    return grantedAuthority;
  }

  public String getUserRole() {
    return userRole;
  }

  public void setUserRole(String userRole) {
    this.userRole = userRole;
  }

  public String getAdminRole() {
    return adminRole;
  }

  public void setAdminRole(String adminRole) {
    this.adminRole = adminRole;
  }
}
