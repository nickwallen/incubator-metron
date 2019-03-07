package org.apache.metron.rest.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Objects;

/**
 * Defines the roles used for authorization.
 */
@Component
public class Roles {

  /**
   * A prefix shared by all Metron roles.
   */
  @Value("${authorization.rolePrefix}")
  private String rolePrefix;

  /**
   * The name of the user role.
   */
  @Value("${authorization.roles.user}")
  private String userRole;

  /**
   * The name of the admin role.
   */
  @Value("${authorization.roles.admin}")
  private String adminRole;

  public Roles() {
    this.rolePrefix = "ROLE_";
    this.userRole = "USER";
    this.adminRole = "ADMIN";
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

  public String getRolePrefix() {
    return rolePrefix;
  }

  public void setRolePrefix(String rolePrefix) {
    this.rolePrefix = rolePrefix;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Roles)) return false;
    Roles roles = (Roles) o;
    return Objects.equals(rolePrefix, roles.rolePrefix) &&
            Objects.equals(userRole, roles.userRole) &&
            Objects.equals(adminRole, roles.adminRole);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rolePrefix, userRole, adminRole);
  }

  @Override
  public String toString() {
    return "Roles{" +
            "rolePrefix='" + rolePrefix + '\'' +
            ", userRole='" + userRole + '\'' +
            ", adminRole='" + adminRole + '\'' +
            '}';
  }
}
