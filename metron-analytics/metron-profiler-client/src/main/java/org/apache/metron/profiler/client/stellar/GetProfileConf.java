/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.metron.profiler.client.stellar;

import org.apache.metron.common.dsl.Context;
import org.apache.metron.common.dsl.ParseException;
import org.apache.metron.common.dsl.Stellar;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Stellar function that can retrieve data contained within a Profile
 * using an override configuration that may be different from the global config,
 * for historical or other reasons.
 *
 *  PROFILE_GET_CONF
 *
 * See {@link GetProfile} for documentation.
 *
 */
@Stellar(
        namespace="PROFILE",
        name="GET_CONF",
        description="Retrieves a series of values from a stored profile.",
        params={
                "profile - The name of the profile.",
                "entity - The name of the entity.",
                "durationAgo - How long ago should values be retrieved from?",
                "units - The units of 'durationAgo'.",
                "config_override - Map of key-value pairs, each overriding the global config parameter of the same name.",
                "groups - Optional - The groups used to sort the profile."
        },
        returns="The profile measurements."
)
public class GetProfileConf extends GetProfile {

  /**
   * Apply the function.
   * @param args The function arguments.
   * @param context
   */
  @Override
  public Object apply(List<Object> args, Context context) throws ParseException {

    List<Object> newArgs = new ArrayList<Object>(args);
    Map configOverrideMap = getArg(4, Map.class, args);
    newArgs.remove(4);

    return applyWithConf(newArgs, configOverrideMap, context);
  }

}
