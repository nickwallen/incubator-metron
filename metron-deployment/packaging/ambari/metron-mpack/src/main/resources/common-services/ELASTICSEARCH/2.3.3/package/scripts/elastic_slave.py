"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

from resource_management.core.resources.system import Execute
from resource_management.libraries.script import Script
from resource_management.core.logger import Logger
from resource_management.libraries.functions.get_user_call_output import get_user_call_output
from resource_management.core.exceptions import ExecutionFailed
from resource_management.core.exceptions import ComponentIsNotRunning

from slave import slave


class Elasticsearch(Script):
    def install(self, env):
        import params
        env.set_params(params)
        Logger.info('Install Elasticsearch slave node')
        self.install_packages(env)

    def configure(self, env, upgrade_type=None, config_dir=None):
        import params
        env.set_params(params)
        Logger.info('Configure Elasticsearch slave node')
        slave()

    def stop(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        Logger.info('Stop Elasticsearch slave node')
        Execute("service elasticsearch stop")

    def start(self, env, upgrade_type=None):
        import params
        env.set_params(params)

        self.configure(env)
        Execute("service elasticsearch start")

    def status(self, env):
        import params
        env.set_params(params)
        Logger.info('Status check Elasticsearch slave node')

        # return codes defined by LSB
        # http://refspecs.linuxbase.org/LSB_3.0.0/LSB-PDA/LSB-PDA/iniscrptact.html
        cmd = "service elasticsearch status"
        rc, out, err = get_user_call_output(cmd, params.elastic_user, is_checked_call=False)

        if rc == 3:
          # if return code = 3, then 'program is not running'
          Logger.info("Elasticsearch slave node is not running")
          raise ComponentIsNotRunning()

        elif rc == 0:
          # if return code = 0, then 'program is running or service is OK'
          Logger.info("Elasticsearch slave node is running")

        else:
          # otherwise program is dead or service state is unknown
          err_msg = "Execution of '{0}' returned {1}".format(cmd, rc)
          raise ExecutionFailed(err_msg, rc, out, err)

    def restart(self, env):
        import params
        env.set_params(params)
        Logger.info('Restart Elasticsearch slave node')
        self.configure(env)
        Execute("service elasticsearch restart")


if __name__ == "__main__":
    Elasticsearch().execute()
