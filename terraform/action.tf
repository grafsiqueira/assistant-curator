###############################################################
#                                                             #
#                     CLOUD FUNCTIONS                         #
#                                                             #
###############################################################

data "ibm_function_namespace" "namespace" {
  count = length(var.namespace) > 0 ? 1 : 0
  name  = var.namespace
}

resource "ibm_function_namespace" "namespace" {
  count             = length(var.namespace) > 0 ? 0 : 1
  name              = length(var.namespace) > 0 ? var.namespace : "functions-namespace"
  resource_group_id = data.ibm_resource_group.group.id
}

resource "null_resource" "actions" {
  count = length(local.actions)
  provisioner "local-exec" {
    command = "/bin/bash scripts/zip_cf.sh"

    environment = {
      FILENAME = local.actions[count.index].name
      FILEPATH = local.actions[count.index].code_path
    }
  }
}

resource "ibm_function_action" "action" {
  count                   = length(local.actions)
  name                    = local.actions[count.index].name
  namespace               = local.namespace_name
  user_defined_parameters = local.actions[count.index].user_defined_parameters

  exec {
    code_path = "${local.actions[count.index].code_path}/${local.actions[count.index].name}.zip"
    kind      = "nodejs:12"
  }
  depends_on = [null_resource.actions]
}

resource "ibm_function_action" "sequence" {
  name      = "assistant-curation-tf"
  namespace = local.namespace_name

  exec {
    kind       = "sequence"
    components = [for o in ibm_function_action.action : "/${local.namespace_id}/${o.name}"]
  }
}

resource "ibm_function_trigger" "trigger" {
  name      = "curation-trigger"
  namespace = local.namespace_name
  feed {
    name       = "/whisk.system/alarms/alarm"
    parameters = <<EOF
				[
					{
						"key":"cron",
						"value":"0 9 * * *"
					}
				]
	  	  EOF
  }
}

resource "ibm_function_rule" "rule" {
  name         = "curator"
  namespace    = local.namespace_name
  trigger_name = ibm_function_trigger.trigger.name
  action_name  = ibm_function_action.sequence.name
}

# WATSON EXPERIMENTS CLOUD FUNCTION

resource "null_resource" "experiments-action" {
  provisioner "local-exec" {
    command = "/bin/bash scripts/zip_cf.sh"

    environment = {
      FILENAME = "watson-experiments-cf"
      FILEPATH = "../cloud-functions/watson-experiments-cf"
    }
  }
}

resource "ibm_function_action" "watson-experiments" {
  name                    = "watson-experiments-cf"
  namespace               = local.namespace_name
  user_defined_parameters = <<EOF
        [
      {
        "key":"db2Creds",
        "value": {
          "connStr": "DATABASE=${ibm_resource_key.db2.credentials["connection.db2.database"]};HOSTNAME=${ibm_resource_key.db2.credentials["connection.db2.hosts.0.hostname"]};PORT=${ibm_resource_key.db2.credentials["connection.db2.hosts.0.port"]};PROTOCL=TCPIP;UID=${ibm_resource_key.db2.credentials["connection.db2.authentication.username"]};PWD=${ibm_resource_key.db2.credentials["connection.db2.authentication.password"]};Security=SSL;",
          "tables": {
            "overviewTable": "OVERVIEW",
            "classDistributionTable":"CLASSDISTRIBUTION",
            "precisionAtKTable":"PRECISIONATK",
            "classAccuracyTable":"CLASSACCURACY",
            "pairWiseClassErrorsTable":"PAIRWISECLASSERRORS",
            "accuracyVsCoverageTable":"ACCURACYVSCOVERAGE"
          }
        }
      },
      {
        "key":"assistantCreds",
        "value": {
          "apiKey": "${ibm_resource_key.assistant.credentials.apikey}",
          "version": "2021-06-14",
          "url": "${ibm_resource_key.assistant.credentials.url}",
          "workspaceID": "${var.skillID}"
        }
      }
    ]
     EOF

  limits {
    timeout = 130000
  }

  exec {
    code_path = "../cloud-functions/watson-experiments-cf/watson-experiments-cf.zip"
    image     = "iisbrasil/experiment-package"
    kind      = "blackbox"
  }
  depends_on = [null_resource.experiments-action]
}
