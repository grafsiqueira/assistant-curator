locals {
  cr_namespace    = "assistant-curator-${formatdate("DDMMYYYYhhmm", timestamp())}"
  db2_jdbc        = replace(ibm_resource_key.db2.credentials["connection.db2.jdbc_url.0"], "user=<userid>;password=<your_password>;", "")
  ce_project_name = "assistant-curation-${formatdate("DDMMYYYYhhmmss", timestamp())}"
  nlu_id          = length(var.nlu_name) > 0 ? data.ibm_resource_instance.nlu[0].id : ibm_resource_instance.nlu[0].id
  db2_id          = length(var.db2_name) > 0 ? data.ibm_resource_instance.db2[0].id : ibm_resource_instance.db2[0].id
  cloudant_id     = length(var.cloudant_name) > 0 ? data.ibm_cloudant.cloudant[0].id : ibm_cloudant.cloudant[0].id
  cognos_id       = length(var.cognos_name) > 0 ? data.ibm_resource_instance.cognos[0].id : ibm_resource_instance.cognos[0].id
  assistant_id    = length(var.assistant_name) > 0 ? data.ibm_resource_instance.assistant[0].id : ibm_resource_instance.assistant[0].id
  cos_id          = length(var.cos_name) > 0 ? data.ibm_resource_instance.cos[0].id : ibm_resource_instance.cos[0].id
  bucket_name     = length(var.bucket_name) > 0 ? var.bucket_name : ibm_cos_bucket.bucket[0].bucket_name
  cos_endpoint    = length(var.bucket_name) > 0 ? data.ibm_cos_bucket.bucket[0].s3_endpoint_private : ibm_cos_bucket.bucket[0].s3_endpoint_private
  cos_crn         = length(var.bucket_name) > 0 ? data.ibm_cos_bucket.bucket[0].crn : ibm_cos_bucket.bucket[0].crn
  namespace_id    = length(var.namespace) > 0 ? data.ibm_function_namespace.namespace[0].id : ibm_function_namespace.namespace[0].id
  namespace_name  = length(var.namespace) > 0 ? var.namespace : ibm_function_namespace.namespace[0].name
  actions = [
    {
      "name" : "create-tables-cf",
      "code_path" : "../cloud-functions/create-tables-cf",
      "user_defined_parameters" : <<EOF
        [
      {
        "key":"dbConfig",
        "value": {
          "connStr": "DATABASE=${ibm_resource_key.db2.credentials["connection.db2.database"]};HOSTNAME=${ibm_resource_key.db2.credentials["connection.db2.hosts.0.hostname"]};PORT=${ibm_resource_key.db2.credentials["connection.db2.hosts.0.port"]};PROTOCL=TCPIP;UID=${ibm_resource_key.db2.credentials["connection.db2.authentication.username"]};PWD=${ibm_resource_key.db2.credentials["connection.db2.authentication.password"]};Security=SSL;",
          "primaryTableName": "logs",
          "secondaryTableName": "conversations",
          "tertiaryTableName": "calls",
          "quaternaryTableName": "contextVariables",
          "quinaryTableName": "conversationPath"
        }
      },
      {
        "key":"assistantConfig",
        "value": {
          "apiKey": "${ibm_resource_key.assistant.credentials.apikey}",
          "version": "2021-06-14",
          "serviceUrl": "${ibm_resource_key.assistant.credentials.url}",
          "skillID": "${var.skillID}",
          "transferNode": ["example1", "example2"],
          "feedbackNode": ["example1", "example2"],
          "relevantTopics": ["example1", "example2"],
          "finalNode": ["example1"]
        }
      },
      {
        "key":"nluConfig",
        "value": {
          "language": "pt",
          "version": "2021-08-01",
          "apikey": "${ibm_resource_key.nlu.credentials.apikey}",
          "serviceUrl": "${ibm_resource_key.nlu.credentials.url}"
        }
      },
      {
        "key":"cosConfig",
        "value": {
          "endpoint": "${local.cos_endpoint}",
          "apiKeyId": "${var.ibmcloud_api_key}",
          "serviceInstanceId": "${local.cos_crn}",
          "bucketName": "${local.bucket_name}"
        }
      },
      {
        "key":"cloudantConfig",
        "value": {
        "apiKey":"${ibm_resource_key.cloudant.credentials.apikey}",
        "url":"${ibm_resource_key.cloudant.credentials.url}",
        "dbName":"logs"
    }
      }
    ]
     EOF
    },
    {
      "name" : "process-logs-cf",
      "code_path" : "../cloud-functions/process-logs-cf",
      "user_defined_parameters" : jsonencode([])
    },
    {
      "name" : "process-conversations-cf",
      "code_path" : "../cloud-functions/process-conversations-cf",
      "user_defined_parameters" : jsonencode([])
    },
    {
      "name" : "enrichment-cf",
      "code_path" : "../cloud-functions/enrichment-cf",
      "user_defined_parameters" : jsonencode([])
    },
    {
      "name" : "insert-logs-cf",
      "code_path" : "../cloud-functions/insert-logs-cf",
      "user_defined_parameters" : jsonencode([])
    }
  ]
}
