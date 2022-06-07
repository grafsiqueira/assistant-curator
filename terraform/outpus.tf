###############################################################
#                                                             #
#                         CLOUDANT                            #
#                                                             #
###############################################################

output "cloudant_url" {
  value = ibm_resource_key.cloudant.credentials.url
}

output "cloudant_apikey" {
  value = ibm_resource_key.cloudant.credentials.apikey
}

###############################################################
#                                                             #
#                           DB2                               #
#                                                             #
###############################################################

output "db2_jdbc" {
  value = local.db2_jdbc
}

output "db2_username" {
  value = ibm_resource_key.db2.credentials["connection.db2.authentication.username"]
}

output "db2_password" {
  value = ibm_resource_key.db2.credentials["connection.db2.authentication.password"]
}

###############################################################
#                                                             #
#                      COGNOS EMBEDED                         #
#                                                             #
###############################################################

output "cognos_username" {
  value = ibm_resource_key.cognos.credentials.client_id
}

output "cognos_password" {
  value = ibm_resource_key.cognos.credentials.client_secret
}
