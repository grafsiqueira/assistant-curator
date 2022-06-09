###############################################################
#                                                             #
#                      COGNOS EMBEDED                         #
#                                                             #
###############################################################

data "ibm_resource_instance" "cognos" {
  count             = length(var.cognos_name) > 0 ? 1 : 0
  name              = var.cognos_name
  location          = var.cloud_region
  resource_group_id = data.ibm_resource_group.group.id
  service           = "dynamic-dashboard-embedded"
}

resource "ibm_resource_instance" "cognos" {
  count             = length(var.cognos_name) > 0 ? 0 : 1
  name              = "cognos-instance"
  service           = "dynamic-dashboard-embedded"
  plan              = "lite"
  location          = var.cloud_region
  resource_group_id = data.ibm_resource_group.group.id
}

resource "ibm_resource_key" "cognos" {
  name                 = "cognos_curator_key"
  role                 = "Manager"
  resource_instance_id = local.cognos_id
}
