###############################################################
#                                                             #
#                         CLOUDANT                            #
#                                                             #
###############################################################

resource "ibm_cloudant" "cloudant" {
  name     = "cloudant-instance"
  location = var.cloud_region
  plan     = var.has_lite_cloudant ? "standard" : "lite"

  timeouts {
    create = "60m"
  }
}

resource "ibm_resource_key" "cloudant" {
  name                 = "cloudant_curator_key"
  role                 = "Manager"
  resource_instance_id = ibm_cloudant.cloudant.id
}
