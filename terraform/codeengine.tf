###############################################################
#                                                             #
#                       CODE ENGINE                           #
#                                                             #
###############################################################

resource "ibm_resource_instance" "codeengine" {
  name              = local.ce_project_name
  service           = "codeengine"
  plan              = "standard"
  location          = var.cloud_region
  resource_group_id = data.ibm_resource_group.group.id
}

resource "ibm_cr_namespace" "namespace" {
  name              = local.cr_namespace
  resource_group_id = data.ibm_resource_group.group.id
}

resource "null_resource" "codeengine" {
  provisioner "local-exec" {
    command = "/bin/bash scripts/codeengine.sh"

    environment = {
      APIKEY               = var.ibmcloud_api_key
      RESOURCE_GROUP       = data.ibm_resource_group.group.id
      REGION               = var.cloud_region
      PROJECT_NAME         = local.ce_project_name
      ICR_NS               = local.cr_namespace
      GIT_REPO_URL         = var.git_repo_url
      DOCKERFILE_DIRECTORY = var.dockerfile_directory
    }
  }
  depends_on = [ibm_resource_instance.codeengine, ibm_cr_namespace.namespace]
}
