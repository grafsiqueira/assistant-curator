provider "ibm" {
  ibmcloud_api_key = var.ibmcloud_api_key
  region           = var.cloud_region
}

terraform {
  required_providers {
    ibm = {
      source  = "IBM-Cloud/ibm"
      version = "1.36.0"
    }
  }
}
