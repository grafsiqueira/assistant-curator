###############################################################
#                                                             #
#                        IBM CLOUD                            #
#                                                             #
###############################################################

variable "ibmcloud_api_key" {
  description = "Secret from IBM Cloud to grant acces to interact with cloud resources. See more about it: https://www.ibm.com/docs/en/app-connect/containers_cd?topic=servers-creating-cloud-api-key"
  type        = string
}

variable "cloud_region" {
  description = "Region to create the resources, and where the existing resources are. See the availabe regions here: https://cloud.ibm.com/docs/certificate-manager?topic=certificate-manager-regions-endpoints#regions"
  type        = string
}

variable "resource_group" {
  description = "Resource group to associate with the services instances. See your resource groups here: https://cloud.ibm.com/account/resource-groups"
  type        = string
}

###############################################################
#                                                             #
#                       CODE ENGINE                           #
#                                                             #
###############################################################

variable "git_repo_url" {
  description = "The HTTPS link to your GitHub Repository root directory.\nSee how you can get this link here: https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository#cloning-a-repository."
  type        = string
}

variable "dockerfile_directory" {
  description = "The location of the Dockerfile in the Git Repository.\nOnly need to change if you change the Dockerfile directory."
  default     = "curator-interface/backend"
  type        = string
}

###############################################################
#                                                             #
#                     CLOUD FUNCTIONS                         #
#                                                             #
###############################################################

variable "namespace" {
  description = "The namespace used to create the Cloud Function Actions.\nIf no namespace is provided, the default 'functions-namespace' will be created."
  type        = string
  default     = ""
}

variable "skillID" {
  description = "The skill ID of your Watson Assistant.\nIf you don't have one yet, referal to the documeation later to add it."
  type        = string
  default     = ""
}

###############################################################
#                                                             #
#                   CLOUD OBJECT STORAGE                      #
#                                                             #
###############################################################

variable "cos_name" {
  description = "The name of your Cloud Object Storage.\nIf no COS name is provided, the default 'cos-instance' will be created."
  type        = string
  default     = ""
}

variable "bucket_name" {
  description = "The bucket used to store the logs from your Assistant.\nIf no buckt name is provided, the default 'assistant-curation' bucket will be created."
  type        = string
  default     = ""
}

###############################################################
#                                                             #
#                 NATURAL LANGUAGE UNDERSTANDING              #
#                                                             #
###############################################################

variable "nlu_name" {
  description = "The name of your Natural Language Understanding Service.\nIf no NLU name is provided, the default 'nlu-instance' will be created."
  type        = string
  default     = ""
}

###############################################################
#                                                             #
#                           DB2                               #
#                                                             #
###############################################################

variable "db2_name" {
  description = "The name of your Db2 Instance.\nIf no Db2 name is provided, the default 'db2-instance' will be created."
  type        = string
  default     = ""
}

###############################################################
#                                                             #
#                         CLOUDANT                            #
#                                                             #
###############################################################

variable "has_lite_cloudant" {
  description = "To prevent erros creating your Cloudant instance, use this variable to inform if you already has any LITE Cloudant Instance in your account.\nIf you change this variable to True, a Standard Cloudant instance will be created."
  type        = bool
  default     = false
}

###############################################################
#                                                             #
#                        ASSISTANT                            #
#                                                             #
###############################################################

variable "assistant_name" {
  description = "The name of your Watson Assistant Instance.\nIf no Assistant name is provided, the default 'assistant-instance' will be created."
  type        = string
  default     = ""
}

###############################################################
#                                                             #
#                      COGNOS EMBEDED                         #
#                                                             #
###############################################################

variable "cognos_name" {
  description = "The name of your Cognos Embeded Instance.\nIf no Cognos name is provided, the default 'cognos-instance' will be created."
  type        = string
  default     = ""
}
