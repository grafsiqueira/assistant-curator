# Automated Deployment

## Intro

This files include a series of configurations that deploys all the infrastructure needed to use this asset.
This guide will help you on how to use this files and IBM Cloud Schematics to run the configuration files on your IBM Cloud Account.

---

## Requirements

| Name                          | Version/Type |
| ----------------------------- | :----------: |
| IBM Cloud Account             | Paid account |
| IBM Cloud Account User Access |     Info     |
| GitHub Account                |    Public    |

### IBM Cloud Account

Most of the services provisioned here use the lite/free plan, but need a paid account due to the possibility to charge when the resource exceeds the maximum of the free tier.

### IBM Cloud Account User Access

The user used to create the API Key must have permission to create all of the resources used on the asset.

### GitHub Account

You will need to create a repository with the zip file provided afterwards. This repository must have public visibility at the time the resources are created in order to everything works.

---

## Services and Instances Provisioned

Most of the services and instances used on the asset are ready to both get the information about an existing one or to create a new one. Some of them can't use an existing service or instance.

The table bellow show you where you can use an existing service or instance, as well as the minimum plan/type used to create them or needed from an existing one, in order to work correctly in the asset.

| Name                           | Accept an Existing Service | Minimum Plan/Type |
| ------------------------------ | :------------------------: | :---------------: |
| Actions                        |             No             |         -         |
| Container Registry             |             No             |         -         |
| Watson Assistatn               |            Yes             |       Free        |
| Cloudant                       |             No             |       Lite        |
| Code Engine                    |             No             |     Standard      |
| Cognos Dashboard Embeded       |            Yes             |     Standard      |
| Cloud Object Storage           |            Yes             |       Lite        |
| Cloud Object Storage - Bucket  |            Yes             |       Smart       |
| Db2                            |            Yes             |     Standard      |
| Natural Language Understanding |            Yes             |       Free        |

---

## How to Deploy your Resources

To deploy this resources you will have to download the [code here](https://portal-de-demos-arquivos.s3.us-south.cloud-object-storage.appdomain.cloud/assistant-curator.zip) and create a GitHub, GitLab or Bitbucket repository. You will use the IBM Cloud Schematics on your account to manage your resources.

### Creating a Shcematics Workspace

To create a workspace you need to go to https://cloud.ibm.com/schematics/overview and click add workspace. Select the option "Select existing template" and paste the link to yout repository (**https://github.com/\<repository\>/tree/\<branch\>/terraform**). Then, you need to select the **terraform_v0.13** version and click next.

![Creating a Schematics Workspace](https://portal-de-demos-imgs.s3.us-south.cloud-object-storage.appdomain.cloud/assistant-curator-terraform-Create%20Schematics.png)

Name your workspace and click next. Check your informations and click to create the workspace.

You need to provide the four first variables (_ibmcloud_api_key, cloud_region, resource_group, git_repo_url_) by editing them. See the variables descriptions in the next section to know which values you’ll need to set the variables.

![Editing Schematics variables](https://portal-de-demos-imgs.s3.us-south.cloud-object-storage.appdomain.cloud/assistant-curator-terraform-Edit%20Variables.png)

You can click to genarete a plan, to see what will be created, or click apply the plan to provision your resources. After the services are provisioned, you can see them by clicking "Resources".

![Planning and Applying Schematics](https://portal-de-demos-imgs.s3.us-south.cloud-object-storage.appdomain.cloud/assistant-curator-terraform-Plans.png)

To see more about Schematics see the complete documentation: https://cloud.ibm.com/docs/schematics

### Declaring the variables

The last thing you need to do before deploying your resources is set the values to the Terraform Variables. Here you can see the name, description, if it's a required variable and the resource affected by that variable, for each variable.

| Variable             | Description                                                                                                                                                                                                                        | Required |       Resource Affected        |
| -------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------: | :----------------------------: |
| ibmcloud_api_key     | Secret from IBM Cloud to grant acces to interact with cloud resources.<br />See more about it: https://www.ibm.com/docs/en/app-connect/containers_cd?topic=servers-creating-cloud-api-key                                          |   Yes    |              All               |
| cloud_region         | Region to create the resources, and where the existing resources are.<br />See the availabe regions here: https://cloud.ibm.com/docs/certificate-manager?topic=certificate-manager-regions-endpoints#regions                       |   Yes    |              All               |
| resource_group       | Resource group to associate with the services instances.<br />See your resource groups here: https://cloud.ibm.com/account/resource-groups                                                                                         |   Yes    |              All               |
| git_repo_url         | The HTTPS link to your GitHub Repository root directory.\nSee how you can get this link here: https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository#cloning-a-repository.                |   Yes    |          Code Engine           |
| dockerfile_directory | The location of the Dockerfile in the Git Repository.<br>Only need to change if you change the Dockerfile directory.                                                                                                               |    No    |          Code Engine           |
| has_lite_cloudant    | To prevent erros creating your Cloudant instance, use this variable to inform if you already has any LITE Cloudant Instance in your account.<br>If you change this variable to True, a Standard Cloudant instance will be created. |    No    |            Cloudant            |
| namespace            | The namespace used to create the Cloud Function Actions.<br />If no namespace is provided, the default 'functions-namespace' will be created.                                                                                      |    No    |        Cloud Functions         |
| skillID              | The skill ID of your Watson Assistant.<br />If you don't have one yet, referal to the documeation later to add it.                                                                                                                 |    No    |        Cloud Functions         |
| cos_name             | The name of your Cloud Object Storage.<br />If no COS name is provided, the default 'cos-instance' will be created.                                                                                                                |    No    |      Cloud Object Storage      |
| bucket_name          | The bucket used to store the logs from your Assistant.<br />If no buckt name is provided, the default 'assistant-curation' bucket will be created.                                                                                 |    No    |      Cloud Object Storage      |
| nlu_name             | The name of your Natural Language Understanding Service.<br />If no NLU name is provided, the default 'nlu-instance' will be created.                                                                                              |    No    | Natural Language Understanding |
| db2_name             | The name of your Db2 Instance.<br />If no Db2 name is provided, the default 'db2-instance' will be created.                                                                                                                        |    No    |              Db2               |
| assistant_name       | The name of your Watson Assistant Instance.<br />If no Assistant name is provided, the default 'assistant-instance' will be created.                                                                                               |    No    |        Watson Assistant        |
| cognos_name          | The name of your Cognos Embeded Instance.<br />If no Cognos name is provided, the default 'cognos-instance' will be created.                                                                                                       |    No    |         Cognos Embeded         |

---

## Editing the parameters to a Cloud Function

Through the creation of the resources, the Cloud Functions Actions are created, and one of them contains some parameters used later on the asset to deal with the data. You can change this values later if you want to, and, some of them need to be edited if you didn't had a Watson Assistant resource previously created before the implemantation of your other resources.

To edit them you need to go to **Cloud Funcionts -> Action -> Search for the Action "create-tables" -> Parameters**. There you will find the parameters previously created by Terraform with the information about other resources. If you see the string in the value of the "assistantConfig" parameter you will find "["example1", "example2"]" for the keys transferNode(The node(s) in your assistant that transfers the user), feedbackNode(The node(s) in your Assistant that contais feedback) and relevantTopics(Intents that appears in a conversation that you consider relevant).

In order to get good insights from your data, you need to substitute them for the infomation corresponding to your assistant. For example, in transferNode: ["talkToHuman"] or ["talkToHuman", "problemNotSolved"]. **Always rememer** that these three keys (transferNode, feedbackNode, relevantTopics) are a list in the format: ["value1] or ["value1", "value2"]

If you want to change any other value in any other parameter, just check if you are using the same format and follow the steps before to access the parameters
