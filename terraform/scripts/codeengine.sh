
# Login into IBM Cloud CLI and target resource group and region
ibmcloud login --apikey $APIKEY
ibmcloud target -g $RESOURCE_GROUP
ibmcloud target -r $REGION

# Install plugins
ibmcloud plugin install -f code-engine
ibmcloud plugin install -f container-registry

# Select the Code Engine Project
ibmcloud ce project select -n $PROJECT_NAME

# Clean the enviromment
ibmcloud ce registry delete -n assistant-curator-cr-secret -f
ibmcloud ce buildrun delete -n curator-run -f
ibmcloud ce build delete -n curator-build -f
# ibmcloud ce app delete -n $PROJECT_NAME -f

# Grab the Container Registry server location based no our region
export ICR=$(ibmcloud cr info | sed -n 's/.*Registry   *\(.*icr.io\).*/\1/p')

# Create an apikey, put it in a registry secret. Used to push/pull image to Container Registry
ibmcloud ce registry create --name assistant-curator-cr-secret \
    --password $APIKEY -s $ICR -u iamapikey

# Create a build from the github repository
ibmcloud ce build create -n curator-build -i "$ICR/${ICR_NS}/app" --rs assistant-curator-cr-secret \
    --source $GIT_REPO_URL --context-dir $DOCKERFILE_DIRECTORY

# Now kick off the build itself
ibmcloud ce buildrun submit -n curator-run --build curator-build --wait

# Create the app on Code Engine with the Image
ibmcloud ce app create -n $PROJECT_NAME --image "$ICR/${ICR_NS}/app" --registry-secret assistant-curator-cr-secret