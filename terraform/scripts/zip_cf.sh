# Install zip
yum install zip unzip -y

# Creates a zip file to the referenced Cloud Function
cd $FILEPATH
zip -r $FILENAME *