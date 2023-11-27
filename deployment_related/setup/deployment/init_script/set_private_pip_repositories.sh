#!/bin/bash
printf "[global]\n" > /etc/pip.conf
printf "extra-index-url=https://datax-ai:$AZURE_DEVOPS_EXT_PAT@$ARTIFACT_URL\n" >> /etc/pip.conf