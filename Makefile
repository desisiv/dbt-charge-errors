# Include environment variables
include .env

# packages project into a docker image
package:
	@echo Building ${IMAGE_NAMESPACE}/${IMAGE_NAME}...
	docker build ./dbt_pipeline -t ${IMAGE_NAMESPACE}/${IMAGE_NAME}:latest

# publishes primary docker images of this project
publish-ci:
	@echo Publishing ${IMAGE_NAMESPACE}/${IMAGE_NAME} image...
	docker push $(IMAGE_NAMESPACE)/${IMAGE_NAME}:latest

# publishes docker images of this project
publish:
	@$(MAKE) package
	@$(MAKE) publish-ci
