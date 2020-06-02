#!/bin/bash

DYNAMODB_DAO_PROJECT_NAME="dynamodb-dao-${RANDOM}"

docker-compose -p ${DYNAMODB_DAO_PROJECT_NAME} run test
EXIT_CODE=${?}
docker-compose -p ${DYNAMODB_DAO_PROJECT_NAME} down

exit ${EXIT_CODE}
